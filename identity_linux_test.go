//go:build linux

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package runc

import (
	"encoding/json"
	"errors"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// requirePidfd skips the test when pidfd_open is unavailable in the kernel
// or blocked by a sandbox, mirroring openPidfd in pidfd_test.go.
func requirePidfd(t *testing.T) {
	t.Helper()
	fd, err := unix.PidfdOpen(unix.Getpid(), 0)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
			t.Skipf("pidfd support required: %v", err)
		}
		t.Fatalf("pidfd_open failed: %v", err)
	}
	_ = unix.Close(fd)
}

func TestProcessIdentityMatchesPid(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	w, err := mon.Watch(sleepCommand(), "ctr-identity")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupWatch(t, w)

	id := w.Identity()
	if id.ContainerID != "ctr-identity" || id.Generation != 1 || id.Pidfd == nil {
		t.Fatalf("unexpected identity: %+v", id)
	}
	if id.StarttimeTicks == 0 {
		t.Fatal("expected the process start time to be recorded")
	}
	if !id.MatchesPid(id.Pid) {
		t.Fatal("identity must match the pid it was opened for")
	}
	if id.MatchesPid(id.Pid + 1) {
		t.Fatal("identity must not match an unrelated pid")
	}

	// A fabricated identity with the same numeric pid but a different start
	// time must not match: that is the pid-reuse boundary.
	impostor := id
	impostor.Pidfd = nil
	impostor.StarttimeTicks = id.StarttimeTicks + 1
	if impostor.MatchesPid(id.Pid) {
		t.Fatal("different start time must not match a recycled pid")
	}
}

func TestIdentityMonitorGenerationsAndPIDReuse(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	const cid = "ctr-restart"

	first, err := mon.Watch(sleepCommand(), cid)
	if err != nil {
		t.Fatal(err)
	}
	firstID := first.Identity()
	if firstID.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", firstID.Generation)
	}

	second, err := mon.Watch(sleepCommand(), cid)
	if err != nil {
		t.Fatal(err)
	}
	secondID := second.Identity()
	if secondID.Generation != 2 {
		t.Fatalf("second generation = %d, want 2", secondID.Generation)
	}
	if firstID.Pidfd.Fd() == secondID.Pidfd.Fd() {
		t.Fatal("each generation must own a distinct pidfd")
	}

	// A different container keeps an independent generation sequence.
	other, err := mon.Watch(sleepCommand(), "ctr-other")
	if err != nil {
		t.Fatal(err)
	}
	if other.Identity().Generation != 1 {
		t.Fatal("generation counters must be scoped per container")
	}

	generations := mon.Generations(cid)
	if len(generations) != 2 || generations[0].Generation != 1 || generations[1].Generation != 2 {
		t.Fatalf("unexpected generations: %+v", generations)
	}
	for _, gen := range generations {
		if gen.Pidfd != nil {
			t.Fatal("returned identities must not expose the monitor-owned pidfd")
		}
	}

	// Simulate the reported incident: the old generation exits while a new
	// process is registered under the same container. Liveness is resolved
	// through the pidfd, so a recycled pid can never confuse the answer.
	if alive, err := mon.Active(firstID); err != nil || !alive {
		t.Fatalf("old generation should be active: alive=%v err=%v", alive, err)
	}
	killProcessTree(t, first.cmd)
	if _, err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if alive, err := mon.Active(secondID); err != nil || !alive {
		t.Fatalf("new generation must stay active: alive=%v err=%v", alive, err)
	}
	// The old pidfd is closed with the watch; the start-time identity
	// survives closing and still proves the generation is gone even if its
	// numeric pid has been recycled.
	oldWithoutFd := firstID
	oldWithoutFd.Pidfd = nil
	if alive, err := mon.Active(oldWithoutFd); err != nil || alive {
		t.Fatalf("old generation must report inactive after exit: alive=%v err=%v", alive, err)
	}
	if remaining := mon.Generations(cid); len(remaining) != 1 || remaining[0].Generation != 2 {
		t.Fatalf("old generation unregister must leave only the new one: %+v", remaining)
	}
	cleanupWatch(t, second)
	cleanupWatch(t, other)
}

func TestIdentityMonitorOldGenerationExitIsolated(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	const cid = "ctr-exit"

	old, err := mon.Watch(exitCommand(7), cid)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := mon.Watch(sleepCommand(), cid)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupWatch(t, newer)

	exit, err := old.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if exit.Status != 7 {
		t.Fatalf("exit status = %d, want 7", exit.Status)
	}
	if exit.ContainerID != cid || exit.Generation != 1 {
		t.Fatalf("exit event must carry the old identity: %+v", exit)
	}

	// The old generation's exit only unregistered itself.
	remaining := mon.Generations(cid)
	if len(remaining) != 1 || remaining[0].Generation != 2 {
		t.Fatalf("old exit removed the wrong generation: %+v", remaining)
	}
	if alive, err := mon.Active(newer.Identity()); err != nil || !alive {
		t.Fatalf("new generation must survive the old generation's exit: alive=%v err=%v", alive, err)
	}
}

func TestWatchEventCoalescing(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	// A one-slot channel with no reader: once it is full, further emits
	// coalesce into the pending intermediate event instead of blocking.
	events := make(chan WatchEvent, 1)
	w, err := mon.Watch(sleepCommand(), "ctr-events", WithEvents(events))
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupWatch(t, w)

	const emitted = 5
	for i := 0; i < emitted; i++ {
		if err := w.Emit(WatchEvent{Type: WatchEventStats}); err != nil {
			t.Fatal(err)
		}
	}
	stats := w.Stats()
	if stats.MergedEvents == 0 {
		t.Fatal("expected intermediate events to be merged on a full channel")
	}
	// Deterministic accounting: the first emit occupies the single channel
	// slot, every later emit coalesces, and one last coalesced event stays
	// pending in the monitor until a reader frees the slot.
	if stats.DeliveredEvents != 1 || stats.MergedEvents != uint64(emitted-2) {
		t.Fatalf("unexpected accounting for %d emits: %+v", emitted, stats)
	}

	// A draining reader receives the queued event, after which the next
	// emit delivers normally; no exit event is mixed in while the process
	// is alive.
	got := <-events
	if got.Type != WatchEventStats {
		t.Fatalf("event type = %q, want stats", got.Type)
	}
	if err := w.Emit(WatchEvent{Type: WatchEventStats}); err != nil {
		t.Fatal(err)
	}
	got = <-events
	if got.Type != WatchEventStats {
		t.Fatalf("event type = %q, want stats", got.Type)
	}

	// The terminal event always flushes the last pending intermediate
	// event first, even on a perpetually busy channel.
	killProcessTree(t, w.cmd)
	if _, err := w.Wait(); err != nil {
		t.Fatal(err)
	}
	final := w.Stats()
	if final.MergedEvents < stats.MergedEvents {
		t.Fatalf("merge counters must never decrease: %+v -> %+v", stats, final)
	}
	// Global identity: every emitted intermediate event is accounted for
	// exactly once, either delivered (including the terminal flush) or
	// merged, with the merged count reported explicitly.
	if final.DeliveredEvents+final.MergedEvents != uint64(emitted)+1 {
		t.Fatalf("intermediate events not fully accounted for: %+v", final)
	}
}

func TestTerminalExitRetainedOnFullChannel(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	// Unbuffered channel with no reader: the terminal event cannot be
	// delivered, yet Wait must still return the final exit status.
	w, err := mon.Watch(exitCommand(9), "ctr-full", WithEvents(make(chan WatchEvent)))
	if err != nil {
		t.Fatal(err)
	}

	exit, err := w.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if exit.Status != 9 {
		t.Fatalf("exit status = %d, want 9", exit.Status)
	}
	if exit.Generation != 1 || exit.ContainerID != "ctr-full" {
		t.Fatalf("terminal exit must carry identity: %+v", exit)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchCloseExitRace(t *testing.T) {
	requirePidfd(t)

	// Repeatable stress: Close and process exit racing must never panic,
	// double-close a channel or stop the exit reader more than once.
	for i := 0; i < 50; i++ {
		mon := NewIdentityMonitor()
		cmd := sleepCommand()
		w, err := mon.Watch(cmd, "ctr-race")
		if err != nil {
			t.Fatal(err)
		}
		pgid := cmd.Process.Pid

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			_ = w.Close()
		}()
		go func() {
			defer wg.Done()
			_, _ = w.Wait()
		}()
		go func() {
			// Close does not kill the process, so terminate the whole
			// process group to drive the exit path concurrently.
			defer wg.Done()
			time.Sleep(time.Millisecond)
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		}()
		wg.Wait()

		// Wait for the watch's own reaper so the killed command is fully
		// collected before the pid (and thus the process group id) can be
		// recycled for the next iteration.
		if _, err := w.Wait(); err != nil && !errors.Is(err, ErrWatchClosed) {
			t.Fatal(err)
		}
		waitForGroupGone(t, pgid)

		// Close must remain idempotent after the race.
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// waitForGroupGone polls the process group until the signal check reports
// ESRCH, used only after the group leader has been reaped.
func waitForGroupGone(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pgid, 0); errors.Is(err, unix.ESRCH) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("process group %d still exists after kill", pgid)
}

func TestWithIdentityDuplicatesCallerPidfd(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	target := sleepCommand()
	if err := target.Start(); err != nil {
		t.Fatal(err)
	}
	external, err := openProcessPidfd(target.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if target.Process != nil {
			_ = target.Process.Kill()
			_ = target.Wait() // reap it so it cannot linger as a zombie
		}
		_ = external.Close()
	}()

	w, err := mon.Watch(sleepCommand(), "ctr-external", WithIdentity(ProcessIdentity{
		ContainerID: "ctr-external",
		Pid:         target.Process.Pid,
		Pidfd:       external,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Close already stopped the watch; reap the still-running process
		// tree and close the monitor pidfd through the idempotent Close.
		killProcessTree(t, w.cmd)
		_, _ = w.Wait()
		_ = w.Close()
	}()
	// Closing the watch must not invalidate the caller's own pidfd; the
	// monitored process is still running, so the caller's pidfd stays alive.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	alive, err := pidfdAlive(external)
	if err != nil {
		t.Fatalf("caller pidfd must remain usable: %v", err)
	}
	if !alive {
		t.Fatal("caller pidfd must remain open after watch close")
	}
}

func TestExitBackwardCompatibleJSON(t *testing.T) {
	// The old three-field JSON shape must still decode.
	var legacy Exit
	if err := json.Unmarshal([]byte(`{"Timestamp":"2026-01-01T00:00:00Z","Pid":42,"Status":0}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.Pid != 42 || legacy.ContainerID != "" || legacy.Generation != 0 {
		t.Fatalf("legacy JSON decoded incorrectly: %+v", legacy)
	}

	// New fields are omitempty, so encoding a zero-added-field value keeps
	// producing the historical shape.
	encoded, err := json.Marshal(Exit{Pid: 1, Status: 0})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"containerId", "generation", "mergedEvents"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("new field %q must be omitted when unset", key)
		}
	}
}

func TestLegacyMonitorUnchanged(t *testing.T) {
	// The existing ProcessMonitor contract still works exactly as before.
	cmd := exec.Command("sh", "-c", "exit 3")
	ec, err := Monitor.Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	status, err := Monitor.Wait(cmd, ec)
	if err != nil {
		t.Fatal(err)
	}
	if status != 3 {
		t.Fatalf("status = %d, want 3", status)
	}
}

func TestWaitOnClosedWatch(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	cmd := sleepCommand()
	w, err := mon.Watch(cmd, "ctr-closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Wait(); !errors.Is(err, ErrWatchClosed) {
		t.Fatalf("Wait after Close = %v, want ErrWatchClosed", err)
	}
	if !isDone(w.Done()) {
		t.Fatal("Done must be closed after Close")
	}
	if err := w.Emit(WatchEvent{}); !errors.Is(err, ErrWatchClosed) {
		t.Fatalf("Emit after Close = %v, want ErrWatchClosed", err)
	}
	// Close does not kill the process; signal it so the still-running reaper
	// goroutine can collect it instead of waiting for the full sleep.
	killProcessTree(t, cmd)
	// Give the reaper time to reap; it exclusively owns cmd.Wait.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(cmd.Process.Pid, 0); errors.Is(err, unix.ESRCH) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("killed process was not reaped")
}

func TestCloseStopsReaderGoroutinesOnce(t *testing.T) {
	requirePidfd(t)

	mon := NewIdentityMonitor()
	cmd := sleepCommand()
	w, err := mon.Watch(cmd, "ctr-reader")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { killProcessTree(t, cmd) })

	// Closing while the process is alive must mark the watch done exactly
	// once and close the monitor-owned pidfd.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if !isDone(w.Done()) {
		t.Fatal("Done must be closed after Close")
	}
	// The monitor-owned pidfd file must be closed. Signal 0 is a no-op on
	// a valid pidfd; on a closed (or recycled) fd number it fails EBADF,
	// which poll alone cannot distinguish reliably.
	if err := unix.PidfdSendSignal(int(w.identity.Pidfd.Fd()), 0, nil, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("monitor pidfd must be closed by Close, got err=%v", err)
	}

	// The start-time identity proves the process itself was not killed:
	// Close stops monitoring, it does not stop the process.
	id := w.identity
	id.Pidfd = nil
	if alive, err := mon.Active(id); err != nil || !alive {
		t.Fatalf("Close must not kill the monitored process: alive=%v err=%v", alive, err)
	}

	// Repeated and concurrent Close calls must all return without panic,
	// proving every close-side transition happens at most once.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Close(); err != nil {
				t.Errorf("concurrent Close: %v", err)
			}
		}()
	}
	wg.Wait()
	killProcessTree(t, cmd)
}

func TestWaitForExitTiming(t *testing.T) {
	requirePidfd(t)
	mon := NewIdentityMonitor()
	start := time.Now()
	w, err := mon.Watch(exec.Command("sh", "-c", "sleep 0.2; exit 0"), "ctr-timing")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Wait(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("Wait returned before the process exited")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// --- test helpers ---

func sleepCommand() *exec.Cmd {
	// Exec sleep directly instead of `sh -c "sleep ..."`: with a shell in
	// between, killing the shell reparents the sleep child to init and pid
	// recycling makes group-based cleanup racy under stress. A single
	// process in its own group has none of those problems.
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

func exitCommand(status int) *exec.Cmd {
	return exec.Command("sh", "-c", "exit "+strconv.Itoa(status))
}

// killProcessTree signals the command's whole process group. The caller
// (or the watch's reaper via cmd.Wait) is responsible for reaping; a
// zombie still belongs to its group, so group emptiness cannot be observed
// before the wait completes.
func killProcessTree(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd.Process == nil {
		return
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil &&
		!errors.Is(err, unix.ESRCH) {
		t.Errorf("kill process group: %v", err)
	}
}

// cleanupWatch terminates the monitored process tree, waits for the
// terminal state and closes the watch, so tests leave no processes behind.
func cleanupWatch(t *testing.T, w *ProcessWatch) {
	t.Helper()
	select {
	case <-w.terminalDone:
	default:
		killProcessTree(t, w.cmd)
	}
	_, _ = w.Wait()
	if err := w.Close(); err != nil {
		t.Errorf("watch close: %v", err)
	}
}

func isDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
