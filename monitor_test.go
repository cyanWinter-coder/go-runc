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
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// newTestHandle starts cmd through the default monitor's lifecycle interface
// and returns its single terminal-state handle.
func newTestHandle(t *testing.T, ctx context.Context, name string, args ...string) ProcessHandle {
	t.Helper()
	mon, ok := Monitor.(ProcessLifecycle)
	if !ok {
		t.Fatalf("default monitor does not implement ProcessLifecycle")
	}
	cmd := exec.CommandContext(ctx, name, args...)
	h, err := mon.StartProcess(cmd, startOptions{ctx: ctx, gracePeriod: 2 * time.Second})
	if err != nil {
		t.Fatalf("StartProcess failed: %v", err)
	}
	return h
}

// TestHandleNaturalExit verifies a normal, non-zero exit is reported with its
// real status and a wait error from the same single reaping call.
func TestHandleNaturalExit(t *testing.T) {
	t.Run("exit zero", func(t *testing.T) {
		h := newTestHandle(t, context.Background(), "/bin/true")
		status, err := h.Wait()
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
	})

	t.Run("exit non-zero keeps real code", func(t *testing.T) {
		h := newTestHandle(t, context.Background(), "/bin/sh", "-c", "exit 42")
		status, err := h.Wait()
		if err == nil {
			t.Fatal("expected an error for non-zero exit")
		}
		if status != 42 {
			t.Fatalf("expected status 42, got %d", status)
		}
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("expected *exec.ExitError from the reaper, got %T", err)
		}
	})
}

// TestHandleWaitIdempotent verifies repeated and concurrent Wait calls all
// observe the same single terminal result and never block.
func TestHandleWaitIdempotent(t *testing.T) {
	h := newTestHandle(t, context.Background(), "/bin/sh", "-c", "exit 7")

	const waiters = 32
	var wg sync.WaitGroup
	results := make(chan [2]interface{}, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, err := h.Wait()
			results <- [2]interface{}{status, err == nil}
		}()
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r[0] != 7 {
			t.Fatalf("expected every Wait to return status 7, got %v", r[0])
		}
		if r[1] != false {
			t.Fatal("expected every Wait to return the same non-nil error")
		}
	}

	// A sequential repeat after completion must return immediately too.
	status, err := h.Wait()
	if status != 7 || err == nil {
		t.Fatalf("repeated Wait diverged: status=%d err=%v", status, err)
	}
}

// TestHandleStopIdempotent verifies concurrent and repeated Stop calls
// terminate the process at most once and converge on one terminal result.
func TestHandleStopIdempotent(t *testing.T) {
	h := newTestHandle(t, context.Background(), "/bin/sleep", "30")

	const stoppers = 16
	var wg sync.WaitGroup
	errs := make(chan error, stoppers)
	for i := 0; i < stoppers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- h.Stop()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("Stop on a signaled process should report the signal error")
		}
	}

	status, err := h.Wait()
	if err == nil {
		t.Fatal("expected terminal error after Stop")
	}
	// SIGTERM is signal 15, so the shell-convention status is 143.
	if status != 128+int(syscall.SIGTERM) {
		t.Fatalf("expected status %d, got %d", 128+int(syscall.SIGTERM), status)
	}
	// Repeated Stop after the terminal state must be a harmless no-op.
	if err := h.Stop(); err == nil {
		t.Fatal("repeated Stop should keep reporting the terminal error")
	}
}

// TestHandleStopDoesNotOverrideNaturalExit is the core race test: when the
// process exits naturally at the same time Stop is requested, the real exit
// information must win and never be replaced by a generic signal/timeout.
func TestHandleStopDoesNotOverrideNaturalExit(t *testing.T) {
	// The helper waits until it is signaled, then exits 42 naturally. Stop's
	// SIGTERM and the natural exit therefore happen at the same instant: if
	// the signal wins the process dies by signal (143), if the exit wins the
	// real code 42 is observed. The reaper must always report a stable,
	// single result and never a generic timeout/empty code.
	script := writeExecutable(t, "#!/bin/sh\ntrap 'exit 42' TERM\nsleep 30\n")
	for trial := 0; trial < 50; trial++ {
		h := newTestHandle(t, context.Background(), script)
		waitUntilRunning(t, h)
		_ = h.Stop()
		status, _ := h.Wait()
		if status != 42 && status != 128+int(syscall.SIGTERM) {
			t.Fatalf("trial %d: terminal status must be the natural code 42 or a"+
				" definite SIGTERM 143, got %d", trial, status)
		}
	}
}

// writeExecutable writes content to a temporary executable file and returns
// its path.
func writeExecutable(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.sh")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f.Name(), 0o755); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

// waitUntilRunning blocks until the handle's process is observed alive, so a
// subsequent Stop races a genuinely running process rather than start-up.
func waitUntilRunning(t *testing.T, h ProcessHandle) {
	t.Helper()
	ph := h.(*processHandle)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(ph.cmd.Process.Pid, 0); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("process never became running")
}

// waitForFile blocks until path exists, synchronizing on a child process's
// readiness marker rather than a fixed sleep.
func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for readiness file %s", path)
}

// TestHandleBoundedTermination verifies a process that ignores the graceful
// signal is forcefully killed within the configured grace period, and that
// context cancellation drives the same bounded path while preserving the
// cancellation cause.
func TestHandleBoundedTermination(t *testing.T) {
	t.Run("stop escalates after grace period", func(t *testing.T) {
		mon := Monitor.(ProcessLifecycle)
		// The helper installs its TERM-ignoring trap and only then signals
		// readiness, so Stop cannot race trap installation.
		ready := filepath.Join(t.TempDir(), "ready")
		script := writeExecutable(t, "#!/bin/sh\n"+
			"trap '' TERM\n"+
			"touch '"+ready+"'\n"+
			"sleep 30\n")
		cmd := exec.CommandContext(context.Background(), script)
		h, err := mon.StartProcess(cmd, startOptions{ctx: context.Background(), gracePeriod: 300 * time.Millisecond})
		if err != nil {
			t.Fatalf("StartProcess failed: %v", err)
		}
		waitForFile(t, ready)
		start := time.Now()
		if err := h.Stop(); err == nil {
			t.Fatal("expected error from forcefully killed process")
		}
		elapsed := time.Since(start)
		if elapsed > 2*time.Second {
			t.Fatalf("bounded termination took too long: %v", elapsed)
		}
		status, _ := h.Wait()
		// Escalation uses SIGKILL (signal 9), so status must be 137.
		if status != 128+int(syscall.SIGKILL) {
			t.Fatalf("expected SIGKILL status %d, got %d", 128+int(syscall.SIGKILL), status)
		}
	})

	t.Run("context cancellation preserves cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		h := newTestHandle(t, ctx, "/bin/sleep", "30")
		cancel()
		status, err := h.Wait()
		if err == nil {
			t.Fatal("expected cancellation error")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected error to wrap context.Canceled, got %v", err)
		}
		if status != 128+int(syscall.SIGTERM) {
			t.Fatalf("expected SIGTERM status %d, got %d", 128+int(syscall.SIGTERM), status)
		}
	})

	t.Run("deadline expiry preserves cause", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		h := newTestHandle(t, ctx, "/bin/sleep", "30")
		_, err := h.Wait()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
	})
}

// TestHandleDeleteWaitsAndCleansUp verifies Delete never removes state
// underneath a live process, runs exactly once, and removes the state
// directory and registered cleanup resources after the terminal state.
func TestHandleDeleteWaitsAndCleansUp(t *testing.T) {
	mon := Monitor.(ProcessLifecycle)

	stateDir := t.TempDir()
	marker := filepath.Join(stateDir, "runc-process-xyz")
	if err := os.WriteFile(marker, []byte("spec"), 0o600); err != nil {
		t.Fatal(err)
	}
	cleaned := make(chan struct{}, 1)
	cmd := exec.CommandContext(context.Background(), "/bin/sleep", "30")
	h, err := mon.StartProcess(cmd, startOptions{
		stateDir:    stateDir,
		gracePeriod: time.Second,
		cleanup:     func() { cleaned <- struct{}{} },
	})
	if err != nil {
		t.Fatalf("StartProcess failed: %v", err)
	}

	// Delete must block while the process is still running.
	done := make(chan struct{})
	go func() {
		_ = h.Delete()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Delete returned before the process reached its terminal state")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := os.Stat(stateDir); err != nil {
		t.Fatalf("state dir removed while process running: %v", err)
	}

	if err := h.Stop(); err == nil {
		t.Fatal("expected Stop error")
	}
	<-done

	if _, err := os.Stat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected state dir removed after Delete, stat err=%v", err)
	}
	select {
	case <-cleaned:
	default:
		t.Fatal("registered cleanup was not invoked")
	}

	// Repeated Delete is idempotent and returns without error.
	if err := h.Delete(); err != nil {
		t.Fatalf("repeated Delete should be a no-op, got %v", err)
	}
}

// TestHandleStartFailureCleansState verifies a failed start releases the
// command state immediately and leaves a terminal handle rather than a
// half-started record.
func TestHandleStartFailureCleansState(t *testing.T) {
	mon := Monitor.(ProcessLifecycle)
	stateDir := t.TempDir()

	cmd := exec.CommandContext(context.Background(), "/path/that/does/not/exist")
	h, err := mon.StartProcess(cmd, startOptions{
		stateDir:    stateDir,
		gracePeriod: time.Second,
		cleanup:     func() {},
	})
	if err == nil {
		t.Fatal("expected StartProcess to fail for a missing binary")
	}
	if h != nil {
		t.Fatal("expected nil handle on start failure")
	}
	// Start failure runs the bounded cleanup and leaves no orphaned state.
	if _, statErr := os.Stat(stateDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected state dir removed on start failure, got %v", statErr)
	}
}

// TestHandleIsolation verifies one command's Stop/Delete never affects an
// unrelated concurrent command.
func TestHandleIsolation(t *testing.T) {
	victim := newTestHandle(t, context.Background(), "/bin/sleep", "2")
	stopped := newTestHandle(t, context.Background(), "/bin/sleep", "30")
	waitUntilRunning(t, victim)
	waitUntilRunning(t, stopped)

	if err := stopped.Stop(); err == nil {
		t.Fatal("expected Stop error")
	}

	// The victim must still be alive and reach its own natural terminal state.
	status, err := victim.Wait()
	if err != nil {
		t.Fatalf("unrelated command was affected: %v", err)
	}
	if status != 0 {
		t.Fatalf("expected victim status 0, got %d", status)
	}
	if err := victim.Delete(); err != nil {
		t.Fatalf("victim Delete failed: %v", err)
	}
}

// TestRunCancellationTerminal verifies the public Run path reports the
// bounded-termination status and wrapped context cause through its
// compatible (status, error) signature.
func TestRunCancellationTerminal(t *testing.T) {
	dummy, err := dummySleepRunc()
	if err != nil {
		t.Fatalf("Failed to create dummy runc: %s", err)
	}
	defer os.Remove(dummy)
	r := &Runc{Command: dummy}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	status, err := r.Run(ctx, "fake-id", "fake-bundle", &CreateOpts{})
	if err == nil {
		t.Fatal("expected cancellation error from Run")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error should wrap context.Canceled, got %v", err)
	}
	if status != 128+int(syscall.SIGTERM) {
		t.Fatalf("expected SIGTERM status %d from Run, got %d", 128+int(syscall.SIGTERM), status)
	}
}

// TestExecCancellationCleansStateDir verifies the per-command state
// directory holding the exec process spec is removed once a canceled Exec
// reaches its terminal state: no half-started record survives a
// cancellation.
func TestExecCancellationCleansStateDir(t *testing.T) {
	dummy, err := dummySleepRunc()
	if err != nil {
		t.Fatalf("Failed to create dummy runc: %s", err)
	}
	defer os.Remove(dummy)

	runtimeDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	r := &Runc{Command: dummy}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan int, 1)
	go func() {
		<-started
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_ = r.Exec(ctx, "fake-id", specs.Process{}, &ExecOpts{Started: started})

	entries, err := os.ReadDir(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "runc-cmd") {
			t.Fatalf("state directory %q survived a canceled Exec", e.Name())
		}
	}
}

// TestExecNaturalExitCleansStateDir verifies the state directory is also
// removed on the normal completion path.
func TestExecNaturalExitCleansStateDir(t *testing.T) {
	r := &Runc{Command: "/bin/true"}
	runtimeDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)

	if err := r.Exec(context.Background(), "fake-id", specs.Process{}, &ExecOpts{}); err != nil {
		t.Fatalf("Exec failed: %v", err)
	}
	entries, err := os.ReadDir(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "runc-cmd") {
			t.Fatalf("state directory %q survived a completed Exec", e.Name())
		}
	}
}
