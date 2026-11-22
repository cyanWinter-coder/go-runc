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
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// socketTestHelperEnv is set when the test binary re-executes itself to act as
// an independent process that creates sockets or holds locks and then exits.
const socketTestHelperEnv = "GORUNC_SOCKET_TEST_HELPER"

// TestMain dispatches the re-executed helper processes; every other test runs
// in the normal test binary.
func TestMain(m *testing.M) {
	switch mode := os.Getenv(socketTestHelperEnv); mode {
	case "":
		os.Exit(m.Run())
	default:
		runSocketHelper(mode)
	}
}

// runSocketHelper is the entry point of re-executed helper processes. It must
// not call t.Fatal: failures exit non-zero so the parent test observes them.
func runSocketHelper(mode string) {
	path := os.Getenv("GORUNC_SOCKET_TEST_PATH")
	if path == "" {
		os.Exit(2)
	}
	switch mode {
	case "stale":
		// Create and publish a socket, then exit without Close to simulate a
		// crash. The kernel drops the flock and the listener but leaves both
		// pathnames on disk.
		s, err := newSocket(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_ = s
		os.Exit(0)
	case "bindcrash":
		// Bind the socket and acquire the lock, but never write the owner
		// record, then exit: the bind-to-record window of a crash.
		lock, err := acquireSocketLock(path + lockSuffix)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		addr, err := net.ResolveUnixAddr("unix", path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		l, err := net.ListenUnix("unix", addr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		l.SetUnlinkOnClose(false)
		_ = lock
		os.Exit(0)
	case "hold":
		// Keep a live socket until the parent kills us.
		s, err := newSocket(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_ = s
		time.Sleep(30 * time.Second)
	case "lock":
		// Hold only the lock file, without binding a socket.
		f, err := acquireSocketLock(path + lockSuffix)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_ = f
		time.Sleep(30 * time.Second)
	default:
		os.Exit(2)
	}
}

// startSocketHelper re-executes the test binary in the given helper mode with
// path as the socket path under test. The returned cleanup waits for (or
// kills) the process.
func startSocketHelper(t *testing.T, mode, path string) func() {
	t.Helper()
	cmd := helperCommand(mode, path)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s helper: %v", mode, err)
	}
	var (
		doneMu    sync.Mutex
		finished  bool
		finishErr error
		finishSig = make(chan struct{})
	)
	go func() {
		err := cmd.Wait()
		doneMu.Lock()
		finished, finishErr = true, err
		doneMu.Unlock()
		close(finishSig)
	}()
	helperFinished := func() (bool, error) {
		doneMu.Lock()
		defer doneMu.Unlock()
		return finished, finishErr
	}

	wantPath := path
	if mode == "lock" {
		wantPath = path + lockSuffix
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(wantPath); err == nil {
			break
		}
		if done, err := helperFinished(); done {
			t.Fatalf("%s helper exited before publishing %q: %v", mode, wantPath, err)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s helper did not publish %q within the timeout", mode, wantPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if mode == "stale" || mode == "bindcrash" {
		// These helpers simulate a crash: each must have fully exited,
		// leaving only the pathnames behind.
		select {
		case <-finishSig:
			if _, err := helperFinished(); err != nil {
				t.Fatalf("%s helper failed: %v", mode, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s helper did not exit", mode)
		}
	}

	return func() {
		// The "stale" helper exits on its own and is already reaped; the
		// "hold"/"lock" helpers are killed and then awaited.
		if done, _ := helperFinished(); !done {
			_ = cmd.Process.Kill()
		}
		select {
		case <-finishSig:
		case <-time.After(5 * time.Second):
			t.Fatalf("helper process %d did not exit", cmd.Process.Pid)
		}
	}
}

func helperCommand(mode, path string) *exec.Cmd {
	// -test.run=^$ matches none of the real tests; TestMain still runs and
	// dispatches to the helper mode via the environment.
	cmd := exec.Command(os.Args[0], "-test.run=^$") // #nosec G204 G702 -- re-executes the test binary itself with fixed flags and a test-controlled socket path.
	cmd.Env = append(os.Environ(),
		socketTestHelperEnv+"="+mode,
		"GORUNC_SOCKET_TEST_PATH="+path,
	)
	return cmd
}

func lockPathOf(path string) string { return path + lockSuffix }

func TestSocketCreateAndClose(t *testing.T) {
	path := testSocketPath(t)

	s, err := NewConsoleSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Path() != path {
		t.Fatalf("expected path %q, got %q", path, s.Path())
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("socket not published: %v", err)
	}
	if _, err := os.Stat(path + lockSuffix); err != nil {
		t.Fatalf("lock file not published: %v", err)
	}

	record := mustReadOwnerRecord(t, path+lockSuffix)
	if record.Pid != os.Getpid() {
		t.Fatalf("owner record pid = %d, want %d", record.Pid, os.Getpid())
	}
	if record.UID != os.Geteuid() {
		t.Fatalf("owner record uid = %d, want %d", record.UID, os.Geteuid())
	}
	if record.SocketInode != mustInode(t, path) {
		t.Fatal("owner record socket inode does not match the bound socket")
	}
	if record.StartTime == 0 {
		t.Fatal("owner record has no process start time")
	}

	// The published socket accepts connections like any unix listener.
	if !socketIsLive(path) {
		t.Fatal("newly created socket reports as not live")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertGone(t, path)
	assertGone(t, path+lockSuffix)

	// Close is idempotent.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestSocketReuseAfterCleanClose(t *testing.T) {
	path := testSocketPath(t)

	first, err := NewPidfdSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := NewPidfdSocket(path)
	if err != nil {
		t.Fatalf("cannot reuse a cleanly closed path: %v", err)
	}
	defer second.Close()
	if !socketIsLive(path) {
		t.Fatal("rebound socket is not live")
	}
}

func TestSocketCrashTakeover(t *testing.T) {
	path := testSocketPath(t)
	cleanup := startSocketHelper(t, "stale", path)
	defer cleanup()

	// The helper has exited: both pathnames remain but nothing listens.
	assertExists(t, path)
	assertExists(t, path+lockSuffix)
	if socketIsLive(path) {
		t.Fatal("crashed socket should not accept connections")
	}
	staleInode := mustInode(t, path)

	s, err := NewPidfdSocket(path)
	if err != nil {
		t.Fatalf("stale path from a crashed owner must be taken over, got: %v", err)
	}
	defer s.Close()

	if mustInode(t, path) == staleInode {
		t.Fatal("takeover rebound onto the stale inode instead of replacing it")
	}
	if !socketIsLive(path) {
		t.Fatal("socket taken over from a crash is not live")
	}
	record := mustReadOwnerRecord(t, path+lockSuffix)
	if record.Pid != os.Getpid() {
		t.Fatalf("owner record still names the crashed pid %d", record.Pid)
	}
}

func TestSocketLiveListenerIsRefused(t *testing.T) {
	path := testSocketPath(t)
	cleanup := startSocketHelper(t, "hold", path)
	defer cleanup()

	_, err := NewConsoleSocket(path)
	if !errors.Is(err, ErrSocketInUse) {
		t.Fatalf("expected ErrSocketInUse for a live listener, got: %v", err)
	}
	// The live listener must keep working and keep both pathnames.
	if !socketIsLive(path) {
		t.Fatal("refused startup disturbed the live listener")
	}
	assertExists(t, path)
	assertExists(t, lockPathOf(path))
}

func TestSocketLockHeldByLiveProcessIsRefused(t *testing.T) {
	path := testSocketPath(t)
	cleanup := startSocketHelper(t, "lock", path)
	defer cleanup()

	_, err := NewPidfdSocket(path)
	if !errors.Is(err, ErrSocketInUse) {
		t.Fatalf("expected ErrSocketInUse while the lock is held, got: %v", err)
	}
}

func TestSocketForeignNonSocketIsNotRemoved(t *testing.T) {
	path := testSocketPath(t)
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewConsoleSocket(path)
	if err == nil {
		t.Fatal("expected an error for a non-socket path")
	}
	data, rErr := os.ReadFile(path)
	if rErr != nil || string(data) != "not a socket" {
		t.Fatalf("foreign path was modified or removed: data=%q err=%v", data, rErr)
	}
}

func TestSocketStaleWithoutOwnerRecordIsRefused(t *testing.T) {
	path := testSocketPath(t)
	// A crashed helper leaves a valid lock file; truncate its record to
	// simulate a path nobody in this trust domain can account for.
	cleanup := startSocketHelper(t, "stale", path)
	defer cleanup()
	if err := os.Truncate(path+lockSuffix, 0); err != nil {
		t.Fatal(err)
	}

	_, err := NewPidfdSocket(path)
	if err == nil {
		t.Fatal("expected refusal when the stale path has no owner record")
	}
	if errors.Is(err, ErrSocketInUse) {
		t.Fatal("an unowned stale path must not be reported as in use")
	}
	assertExists(t, path)
	assertExists(t, path+lockSuffix)
}

func TestSocketOwnerLivenessAcrossPidReuse(t *testing.T) {
	record := socketOwner{
		Pid:         os.Getpid(),
		StartTime:   processStartTime(os.Getpid()),
		UID:         os.Geteuid(),
		SocketInode: 1,
		LockInode:   2,
	}
	if !ownerAlive(record) {
		t.Fatal("current process reported as dead")
	}
	// A different start time at the same pid means the original process died
	// and the pid was reused.
	record.StartTime++
	if ownerAlive(record) {
		t.Fatal("reused pid with a changed start time reported alive")
	}
	// A pid that cannot exist is dead regardless of the start time.
	record.Pid = 1 << 22
	record.StartTime = 0
	if ownerAlive(record) {
		t.Fatal("impossible pid reported alive")
	}
}

func TestSocketConcurrentStartsHaveOneWinner(t *testing.T) {
	path := testSocketPath(t)
	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	sockets := make([]*Socket, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			s, err := NewPidfdSocket(path)
			errs[i] = err
			sockets[i] = s
		}(i)
	}
	wg.Wait()

	winners, losers := 0, 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrSocketInUse):
			losers++
		default:
			t.Fatalf("unexpected error from concurrent start: %v", err)
		}
		if err != nil && sockets[i] != nil {
			t.Fatal("failed start returned a socket")
		}
	}
	if winners != 1 || losers != n-1 {
		t.Fatalf("expected exactly 1 winner and %d losers, got %d winners and %d losers", n-1, winners, losers)
	}
	for _, s := range sockets {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	assertGone(t, path)
	assertGone(t, path+lockSuffix)
}

func TestSocketCloseDoesNotRemoveReplacement(t *testing.T) {
	path := testSocketPath(t)
	// A real helper process creates the socket, publishes its owner record and
	// exits without Close, mimicking a crash. Its flock is released by the
	// kernel while both pathnames remain.
	cleanup := startSocketHelper(t, "stale", path)
	defer cleanup()
	oldSocketInode := mustInode(t, path)
	oldLockInode := mustInode(t, path+lockSuffix)

	// Takeover rebinds a brand new socket (new inode) at the same path. The
	// lock file itself is reused, but its owner record must be rewritten.
	second, err := NewPidfdSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	if mustInode(t, path) == oldSocketInode {
		t.Fatal("takeover rebound onto the crashed socket's inode")
	}
	record := mustReadOwnerRecord(t, path+lockSuffix)
	if record.Pid != os.Getpid() {
		t.Fatalf("takeover kept the crashed owner's record, pid = %d", record.Pid)
	}
	if record.SocketInode != mustInode(t, path) {
		t.Fatal("rewritten record does not name the replacement socket inode")
	}
	if !socketIsLive(path) {
		t.Fatal("replacement socket is not live")
	}

	// A stale Socket object left over from the crashed owner carries its old
	// inodes. To exercise both guards at once, give the stale record inodes
	// that no longer match the replacement objects on disk: its cleanup must
	// be a complete no-op.
	stale := &Socket{
		l:           nil,
		path:        path,
		lockPath:    path + lockSuffix,
		lockFile:    nil,
		socketInode: oldSocketInode,
		lockInode:   oldLockInode + 1,
	}
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	if !socketIsLive(path) {
		t.Fatal("stale cleanup removed the replacement listener")
	}
	assertExists(t, path)
	assertExists(t, path+lockSuffix)

	// The real owner's own Close removes only its objects.
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	assertGone(t, path)
	assertGone(t, path+lockSuffix)
}

func TestSocketBindFailureLeavesNoRecordlessSocket(t *testing.T) {
	// A helper that binds the socket and exits before the owner record is
	// written emulates a crash in the bind-to-record window. Its lock file is
	// empty, so the next startup must refuse rather than guess, and a client
	// can at least see why instead of hitting a silent stale endpoint.
	path := testSocketPath(t)
	cleanup := startSocketHelper(t, "bindcrash", path)
	defer cleanup()

	assertExists(t, path)
	assertExists(t, path+lockSuffix)
	if socketIsLive(path) {
		t.Fatal("crashed socket should not accept connections")
	}
	_, err := NewPidfdSocket(path)
	if err == nil {
		t.Fatal("expected refusal for a socket without an owner record")
	}
	if errors.Is(err, ErrSocketInUse) {
		t.Fatal("a recordless stale socket must not be reported as in use")
	}

	// Manual removal of the unowned pathname is the documented escape hatch;
	// the next startup then binds cleanly and publishes a full record.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s, err := NewConsoleSocket(path)
	if err != nil {
		t.Fatalf("recovery after manual cleanup failed: %v", err)
	}
	defer s.Close()
	record := mustReadOwnerRecord(t, path+lockSuffix)
	if record.Pid != os.Getpid() || record.SocketInode != mustInode(t, path) {
		t.Fatal("recovered socket has an inconsistent owner record")
	}
}

func TestTempSocketLifecycle(t *testing.T) {
	s, err := NewTempConsoleSocket()
	if err != nil {
		t.Fatal(err)
	}
	path := s.Path()
	assertExists(t, path)
	assertExists(t, path+lockSuffix)
	ensureSocketCleanup(t, s, path)
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("temp directory %q still exists", filepath.Dir(path))
	}
}

func TestSocketFdTransferStillWorks(t *testing.T) {
	// End-to-end: the ownership machinery must not interfere with the actual
	// purpose of the socket, receiving an fd from runc (simulated by sendFd).
	s, err := NewPidfdSocket(testSocketPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	go sendFd(t, s.Path(), "standard", openPidfd(t, os.Getpid()))
	f, err := s.ReceivePidfd()
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if pid := pidfdTargetPid(t, f); pid != os.Getpid() {
		t.Fatalf("expected pidfd for %d, got %d", os.Getpid(), pid)
	}
}

// helpers -------------------------------------------------------------------

func mustReadOwnerRecord(t *testing.T, lockPath string) socketOwner {
	t.Helper()
	f, err := os.Open(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	record, err := readOwnerRecord(f)
	if err != nil {
		t.Fatalf("read owner record: %v", err)
	}
	return record
}

func mustInode(t *testing.T, path string) uint64 {
	t.Helper()
	inode, err := pathInode(path)
	if err != nil {
		t.Fatal(err)
	}
	return inode
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %q to exist: %v", path, err)
	}
}

func assertGone(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %q to be removed, stat err = %v", path, err)
	}
}
