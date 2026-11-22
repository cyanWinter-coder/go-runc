//go:build !windows

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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"
)

// scriptRunc writes an executable shell script and returns a Runc whose
// Command points at it, plus a cleanup func.
func scriptRunc(t *testing.T, body string) (*Runc, func()) {
	t.Helper()
	fh, err := os.CreateTemp("", "runc-lifecycle-*.sh")
	if err != nil {
		t.Fatalf("create temp script: %s", err)
	}
	if _, err := fh.WriteString("#!/bin/sh\n" + body); err != nil {
		fh.Close()
		os.Remove(fh.Name())
		t.Fatalf("write temp script: %s", err)
	}
	if err := fh.Close(); err != nil {
		os.Remove(fh.Name())
		t.Fatalf("close temp script: %s", err)
	}
	if err := os.Chmod(fh.Name(), 0o755); err != nil {
		os.Remove(fh.Name())
		t.Fatalf("chmod temp script: %s", err)
	}
	return &Runc{Command: fh.Name(), KillGracePeriod: time.Second}, func() { os.Remove(fh.Name()) }
}

// TestWaitIdempotent verifies that a process publishes exactly one exit
// result: repeated Wait calls on the same command return identical status and
// error, never an empty/zero status from a consumed channel.
func TestWaitIdempotent(t *testing.T) {
	mon := &defaultMonitor{}
	cmd := exec.Command("/bin/false")
	ec, err := mon.Start(cmd)
	if err != nil {
		t.Fatalf("Start: %s", err)
	}

	const waiters = 8
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		status = make([]int, waiters)
		errs   = make([]error, waiters)
	)
	wg.Add(waiters)
	for i := 0; i < waiters; i++ {
		go func(i int) {
			defer wg.Done()
			s, e := mon.Wait(cmd, ec)
			mu.Lock()
			status[i], errs[i] = s, e
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	for i := 0; i < waiters; i++ {
		if status[i] != 1 {
			t.Fatalf("waiter %d: expected status 1, got %d", i, status[i])
		}
		var exitErr *ExitError
		if !errors.As(errs[i], &exitErr) || exitErr.Status != 1 {
			t.Fatalf("waiter %d: expected *ExitError{1}, got %v", i, errs[i])
		}
	}
}

// TestWaitRepeatedSequential reproduces the original empty-status bug: after
// the first Wait consumes a one-shot result, a second, later Wait on the same
// command must still return the same status rather than a zero-value Exit.
func TestWaitRepeatedSequential(t *testing.T) {
	mon := &defaultMonitor{}

	t.Run("nonzero stays nonzero", func(t *testing.T) {
		cmd := exec.Command("/bin/sh", "-c", "exit 9")
		ec, err := mon.Start(cmd)
		if err != nil {
			t.Fatalf("Start: %s", err)
		}
		first, _ := mon.Wait(cmd, ec)
		second, err2 := mon.Wait(cmd, ec)
		if first != 9 || second != 9 {
			t.Fatalf("expected status 9 on both waits, got %d then %d", first, second)
		}
		var ee *ExitError
		if !errors.As(err2, &ee) || ee.Status != 9 {
			t.Fatalf("second wait lost its error: %v", err2)
		}
	})

	t.Run("zero stays zero", func(t *testing.T) {
		cmd := exec.Command("/bin/true")
		ec, err := mon.Start(cmd)
		if err != nil {
			t.Fatalf("Start: %s", err)
		}
		if s, e := mon.Wait(cmd, ec); s != 0 || e != nil {
			t.Fatalf("first wait: expected (0,nil), got (%d,%v)", s, e)
		}
		if s, e := mon.Wait(cmd, ec); s != 0 || e != nil {
			t.Fatalf("second wait: expected (0,nil), got (%d,%v)", s, e)
		}
	})
}

// TestExitErrorClassification checks that clean, non-zero and signal exits
// produce distinguishable errors, all drawn from the same reclamation.
func TestExitErrorClassification(t *testing.T) {
	mon := &defaultMonitor{}

	t.Run("clean", func(t *testing.T) {
		cmd := exec.Command("/bin/true")
		ec, err := mon.Start(cmd)
		if err != nil {
			t.Fatalf("Start: %s", err)
		}
		status, err := mon.Wait(cmd, ec)
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if status != 0 {
			t.Fatalf("expected status 0, got %d", status)
		}
	})

	t.Run("nonzero", func(t *testing.T) {
		cmd := exec.Command("/bin/sh", "-c", "exit 7")
		ec, err := mon.Start(cmd)
		if err != nil {
			t.Fatalf("Start: %s", err)
		}
		status, err := mon.Wait(cmd, ec)
		var exitErr *ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("expected *ExitError, got %T: %v", err, err)
		}
		if exitErr.Status != 7 || status != 7 {
			t.Fatalf("expected status 7, got status=%d err=%v", status, err)
		}
		var sigErr *SignalError
		if errors.As(err, &sigErr) {
			t.Fatal("non-zero exit must not be reported as a signal exit")
		}
	})

	t.Run("signal", func(t *testing.T) {
		cmd := exec.Command("/bin/sh", "-c", "kill -TERM $$")
		ec, err := mon.Start(cmd)
		if err != nil {
			t.Fatalf("Start: %s", err)
		}
		status, err := mon.Wait(cmd, ec)
		var sigErr *SignalError
		if !errors.As(err, &sigErr) {
			t.Fatalf("expected *SignalError, got %T: %v", err, err)
		}
		if sigErr.Signal != syscall.SIGTERM {
			t.Fatalf("expected SIGTERM, got %s", sigErr.Signal)
		}
		if status != -1 {
			t.Fatalf("expected status -1 for signal exit, got %d", status)
		}
	})
}

// TestStartFailure verifies a command that cannot start yields a
// *StartError and never enters the reaper map.
func TestStartFailure(t *testing.T) {
	mon := &defaultMonitor{}
	cmd := exec.Command("/nonexistent/binary-that-does-not-exist")
	_, err := mon.Start(cmd)
	var startErr *StartError
	if !errors.As(err, &startErr) {
		t.Fatalf("expected *StartError, got %T: %v", err, err)
	}
	if _, loaded := mon.handles.Load(cmd); loaded {
		t.Fatal("a failed start must not register a reaper handle")
	}

	// Through the public API too.
	r := &Runc{Command: "/nonexistent/binary-that-does-not-exist"}
	_, err = r.Run(context.Background(), "id", "bundle", &CreateOpts{})
	if !errors.As(err, &startErr) {
		t.Fatalf("Run: expected *StartError, got %T: %v", err, err)
	}
}

// TestCancelGracefulTermination cancels a context while a process sleeps and
// expects graceful SIGTERM termination to be reported as a signal exit rather
// than a success or a generic failure.
func TestCancelGracefulTermination(t *testing.T) {
	r, cleanup := scriptRunc(t, "sleep 30\n")
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	status, err := r.Run(ctx, "id", "bundle", &CreateOpts{})
	elapsed := time.Since(start)

	var sigErr *SignalError
	if !errors.As(err, &sigErr) {
		t.Fatalf("expected *SignalError on graceful cancel, got %T: %v", err, err)
	}
	if sigErr.Signal != syscall.SIGTERM {
		t.Fatalf("expected SIGTERM, got %s", sigErr.Signal)
	}
	if status != -1 {
		t.Fatalf("expected status -1, got %d", status)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("graceful termination took too long: %s", elapsed)
	}
}

// TestCancelRetainsOutputTail verifies the close-ordering rule with custom
// pipe IO: termination happens before any pipe teardown, so data already
// written before cancellation remains readable on the caller-owned read end,
// and the bounded WaitDelay still guarantees Run returns.
func TestCancelRetainsOutputTail(t *testing.T) {
	// Emit a marker, then ignore SIGTERM and park. The runtime must escalate
	// to SIGKILL after KillGracePeriod while preserving the marker.
	r, cleanup := scriptRunc(t, "echo tail-marker\ntrap '' TERM\nsleep 30\n")
	defer cleanup()

	pipeIO, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Give the child time to write the marker and reach sleep.
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, e := r.Run(ctx, "id", "bundle", &CreateOpts{IO: pipeIO})
		done <- e
	}()

	// Read the tail from the caller-owned read end. Ownership rule: the read
	// end is closed by its owner (this test), never by exec.Wait, so close it
	// after Run returns to signal EOF to the copier.
	var out bytes.Buffer
	readerDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&out, pipeIO.Stdout())
		close(readerDone)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within bounded grace period")
	}
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("bounded kill took too long: %s", elapsed)
	}
	// Now that the process is reaped, release the caller-owned read end.
	_ = pipeIO.Stdout().Close()
	select {
	case <-readerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("tail data was not flushed to the caller-owned read end")
	}
	if got := out.String(); got != "tail-marker\n" {
		t.Fatalf("expected tail output to be preserved, got %q", got)
	}
}

// TestCancelBufferPathTail covers the internal bytes.Buffer path (no custom
// IO), ensuring the same ownership/ordering rules apply.
func TestCancelBufferPathTail(t *testing.T) {
	r, cleanup := scriptRunc(t, "echo buffered-tail\ntrap '' TERM\nsleep 30\n")
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, e := r.Run(ctx, "id", "bundle", &CreateOpts{})
		done <- e
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error from canceled command")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within bounded grace period")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("bounded kill did not trigger in time")
	}
}

// TestCancelRacesNaturalExit hammers the exact scenario from the report: a
// context cancellation landing at the same instant the process exits on its
// own. Across many iterations the caller must always receive a definite
// outcome (a real non-zero status, a signal error, or a context/wait error)
// and never an empty zero status with a nil error.
func TestCancelRacesNaturalExit(t *testing.T) {
	r, cleanup := scriptRunc(t, "exit 3\n")
	defer cleanup()
	r.KillGracePeriod = 250 * time.Millisecond

	const iterations = 60
	for i := 0; i < iterations; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel at roughly the moment the short-lived process is exiting. The
		// jitter ensures we exercise both orderings across iterations. Compute
		// the delay outside the goroutine so the closure shares no mutable
		// loop state.
		delay := time.Duration(i%5) * time.Millisecond
		go func() {
			time.Sleep(delay)
			cancel()
		}()

		status, err := r.Run(ctx, "id", "bundle", &CreateOpts{})
		cancel()

		// The process always exits 3 unless the cancellation signal wins. In
		// neither ordering can the caller observe an unexplained zero status:
		// the only way to get status 0 is a genuinely clean exit, which this
		// process cannot produce.
		if status == 0 {
			t.Fatalf("iteration %d: got empty/zero status from a process that exits 3 (err=%v)", i, err)
		}
	}
}

// TestCustomIOOwnership makes sure custom IO and standard pipe IO share the
// same ownership rules: CloseAfterStart is invoked once and the process is
// still reaped even when that close reports an error.
func TestCustomIOOwnership(t *testing.T) {
	r, cleanup := scriptRunc(t, "trap '' TERM\nsleep 30\n")
	defer cleanup()
	r.KillGracePeriod = 200 * time.Millisecond

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open devnull: %s", err)
	}
	ioImpl := &failingStartCloser{devNull: devNull}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, e := r.Run(ctx, "id", "bundle", &CreateOpts{IO: ioImpl})
		done <- e
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the CloseAfterStart error to be surfaced")
		}
		if !errors.Is(err, errCloseAfterStart) {
			t.Fatalf("expected joined close error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run hung: process was leaked after CloseAfterStart failure")
	}
	if ioImpl.closeCount != 1 {
		t.Fatalf("expected CloseAfterStart called once, got %d", ioImpl.closeCount)
	}
}

var errCloseAfterStart = errors.New("simulated CloseAfterStart failure")

type failingStartCloser struct {
	closeCount int
	devNull    *os.File
}

func (f *failingStartCloser) Close() error           { return f.devNull.Close() }
func (f *failingStartCloser) Stdin() io.WriteCloser  { return nil }
func (f *failingStartCloser) Stdout() io.ReadCloser  { return nil }
func (f *failingStartCloser) Stderr() io.ReadCloser  { return nil }
func (f *failingStartCloser) Set(c *exec.Cmd)        { c.Stdout = f.devNull; c.Stderr = f.devNull }
func (f *failingStartCloser) CloseAfterStart() error { f.closeCount++; return errCloseAfterStart }

// TestCustomMonitorFallback keeps the channel-based fallback working for
// ProcessMonitor implementations outside this package's default monitor.
func TestCustomMonitorFallback(t *testing.T) {
	prev := Monitor
	defer func() { Monitor = prev }()
	Monitor = &channelMonitor{}

	r := &Runc{Command: "/bin/sh"}
	cmd := r.command(context.Background(), "-c", "exit 3")
	ec, err := Monitor.Start(cmd)
	if err != nil {
		t.Fatalf("Start: %s", err)
	}
	status, err := Monitor.Wait(cmd, ec)
	if err != nil {
		t.Fatalf("custom monitor Wait: %s", err)
	}
	if status != 3 {
		t.Fatalf("expected status 3, got %d", status)
	}
}

// channelMonitor mimics an external monitor that only speaks the channel
// protocol and never registers a handle.
type channelMonitor struct{}

func (channelMonitor) Start(c *exec.Cmd) (chan Exit, error) {
	if err := c.Start(); err != nil {
		return nil, err
	}
	ec := make(chan Exit, 1)
	go func() {
		err := c.Wait()
		status := 0
		if err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				status = ee.ExitCode()
			} else {
				status = 255
			}
		}
		ec <- Exit{Pid: c.Process.Pid, Status: status}
		close(ec)
	}()
	return ec, nil
}

func (channelMonitor) StartLocked(c *exec.Cmd) (chan Exit, error) {
	return channelMonitor{}.Start(c)
}

func (channelMonitor) Wait(_ *exec.Cmd, ec chan Exit) (int, error) {
	e := <-ec
	return e.Status, nil
}
