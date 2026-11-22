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
	"os/exec"
	"runtime"
	"sync"
	"time"
)

// Monitor is the default ProcessMonitor for handling runc process exit
var Monitor ProcessMonitor = &defaultMonitor{}

// Exit holds the exit information from a process.
//
// A process publishes exactly one Exit value: Status and Err both come from
// the same process reclamation, so a caller never sees a status without the
// error that explains it (or vice versa).
type Exit struct {
	Timestamp time.Time
	Pid       int
	Status    int
	// Err is the error observed while reclaiming the process. It is nil for a
	// clean exit, otherwise it is one of the package's distinguishable error
	// types (or wraps one): *StartError, *ExitError, *SignalError, or an I/O /
	// context error such as [exec.ErrWaitDelay].
	Err error
}

// ProcessMonitor is an interface for process monitoring.
//
// It allows daemons using go-runc to have a SIGCHLD handler
// to handle exits without introducing races between the handler
// and go's exec.Cmd.
//
// ProcessMonitor also provides a StartLocked method which is similar to
// Start, but locks the goroutine used to start the process to an OS thread
// (for example: when Pdeathsig is set).
type ProcessMonitor interface {
	Start(*exec.Cmd) (chan Exit, error)
	StartLocked(*exec.Cmd) (chan Exit, error)
	Wait(*exec.Cmd, chan Exit) (int, error)
}

// StartError reports that a process could not be started, so it was never
// reaped (there is no exit status or signal).
type StartError struct {
	// Path is the command that failed to start.
	Path string
	Err  error
}

func (e *StartError) Error() string {
	return "failed to start " + e.Path + ": " + e.Err.Error()
}

func (e *StartError) Unwrap() error { return e.Err }

// ExitError holds the status return code when a process exits with a non-zero
// exit status on its own (i.e. it was not terminated by a signal).
type ExitError struct {
	Status int
}

func (e *ExitError) Error() string {
	return exitStatusString(e.Status)
}

// processHandle owns the lifecycle of a single started *exec.Cmd. The reaper
// goroutine calls cmd.Wait exactly once and caches the resulting Exit; every
// subsequent Wait returns that cached value, making repeated waits idempotent.
type processHandle struct {
	cmd   *exec.Cmd
	ready chan struct{}
	exit  Exit
}

// newProcessHandle starts the reaper for an already-started command. The
// reaper is the sole caller of cmd.Wait, guaranteeing one exit result per
// process with status and error drawn from the same reclamation.
func newProcessHandle(cmd *exec.Cmd) *processHandle {
	h := &processHandle{
		cmd:   cmd,
		ready: make(chan struct{}),
	}
	go h.reap()
	return h
}

func (h *processHandle) reap() {
	// Defer the single publication so an unexpected panic in classification
	// still closes ready and cannot leave waiters blocked forever.
	defer close(h.ready)

	status, err := reapProcess(h.cmd)
	h.exit = Exit{
		Timestamp: time.Now(),
		Pid:       processPID(h.cmd),
		Status:    status,
		Err:       err,
	}
}

// result blocks until the single exit result is available and returns it.
func (h *processHandle) result() Exit {
	<-h.ready
	return h.exit
}

type defaultMonitor struct {
	// handles maps each started command to its single owner/reaper. Entries are
	// retained after exit (rather than deleted) so that repeated and concurrent
	// Wait calls on the same command stay idempotent: a consumable channel
	// could only hand the result to the first waiter. The cost is one small
	// entry per started command, which matches exec.Cmd itself being single
	// use and not reusable across invocations.
	handles sync.Map // map[*exec.Cmd]*processHandle
}

func (m *defaultMonitor) register(cmd *exec.Cmd) {
	m.handles.Store(cmd, newProcessHandle(cmd))
}

func (m *defaultMonitor) Start(c *exec.Cmd) (chan Exit, error) {
	if err := c.Start(); err != nil {
		return nil, &StartError{Path: commandPath(c), Err: err}
	}
	m.register(c)
	return make(chan Exit, 1), nil
}

// StartLocked is like Start, but locks the goroutine used to start the process to
// the OS thread for use-cases where the parent thread matters to the child process
// (for example: when Pdeathsig is set).
func (m *defaultMonitor) StartLocked(c *exec.Cmd) (chan Exit, error) {
	started := make(chan error)
	ec := make(chan Exit, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		if err := c.Start(); err != nil {
			started <- &StartError{Path: commandPath(c), Err: err}
			return
		}
		m.register(c)
		close(started)
	}()
	if err := <-started; err != nil {
		return nil, err
	}
	return ec, nil
}

// Wait returns the exit status and error of a command. It is idempotent: for
// commands started by this monitor the same cached result is returned on every
// call, so concurrent or repeated waits never race with the reaper or observe
// an empty status. The ec channel is retained for compatibility with custom
// ProcessMonitor implementations; the default monitor derives its result from
// the per-command handle instead.
func (m *defaultMonitor) Wait(c *exec.Cmd, ec chan Exit) (int, error) {
	if h, ok := m.handles.Load(c); ok {
		e := h.(*processHandle).result()
		return e.Status, e.Err
	}
	// Fallback for a custom monitor that does not register a handle. The
	// channel owns the single result in that case.
	e := <-ec
	return e.Status, e.Err
}

func commandPath(c *exec.Cmd) string {
	if c.Path != "" {
		return c.Path
	}
	if len(c.Args) > 0 {
		return c.Args[0]
	}
	return ""
}

func processPID(c *exec.Cmd) int {
	if c.Process != nil {
		return c.Process.Pid
	}
	return -1
}

// terminateBounded gracefully terminates a started process and, if it has not
// been reaped within grace, escalates to a forced kill. This mirrors the
// Cancel/WaitDelay policy for teardown paths that run without a context
// cancellation (e.g. a CloseAfterStart failure), so the process is never
// leaked and the subsequent Wait always returns. It waits on the reaper's
// own completion signal rather than polling the pid, which is unreliable once
// the process becomes a zombie or changes ownership. It does not reap: the
// processHandle remains the single cmd.Wait caller.
func (m *defaultMonitor) terminateBounded(cmd *exec.Cmd, setpgid bool, grace time.Duration) {
	if cmd.Process == nil {
		return
	}
	h, ok := m.handles.Load(cmd)
	if !ok {
		// A custom monitor owns this command; best-effort signal only.
		cancelProcess(cmd, setpgid)
		return
	}
	handle := h.(*processHandle)
	if err := cancelProcess(cmd, setpgid); err != nil {
		// Already gone or not signalable; Wait will settle it.
		return
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-handle.ready:
		// Reaped after the graceful signal.
	case <-timer.C:
		forceKillProcess(cmd, setpgid)
	}
}
