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
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// Monitor is the default ProcessMonitor for handling runc process exit
var Monitor ProcessMonitor = &defaultMonitor{}

// DefaultTerminationGracePeriod bounds how long a canceled or stopped
// command is given to exit after the graceful termination signal before it
// is forcefully killed. It also bounds the time waited for I/O pipes to be
// drained after exit.
const DefaultTerminationGracePeriod = 10 * time.Second

// Exit holds the exit information from a process.
//
// An Exit value is the single terminal result of a started command: it is
// published exactly once, and every Wait on the same command observes the
// same value and error.
type Exit struct {
	Timestamp time.Time
	Pid       int
	// Status is the process exit code. For a process terminated by a signal
	// it follows the shell convention of 128 + signal number. It is -1 when
	// no exit status is available (for example the wait itself failed).
	Status int
	// Err is the error observed while reaping the process, if any. It wraps
	// the associated Context's error when the process was terminated due to
	// cancellation or deadline expiry.
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
//
// Implementations may also implement ProcessLifecycle to participate in the
// single terminal-state lifecycle (bounded termination, idempotent Wait and
// resource cleanup). The built-in defaultMonitor implements it; third-party
// monitors keep working through the legacy channel-based contract.
type ProcessMonitor interface {
	Start(*exec.Cmd) (chan Exit, error)
	StartLocked(*exec.Cmd) (chan Exit, error)
	Wait(*exec.Cmd, chan Exit) (int, error)
}

// ProcessLifecycle is implemented by process monitors which own the full
// lifecycle of the commands they start. A processHandle is the single
// terminal state of one command:
//
//   - Start succeeds once and returns the same handle on no further attempt;
//   - the exit result is published exactly once, from the single reaper;
//   - Wait is idempotent: concurrent and repeated calls block until the
//     terminal state and return the identical status and error;
//   - Stop is idempotent: concurrent and repeated calls terminate the
//     process at most once and converge on the same terminal result;
//   - Delete releases the command's state (state directory, pipes) exactly
//     once, after the process has reached its terminal state.
type ProcessLifecycle interface {
	// StartProcess starts cmd and returns its terminal-state handle. The
	// opts carry per-command resources that must be released once the
	// process reaches its terminal state.
	StartProcess(cmd *exec.Cmd, opts startOptions) (ProcessHandle, error)
}

// ProcessHandle is the terminal-state handle of a single started command.
type ProcessHandle interface {
	// Wait blocks until the command reaches its terminal state and returns
	// the exit status and wait error from the single reaping call. Repeated
	// and concurrent calls are idempotent and return the same result.
	Wait() (int, error)
	// Stop requests a bounded, graceful termination of the command, waiting
	// for it to reach its terminal state. If the command does not exit
	// within the grace period it is forcefully killed. Repeated and
	// concurrent calls are idempotent and never override a natural exit
	// result already observed by the reaper.
	Stop() error
	// Delete releases the command's state directory and any registered
	// resources. It blocks until the process has reached its terminal state
	// first, so cleanup never races a still-running process. Repeated calls
	// are no-ops.
	Delete() error
}

// startOptions carries the per-command state tied to one process lifecycle.
type startOptions struct {
	// locked locks the goroutine starting the process to an OS thread.
	locked bool
	// ctx is the command's context, when it was created with
	// CommandContext. It lets the terminal result preserve the
	// cancellation/deadline cause even when the wait error only describes
	// the resulting signal.
	ctx context.Context
	// stateDir, when non-empty, is a command-private directory removed on
	// Delete after the process has terminated.
	stateDir string
	// cleanup runs once, after the process reaches its terminal state,
	// before stateDir is removed.
	cleanup func()
	// gracePeriod bounds termination after Stop or context cancellation.
	gracePeriod time.Duration
}

type defaultMonitor struct{}

// processHandle is the single terminal state of one started command.
//
// All mutation happens before the done channel is closed. Waiters
// synchronize on that channel, so once it is closed every field is final
// and safe to read without holding the lock.
type processHandle struct {
	cmd         *exec.Cmd
	ctx         context.Context
	stateDir    string
	cleanup     func()
	gracePeriod time.Duration

	stopOnce  sync.Once
	closeOnce sync.Once

	done chan struct{}

	exitMu sync.Mutex
	exit   Exit
}

func (m *defaultMonitor) StartProcess(cmd *exec.Cmd, opts startOptions) (ProcessHandle, error) {
	if opts.gracePeriod <= 0 {
		opts.gracePeriod = DefaultTerminationGracePeriod
	}
	h := &processHandle{
		cmd:         cmd,
		ctx:         opts.ctx,
		stateDir:    opts.stateDir,
		cleanup:     opts.cleanup,
		gracePeriod: opts.gracePeriod,
		done:        make(chan struct{}),
	}

	// Bounded termination: on context cancellation send the graceful signal
	// first; if the process ignores it, WaitDelay forcefully kills it and
	// closes any pipes left open, so cancellation can never hang forever.
	configureCommandTermination(cmd, opts.ctx != nil, opts.gracePeriod)

	startErr := make(chan error, 1)
	go func() {
		if opts.locked {
			// LockOSThread is required when Pdeathsig is set: an OS thread
			// exiting under the child would spuriously deliver the
			// parent-death signal. See https://go.dev/issue/27505.
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
		}
		if err := cmd.Start(); err != nil {
			startErr <- err
			return
		}
		close(startErr)
		h.reap()
	}()

	if err := <-startErr; err != nil {
		// Start failed: there is no process to reap and no terminal result
		// will ever be published, so release the command state immediately
		// and mark the handle terminal so later Wait/Delete calls do not
		// block forever.
		h.close()
		h.finish(Exit{Timestamp: time.Now(), Pid: pidOf(cmd), Status: -1, Err: err})
		return nil, err
	}
	return h, nil
}

// reap is the single reaper of the command. cmd.Wait is called exactly
// once here; every other caller observes its result through the done
// channel, which keeps repeated Wait calls safe (a second cmd.Wait would
// error) and guarantees the status and wait error originate together.
func (h *processHandle) reap() {
	waitErr := h.cmd.Wait()
	h.finish(h.exitOf(waitErr))
}

// finish publishes the single terminal result. The first call wins, so a
// natural exit observed by the reaper can never be overwritten by Stop or
// context cancellation racing it.
func (h *processHandle) finish(e Exit) {
	h.exitMu.Lock()
	select {
	case <-h.done:
		// Already terminal: keep the first published result.
		h.exitMu.Unlock()
		return
	default:
		h.exit = e
		close(h.done)
	}
	h.exitMu.Unlock()
}

// Wait implements ProcessHandle.
func (h *processHandle) Wait() (int, error) {
	<-h.done
	h.exitMu.Lock()
	e := h.exit
	h.exitMu.Unlock()
	return e.Status, e.Err
}

// Stop implements ProcessHandle.
func (h *processHandle) Stop() error {
	h.stopOnce.Do(func() {
		// Initiate graceful termination. If the process has already exited
		// (natural exit racing cancellation) the signal returns
		// os.ErrProcessDone and nothing is interrupted.
		if err := terminateProcess(h.cmd); err != nil && !errors.Is(err, os.ErrProcessDone) {
			// The process could not be signaled; the reaper still owns the
			// terminal result, so fall through and wait for it.
			_ = err
		}
		// Self-contained escalation. os/exec's WaitDelay only starts when
		// the command's context is done or Wait observes an exit, so a Stop
		// on a command whose context is never canceled must bound the wait
		// itself: forcefully kill a process that survives the graceful
		// signal past the grace period. If the process exits sooner, the
		// done channel fires and the watchdog disarms without acting.
		go func() {
			timer := time.NewTimer(h.gracePeriod)
			defer timer.Stop()
			select {
			case <-h.done:
			case <-timer.C:
				_ = forceKillProcess(h.cmd)
			}
		}()
	})
	_, err := h.Wait()
	return err
}

// Delete implements ProcessHandle.
func (h *processHandle) Delete() error {
	// Never clean up underneath a live process.
	<-h.done
	var err error
	h.closeOnce.Do(func() {
		if h.cleanup != nil {
			h.cleanup()
		}
		if h.stateDir != "" {
			err = os.RemoveAll(h.stateDir)
		}
	})
	return err
}

// close runs the resource cleanup without removing the state directory a
// second time; Delete remains the single place stateDir is removed.
func (h *processHandle) close() {
	h.closeOnce.Do(func() {
		if h.cleanup != nil {
			h.cleanup()
		}
		if h.stateDir != "" {
			_ = os.RemoveAll(h.stateDir)
		}
	})
}

// exitOf builds the terminal Exit from the single cmd.Wait result, pairing
// the status and the wait error so they can never diverge.
func (h *processHandle) exitOf(waitErr error) Exit {
	e := Exit{
		Timestamp: time.Now(),
		Pid:       pidOf(h.cmd),
		Status:    0,
		Err:       waitErr,
	}
	if waitErr == nil {
		return e
	}

	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			switch {
			case ws.Exited():
				e.Status = ws.ExitStatus()
			case ws.Signaled():
				// Shell convention: a command killed by signal N reports
				// 128 + N, instead of an empty/negative status.
				e.Status = 128 + int(ws.Signal())
			default:
				e.Status = -1
			}
		} else {
			e.Status = -1
		}
		// Go does not wrap the context error into an ExitError caused by a
		// signal, so attach it here to preserve the cancellation cause.
		if h.ctx != nil {
			if ctxErr := h.ctx.Err(); ctxErr != nil && !errors.Is(waitErr, ctxErr) {
				e.Err = fmt.Errorf("%w: %w", waitErr, ctxErr)
			}
		}
		return e
	}

	// A wait failure (e.g. I/O problem) carries no meaningful exit code.
	e.Status = -1
	return e
}

func pidOf(cmd *exec.Cmd) int {
	if cmd.Process != nil {
		return cmd.Process.Pid
	}
	return -1
}

// --- legacy ProcessMonitor implementation (channel-based contract) ---

func (m *defaultMonitor) Start(c *exec.Cmd) (chan Exit, error) {
	h, err := m.StartProcess(c, startOptions{})
	if err != nil {
		return nil, err
	}
	return legacyChan(h), nil
}

func (m *defaultMonitor) StartLocked(c *exec.Cmd) (chan Exit, error) {
	h, err := m.StartProcess(c, startOptions{locked: true})
	if err != nil {
		return nil, err
	}
	return legacyChan(h), nil
}

// legacyChan adapts a ProcessHandle to the historical buffered Exit channel
// consumed by ProcessMonitor.Wait. It publishes the single terminal result
// exactly once.
func legacyChan(h ProcessHandle) chan Exit {
	ec := make(chan Exit, 1)
	go func() {
		status, err := h.Wait()
		ec <- Exit{Timestamp: time.Now(), Status: status, Err: err}
		close(ec)
	}()
	return ec
}

func (m *defaultMonitor) Wait(_ *exec.Cmd, ec chan Exit) (int, error) {
	e := <-ec
	return e.Status, e.Err
}
