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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// syscallSignal is the signal type used to terminate a child.
type syscallSignal = syscall.Signal

// SignalError reports that a process was terminated by a signal. Status is -1
// to match the convention used by [os.ProcessState.ExitCode]; inspect Signal
// for the concrete signal.
type SignalError struct {
	Signal syscall.Signal
}

func (e *SignalError) Error() string {
	return "signal: " + e.Signal.String()
}

func exitStatusString(status int) string {
	return fmt.Sprintf("exit status %d", status)
}

// reapProcess calls cmd.Wait exactly once and translates the outcome into a
// status code plus a distinguishable error, both derived from the same
// reclamation:
//   - clean exit:                  (code, nil)
//   - non-zero exit:               (code, *ExitError)
//   - terminated by a signal:      (-1,   *SignalError)
//   - I/O / WaitDelay / ctx error: (code from ProcessState, wrapped error)
//
// The status is taken from cmd.ProcessState rather than the error so it stays
// accurate even when Wait fails for a reason other than the exit itself (e.g.
// a context cancellation or ErrWaitDelay observed alongside a real exit).
func reapProcess(cmd *exec.Cmd) (int, error) {
	waitErr := cmd.Wait()
	status := processStateCode(cmd)
	if waitErr == nil {
		return status, nil
	}

	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		// The process exited but another problem was observed (a pipe copy
		// failure, [exec.ErrWaitDelay], a context error injected by Cancel,
		// ...). Keep the real status and surface the underlying error.
		return status, waitErr
	}

	if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return -1, &SignalError{Signal: ws.Signal()}
	}
	if status == -1 {
		// No conventional exit code (e.g. a platform without a WaitStatus
		// mapping); keep the status distinguishable from a real code.
		return -1, waitErr
	}
	return status, &ExitError{Status: status}
}

// processStateCode reads the exit code captured by the single cmd.Wait call.
// It returns -1 when the process did not exit with a conventional status.
func processStateCode(cmd *exec.Cmd) int {
	if cmd.ProcessState == nil {
		return -1
	}
	return cmd.ProcessState.ExitCode()
}

// cancelProcess gracefully terminates a process on context cancellation. When
// Setpgid was requested the whole process group is signaled, otherwise only
// the direct child. exec.Cmd falls back to Process.Kill after WaitDelay.
func cancelProcess(cmd *exec.Cmd, setpgid bool) error {
	return sendSignal(cmd, setpgid, syscall.SIGTERM)
}

// forceKillProcess escalates termination to SIGKILL, targeting the process
// group when one was created.
func forceKillProcess(cmd *exec.Cmd, setpgid bool) {
	_ = sendSignal(cmd, setpgid, syscall.SIGKILL)
}

// terminateProcess asks a process owned by a custom monitor to stop. It is
// best-effort: that monitor's own Wait remains the authoritative reclamation.
func terminateProcess(cmd *exec.Cmd, setpgid bool) {
	if cmd.Process == nil {
		return
	}
	_ = cancelProcess(cmd, setpgid)
}

// sendSignal delivers sig to the process group (negative pid when Setpgid was
// used) or the direct child. An ESRCH on the group falls back to the child.
func sendSignal(cmd *exec.Cmd, setpgid bool, sig syscall.Signal) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	if setpgid {
		err := syscall.Kill(-cmd.Process.Pid, sig)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.ESRCH):
			// The group is already gone; the child may have exited on its own.
			// Fall through to signaling the child directly.
		default:
			return err
		}
	}
	return cmd.Process.Signal(sig)
}
