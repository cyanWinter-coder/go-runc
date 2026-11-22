//go:build windows

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
)

// syscallSignal is a placeholder signal type on Windows, which has no POSIX
// signals. SignalError is therefore never produced there.
type syscallSignal int

// SignalError reports that a process was terminated by a signal. Windows has
// no POSIX signals, so this error is never returned; it exists to keep the
// error taxonomy identical across platforms.
type SignalError struct {
	Signal syscallSignal
}

func (e *SignalError) Error() string {
	return fmt.Sprintf("signal: %d", e.Signal)
}

func exitStatusString(status int) string {
	return fmt.Sprintf("exit status %d", status)
}

// reapProcess calls cmd.Wait exactly once and translates the outcome into a
// status code plus a distinguishable error, both derived from the same
// reclamation. The status is taken from cmd.ProcessState so it stays accurate
// when Wait fails for a reason other than the exit itself.
func reapProcess(cmd *exec.Cmd) (int, error) {
	waitErr := cmd.Wait()
	status := processStateCode(cmd)
	if waitErr == nil {
		return status, nil
	}

	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		// A pipe copy failure, [exec.ErrWaitDelay], a context error injected
		// by Cancel, etc.
		return status, waitErr
	}

	if status == -1 {
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

// cancelProcess forcibly terminates the process on context cancellation;
// Windows has no graceful POSIX signal equivalent for arbitrary processes.
func cancelProcess(cmd *exec.Cmd, _ bool) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	return cmd.Process.Kill()
}

// forceKillProcess is the same as cancelProcess on Windows: termination is
// already unconditional.
func forceKillProcess(cmd *exec.Cmd, setpgid bool) {
	_ = cancelProcess(cmd, setpgid)
}

// terminateProcess asks a process owned by a custom monitor to stop. It is
// best-effort: that monitor's own Wait remains the authoritative reclamation.
func terminateProcess(cmd *exec.Cmd, setpgid bool) {
	if cmd.Process == nil {
		return
	}
	_ = cancelProcess(cmd, setpgid)
}
