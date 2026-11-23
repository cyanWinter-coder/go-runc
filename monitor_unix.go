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
	"os"
	"os/exec"
	"syscall"
	"time"
)

// configureCommandTermination wires bounded termination into an
// exec.CommandContext: on context cancellation a graceful SIGTERM is sent;
// if the process does not exit within the grace period the os/exec package
// escalates to SIGKILL and closes any pipes still held open, so a canceled
// command can never block its reaper indefinitely.
//
// A command created without a context (exec.Command) cannot take a Cancel
// function, so only WaitDelay is configured there. The handle's own Stop
// still performs the graceful-to-forceful escalation.
func configureCommandTermination(cmd *exec.Cmd, hasContext bool, gracePeriod time.Duration) {
	cmd.WaitDelay = gracePeriod
	if !hasContext {
		return
	}
	cmd.Cancel = func() error {
		return terminateProcess(cmd)
	}
}

// terminateProcess initiates graceful termination of the command. When the
// command was started in its own process group (Setpgid), signal the whole
// group so child processes such as runc's container init are terminated as
// well; otherwise signal the process directly.
func terminateProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	if cmd.SysProcAttr != nil && cmd.SysProcAttr.Setpgid {
		// Negative pid addresses the process group led by the child.
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		if err == nil {
			return nil
		}
		// Fall back to signaling the process directly if the group is gone.
		if err != syscall.ESRCH {
			return err
		}
	}
	return cmd.Process.Signal(syscall.SIGTERM)
}

// forceKillProcess escalates termination to SIGKILL, targeting the whole
// process group when the command owns one.
func forceKillProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	if cmd.SysProcAttr != nil && cmd.SysProcAttr.Setpgid {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err == nil {
			return nil
		}
		if err != syscall.ESRCH {
			return err
		}
	}
	return cmd.Process.Kill()
}
