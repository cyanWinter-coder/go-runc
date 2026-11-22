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
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// defaultKillGracePeriod is how long a canceled process gets to exit and
// drain its output pipes before it is SIGKILLed.
const defaultKillGracePeriod = 5 * time.Second

func (r *Runc) command(context context.Context, args ...string) *exec.Cmd {
	return r.commandWithCustomLogFile(context, "", args...)
}

func (r *Runc) commandWithCustomLogFile(context context.Context, logFile string, args ...string) *exec.Cmd {
	command := r.Command
	if command == "" {
		command = DefaultCommand
	}
	cmd := exec.CommandContext(context, command, append(r.args(logFile), args...)...) // #nosec G702 -- executing the caller-configured runtime is the purpose of this package.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: r.Setpgid,
	}
	cmd.Env = filterEnv(os.Environ(), "NOTIFY_SOCKET") // NOTIFY_SOCKET introduces a special behavior in runc but should only be set if invoked from systemd
	cmd.Env = append(cmd.Env, extraEnv(context)...)
	cmd.Dir = r.WorkDir
	if r.PdeathSignal != 0 {
		cmd.SysProcAttr.Pdeathsig = r.PdeathSignal
	}
	r.configureLifecycle(cmd)

	return cmd
}

// killGracePeriod returns the configured bounded-termination delay or the
// default when unset.
func (r *Runc) killGracePeriod() time.Duration {
	if r.KillGracePeriod > 0 {
		return r.KillGracePeriod
	}
	return defaultKillGracePeriod
}

// configureLifecycle installs the bounded cancellation policy on a command.
// On context cancellation the process is terminated gracefully first; if it
// neither exits nor drains its output pipes within the grace period, the
// runtime sends SIGKILL and forcibly closes the pipes. Closing happens only
// after signaling, so output already produced before termination is retained.
func (r *Runc) configureLifecycle(cmd *exec.Cmd) {
	delay := r.killGracePeriod()
	setpgid := r.Setpgid
	cmd.Cancel = func() error {
		return cancelProcess(cmd, setpgid)
	}
	cmd.WaitDelay = delay
}

func filterEnv(in []string, names ...string) []string {
	out := make([]string, 0, len(in))
loop0:
	for _, v := range in {
		for _, k := range names {
			if strings.HasPrefix(v, k+"=") {
				continue loop0
			}
		}
		out = append(out, v)
	}
	return out
}
