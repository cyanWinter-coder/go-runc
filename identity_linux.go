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
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// MatchesPid reports whether the process currently registered under pid is
// the same process generation as id. The pidfd is authoritative when
// present; otherwise the process start time has to match. A bare numeric
// pid match is only accepted when neither stable identifier exists.
func (id ProcessIdentity) MatchesPid(pid int) bool {
	if id.Pidfd != nil {
		target, err := pidfdPid(id.Pidfd)
		if err == nil {
			return target == pid
		}
		// A stale pidfd no longer resolves: its generation has exited even
		// if the numeric pid got recycled for a new process.
		return false
	}
	if id.StarttimeTicks != 0 {
		starttime, err := processStarttime(pid)
		if err != nil {
			return false
		}
		return starttime == id.StarttimeTicks
	}
	return id.Pid == pid
}

// openProcessPidfd opens a pidfd referring to pid. A nil pidfd without an
// error means the platform or sandbox rejects pidfd_open; the caller then
// falls back to start time based identity.
func openProcessPidfd(pid int) (*os.File, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
			return nil, nil
		}
		return nil, fmt.Errorf("runc: failed to open pidfd for pid %d: %w", pid, err)
	}
	return os.NewFile(uintptr(fd), fmt.Sprintf("pidfd:%d", pid)), nil
}

// dupFile duplicates f so the monitor owns its own file description and
// closing the monitor copy never affects the caller's file.
func dupFile(f *os.File) (*os.File, error) {
	fd, err := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), f.Name()), nil
}

// pidfdPid returns the pid a pidfd refers to, as reported by procfs.
func pidfdPid(f *os.File) (int, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", f.Fd()))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "Pid:") {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Pid:")))
		if err != nil {
			return 0, err
		}
		return pid, nil
	}
	return 0, fmt.Errorf("runc: fd %d is not a pidfd", f.Fd())
}

// processStarttime reads field 22 (starttime, in clock ticks since boot)
// from /proc/<pid>/stat. Unlike a numeric pid, the start time changes for
// every process generation, so it tells a recycled pid from the original.
func processStarttime(pid int) (uint64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	// The comm field can contain spaces and parentheses, so parse from the
	// closing parenthesis instead of splitting on spaces.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 || end+2 >= len(data) {
		return 0, fmt.Errorf("runc: malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(string(data[end+2:]))
	// The first field after ')' is stat field 3 (state), so overall field
	// 22 (starttime) is zero-based index 19 of fields.
	const starttimeIndex = 19
	if len(fields) <= starttimeIndex {
		return 0, fmt.Errorf("runc: /proc/%d/stat has no starttime field", pid)
	}
	starttime, err := strconv.ParseUint(fields[starttimeIndex], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("runc: invalid starttime in /proc/%d/stat: %w", pid, err)
	}
	return starttime, nil
}

// pidfdAlive reports whether the process referred to by the open pidfd is
// still running. A pidfd becomes pollable only when its own process exits,
// so a recycled pid cannot confuse the result. The caller must keep the
// file open: polling a closed (and possibly recycled) fd number is
// meaningless.
func pidfdAlive(f *os.File) (bool, error) {
	for {
		n, err := unix.Poll([]unix.PollFd{{
			Fd:     int32(f.Fd()),
			Events: unix.POLLIN,
		}}, 0)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return false, err
		}
		return n == 0, nil
	}
}
