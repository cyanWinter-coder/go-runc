//go:build linux && !windows

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
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// processStartTime returns the boot-based start time of pid (field 22 of
// /proc/<pid>/stat), in clock ticks. It is zero when the process no longer
// exists or the value cannot be read. The start time distinguishes two
// different processes that happen to reuse the same pid.
func processStartTime(pid int) uint64 {
	data, err := os.ReadFile(statPath(pid))
	if err != nil {
		return 0
	}
	// The comm field is parenthesized and may itself contain spaces and
	// parentheses, so split on the closing parenthesis and parse the fields
	// after it.
	rest := string(data)
	idx := strings.LastIndexByte(rest, ')')
	if idx < 0 || idx+2 >= len(rest) {
		return 0
	}
	fields := strings.Fields(rest[idx+2:])
	// Field 22 of /proc/<pid>/stat is index 21 from the start; after removing
	// pid and comm (the first two fields) it is index 19 of the remainder.
	const startTimeIndex = 19
	if len(fields) <= startTimeIndex {
		return 0
	}
	start, err := strconv.ParseUint(fields[startTimeIndex], 10, 64)
	if err != nil {
		return 0
	}
	return start
}

// ownerAlive reports whether the process described by the owner record is
// still the same running process that created the socket. A missing procfs
// entry, or a changed start time (pid reuse), means it is gone.
func ownerAlive(record socketOwner) bool {
	if killErr := unix.Kill(record.Pid, 0); killErr != nil {
		return false
	}
	if record.StartTime == 0 {
		// No start time was recorded; the successful signal above is the best
		// information available.
		return true
	}
	return processStartTime(record.Pid) == record.StartTime
}

func statPath(pid int) string {
	return "/proc/" + strconv.Itoa(pid) + "/stat"
}
