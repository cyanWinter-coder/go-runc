//go:build !linux && !windows

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

import "golang.org/x/sys/unix"

// processStartTime cannot be determined portably outside Linux; zero means
// "unavailable".
func processStartTime(int) uint64 {
	return 0
}

// ownerAlive reports whether the recorded pid still exists. Platforms without
// procfs cannot tell a reused pid from the original process, so the exclusive
// flock and the dial-based liveness check remain the authoritative guards.
func ownerAlive(record socketOwner) bool {
	return unix.Kill(record.Pid, 0) == nil
}
