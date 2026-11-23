//go:build !linux

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
)

// MatchesPid reports whether the process currently registered under pid is
// the same process generation as id. On platforms without a pidfd or
// procfs, the generation registry is the only stable boundary and a bare
// numeric pid comparison remains the best available check.
func (id ProcessIdentity) MatchesPid(pid int) bool {
	return id.Pid == pid
}

// openProcessPidfd reports that pidfd based identity is unsupported.
func openProcessPidfd(pid int) (*os.File, error) {
	return nil, ErrIdentityUnsupported
}

func dupFile(f *os.File) (*os.File, error) {
	return nil, ErrIdentityUnsupported
}

func processStarttime(pid int) (uint64, error) {
	return 0, ErrIdentityUnsupported
}

func pidfdPid(f *os.File) (int, error) {
	return 0, ErrIdentityUnsupported
}

func pidfdAlive(f *os.File) (bool, error) {
	return false, ErrIdentityUnsupported
}
