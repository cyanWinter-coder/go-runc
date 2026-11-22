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
	"github.com/containerd/console"
)

// NewConsoleSocket creates a new unix socket at the provided path to accept a
// pty master created by runc for use by the container.
//
// If a socket already exists at path it is not removed blindly: a stale socket
// left by a crashed process of the same user is taken over, while a live
// listener, a foreign user's path or a path without a valid owner record makes
// the call fail and leaves the path untouched. The socket path and a lock file
// at path+".lock" are both removed by Close.
func NewConsoleSocket(path string) (*Socket, error) {
	return newSocket(path)
}

// NewTempConsoleSocket returns a temp console socket for use with a container.
// On Close(), the socket, its lock file and the private temporary directory
// are deleted.
func NewTempConsoleSocket() (*Socket, error) {
	return newTempSocket("pty", "pty.sock")
}

// ReceiveMaster blocks until the socket receives the pty master
func (c *Socket) ReceiveMaster() (console.Console, error) {
	f, err := c.receive()
	if err != nil {
		return nil, err
	}
	return console.ConsoleFromFile(f)
}
