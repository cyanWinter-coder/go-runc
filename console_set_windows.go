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
	"context"
	"errors"

	"github.com/containerd/console"
)

// ErrConsoleClosed is returned by ConsoleHandle.ReceiveMaster when the
// handle is closed while a receive is pending or in progress.
var ErrConsoleClosed = errors.New("console handle is closed")

// errConsolesNotImplemented is returned by every console set operation on
// Windows, where runc does not deliver pty masters over unix sockets.
var errConsolesNotImplemented = errors.New("console sets are not implemented on Windows")

// ConsoleSet is a transactionally allocated set of console sockets. It is
// not implemented on Windows.
type ConsoleSet struct{}

// ConsoleHandle is a single console within a ConsoleSet. It is not
// implemented on Windows.
type ConsoleHandle struct{}

// NewConsoleSet is not implemented on Windows.
func NewConsoleSet(paths ...string) (*ConsoleSet, error) {
	return nil, errConsolesNotImplemented
}

// NewTempConsoleSet is not implemented on Windows.
func NewTempConsoleSet(count int) (*ConsoleSet, error) {
	return nil, errConsolesNotImplemented
}

// Len returns the number of consoles in the set.
func (s *ConsoleSet) Len() int {
	return 0
}

// Get returns the console handle at index i.
func (s *ConsoleSet) Get(i int) *ConsoleHandle {
	return nil
}

// Paths returns the socket path of every console in the set.
func (s *ConsoleSet) Paths() []string {
	return nil
}

// ReceiveMasters is not implemented on Windows.
func (s *ConsoleSet) ReceiveMasters(ctx context.Context) ([]console.Console, error) {
	return nil, errConsolesNotImplemented
}

// Close closes every console in the set.
func (s *ConsoleSet) Close() error {
	return nil
}

// Path returns the path of the console socket on disk.
func (c *ConsoleHandle) Path() string {
	return ""
}

// Master returns the received pty master, or nil if it has not been
// received yet.
func (c *ConsoleHandle) Master() console.Console {
	return nil
}

// ReceiveMaster is not implemented on Windows.
func (c *ConsoleHandle) ReceiveMaster(ctx context.Context) (console.Console, error) {
	return nil, errConsolesNotImplemented
}

// Close closes the console's pty master, if received, and its socket.
func (c *ConsoleHandle) Close() error {
	return nil
}
