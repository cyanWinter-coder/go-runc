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
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Errors returned by the receive operations of a Socket, so callers can tell
// apart the ways a file descriptor handoff can fail.
var (
	// ErrSocketClosed is returned by a receive on a socket whose Close
	// method was already called.
	ErrSocketClosed = errors.New("runc: socket is closed")
	// ErrPeerClosed is returned when the peer connected but closed the
	// connection without sending a file descriptor.
	ErrPeerClosed = errors.New("runc: peer closed the connection without sending a file descriptor")
	// ErrTruncated is returned when the message carrying the file
	// descriptor, or its ancillary data, did not fit the receive buffers.
	ErrTruncated = errors.New("runc: file descriptor message was truncated")
	// ErrReceiveTimeout is returned when no file descriptor arrived before
	// the deadline set with SetReceiveDeadline.
	ErrReceiveTimeout = errors.New("runc: timed out waiting to receive a file descriptor")
)

// Socket is a unix socket that accepts a file descriptor sent by runc, such as
// the pty master for the container's console or the pidfd of the process runc
// creates.
//
// A received file descriptor is owned by exactly one party: once a receive
// succeeds, ownership moves to the returned *os.File and the Socket no longer
// refers to it. If a receive fails, any descriptor that arrived before the
// failure is closed by the Socket and must not be used by the caller.
type Socket struct {
	rmdir bool
	l     *net.UnixListener

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error

	mu       sync.Mutex
	deadline time.Time
}

// newSocket creates a new unix socket listening at the provided path.
func newSocket(path string) (*Socket, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveUnixAddr("unix", abs)
	if err != nil {
		return nil, err
	}
	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, err
	}
	return &Socket{
		l: l,
	}, nil
}

// newTempSocket creates a unix socket named name inside a new temp directory
// created with the provided prefix. The directory is removed on Close().
func newTempSocket(prefix, name string) (*Socket, error) {
	runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
	dir, err := os.MkdirTemp(runtimeDir, prefix)
	if err != nil {
		return nil, err
	}
	s, err := newSocket(filepath.Join(dir, name))
	if err != nil {
		_ = os.RemoveAll(dir) // #nosec G703 -- dir was created by os.MkdirTemp above.
		return nil, err
	}
	s.rmdir = true
	if runtimeDir != "" {
		if err := os.Chmod(s.Path(), 0o755|os.ModeSticky); err != nil { // #nosec G703 -- the path identifies the socket created in the temporary directory.
			_ = s.Close()
			return nil, err
		}
	}
	return s, nil
}

// Path returns the path to the unix socket on disk
func (c *Socket) Path() string {
	return c.l.Addr().String()
}

// SetReceiveDeadline sets the deadline for future and pending receive
// operations on the socket, after which they fail with ErrReceiveTimeout.
// A zero value disables the timeout.
func (c *Socket) SetReceiveDeadline(t time.Time) error {
	if c.closed.Load() {
		return ErrSocketClosed
	}
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return c.l.SetDeadline(t)
}

// Close closes the unix socket and removes it from disk if it was created as
// a temp socket. Pending and future receives fail with ErrSocketClosed.
// Close is idempotent: calling it more than once returns the result of the
// first call.
func (c *Socket) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = c.l.Close()
		if c.rmdir {
			if rErr := os.RemoveAll(filepath.Dir(c.Path())); c.closeErr == nil { // #nosec G703 -- rmdir is set only for sockets created in a private temporary directory.
				c.closeErr = rErr
			}
		}
	})
	return c.closeErr
}

// receive blocks until the socket receives a file descriptor. On success the
// descriptor is handed off to the caller exactly once; on failure any
// descriptor received before the failure is released.
func (c *Socket) receive() (*os.File, error) {
	if c.closed.Load() {
		return nil, ErrSocketClosed
	}
	conn, err := c.l.Accept()
	if err != nil {
		if c.closed.Load() {
			return nil, ErrSocketClosed
		}
		return nil, receiveError(err)
	}
	defer conn.Close()
	c.mu.Lock()
	deadline := c.deadline
	c.mu.Unlock()
	if !deadline.IsZero() {
		_ = conn.SetReadDeadline(deadline)
	}
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, fmt.Errorf("received connection which was not a unix socket")
	}
	return recvFd(uc)
}

// receiveError maps connection-level failures onto the distinguishable
// receive errors, leaving anything else untouched.
func receiveError(err error) error {
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return fmt.Errorf("%w: %v", ErrReceiveTimeout, err)
	}
	if errors.Is(err, io.EOF) || errors.Is(err, unix.ECONNRESET) {
		return ErrPeerClosed
	}
	return err
}

// recvFd waits for a file descriptor to be sent over the given AF_UNIX
// socket. The file name of the remote file descriptor will be recreated
// locally (it is sent as non-auxiliary data in the same payload).
//
// On success the returned file owns the received descriptor. On failure any
// descriptor that arrived before the failure is closed before returning.
func recvFd(socket *net.UnixConn) (*os.File, error) {
	const MaxNameLen = 4096
	oobSpace := unix.CmsgSpace(4)

	name := make([]byte, MaxNameLen)
	oob := make([]byte, oobSpace)

	n, oobn, flags, _, err := socket.ReadMsgUnix(name, oob)
	if err != nil {
		return nil, receiveError(err)
	}
	if n == 0 && oobn == 0 {
		return nil, ErrPeerClosed
	}

	// Parse whatever descriptors arrived first, so the failure paths below
	// can release them instead of leaking them.
	fds, err := parseFds(oob[:oobn])
	if err != nil {
		return nil, err
	}

	if flags&unix.MSG_CTRUNC != 0 || n >= MaxNameLen {
		closeFds(fds)
		return nil, fmt.Errorf("%w (n=%d oobn=%d)", ErrTruncated, n, oobn)
	}

	if len(fds) != 1 {
		closeFds(fds)
		return nil, fmt.Errorf("recvfd: number of fds is not 1: %d", len(fds))
	}
	fd := uintptr(fds[0])

	return os.NewFile(fd, string(name[:n])), nil
}

// parseFds extracts the file descriptors from the ancillary data of a
// received message.
func parseFds(oob []byte) ([]int, error) {
	if len(oob) == 0 {
		return nil, nil
	}
	scms, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, err
	}
	if len(scms) != 1 {
		return nil, fmt.Errorf("recvfd: number of SCMs is not 1: %d", len(scms))
	}
	fds, err := unix.ParseUnixRights(&scms[0])
	if err != nil {
		return nil, err
	}
	return fds, nil
}

// closeFds releases descriptors that were received but cannot be handed off.
func closeFds(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}
