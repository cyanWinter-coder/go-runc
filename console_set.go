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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/containerd/console"
)

// ErrConsoleClosed is returned by ConsoleHandle.ReceiveMaster when the
// handle is closed while a receive is pending or in progress.
var ErrConsoleClosed = errors.New("console handle is closed")

// ConsoleSet is a transactionally allocated set of console sockets.
//
// The set is published only after every console socket has been created:
// if any socket fails to be created, every socket created so far is closed
// and removed, and no set is returned. Likewise, ReceiveMasters publishes
// the pty masters only after every console has delivered; on failure the
// masters received so far are closed and the whole set is reclaimed, so a
// caller never observes a partially delivered set of consoles.
type ConsoleSet struct {
	consoles []*ConsoleHandle
}

// NewConsoleSet creates a console socket at each of the provided paths as a
// single transaction: if any socket cannot be created, all sockets created
// so far are rolled back and no set is published.
func NewConsoleSet(paths ...string) (*ConsoleSet, error) {
	if len(paths) == 0 {
		return nil, errors.New("no console paths provided")
	}
	set := &ConsoleSet{}
	for i, p := range paths {
		s, err := newSocket(p)
		if err != nil {
			set.Close()
			return nil, fmt.Errorf("failed to create console socket %d: %w", i, err)
		}
		set.consoles = append(set.consoles, newConsoleHandle(s))
	}
	return set, nil
}

// NewTempConsoleSet creates count temp console sockets as a single
// transaction. Each socket lives in its own temp directory, which is
// removed when the owning console handle is closed.
func NewTempConsoleSet(count int) (*ConsoleSet, error) {
	if count <= 0 {
		return nil, fmt.Errorf("console count must be positive: %d", count)
	}
	set := &ConsoleSet{}
	for i := 0; i < count; i++ {
		s, err := newTempSocket("pty", "pty.sock")
		if err != nil {
			set.Close()
			return nil, fmt.Errorf("failed to create temp console socket %d: %w", i, err)
		}
		set.consoles = append(set.consoles, newConsoleHandle(s))
	}
	return set, nil
}

// Len returns the number of consoles in the set.
func (s *ConsoleSet) Len() int {
	return len(s.consoles)
}

// Get returns the console handle at index i.
func (s *ConsoleSet) Get(i int) *ConsoleHandle {
	return s.consoles[i]
}

// Paths returns the socket path of every console in the set.
func (s *ConsoleSet) Paths() []string {
	paths := make([]string, 0, len(s.consoles))
	for _, c := range s.consoles {
		paths = append(paths, c.Path())
	}
	return paths
}

// ReceiveMasters blocks until every console in the set has received its pty
// master, then publishes them as a whole. If any receive fails or ctx is
// canceled — for example because the process expected to deliver a console
// exited — the masters received so far are closed, the remaining receives
// are woken, and the entire set is reclaimed.
func (s *ConsoleSet) ReceiveMasters(ctx context.Context) ([]console.Console, error) {
	masters := make([]console.Console, 0, len(s.consoles))
	for i, c := range s.consoles {
		m, err := c.ReceiveMaster(ctx)
		if err != nil {
			// Roll back: closing the set closes every master received
			// so far and wakes any blocked receive.
			s.Close()
			return nil, fmt.Errorf("failed to receive console master %d: %w", i, err)
		}
		masters = append(masters, m)
	}
	return masters, nil
}

// Close closes every console in the set. It is safe to call multiple times;
// each console is closed exactly once.
func (s *ConsoleSet) Close() error {
	var err error
	for _, c := range s.consoles {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// ConsoleHandle is a single console within a ConsoleSet. Each handle owns an
// independent close state: closing a handle never closes its siblings, and
// closing a handle more than once is a no-op.
//
// A handle satisfies the ConsoleSocket interface through its Path method
// and can be handed to CreateOpts or ExecOpts directly.
type ConsoleHandle struct {
	socket *Socket
	done   chan struct{}
	once   sync.Once
	err    error

	mu     sync.Mutex
	master console.Console
	closed bool
}

var _ ConsoleSocket = (*ConsoleHandle)(nil)

func newConsoleHandle(s *Socket) *ConsoleHandle {
	return &ConsoleHandle{
		socket: s,
		done:   make(chan struct{}),
	}
}

// Path returns the path of the console socket on disk.
func (c *ConsoleHandle) Path() string {
	return c.socket.Path()
}

// Master returns the received pty master, or nil if it has not been
// received yet.
func (c *ConsoleHandle) Master() console.Console {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.master
}

// ReceiveMaster blocks until the socket receives the pty master, ctx is
// canceled, or the handle is closed. Canceling ctx — for example when the
// process expected to deliver the console exits — wakes the receive and
// reclaims the underlying socket.
func (c *ConsoleHandle) ReceiveMaster(ctx context.Context) (console.Console, error) {
	type result struct {
		master console.Console
		err    error
	}
	resCh := make(chan result, 1)
	go func() {
		f, err := c.socket.receive()
		if err != nil {
			resCh <- result{err: err}
			return
		}
		m, err := console.ConsoleFromFile(f)
		if err != nil {
			f.Close()
			resCh <- result{err: err}
			return
		}
		resCh <- result{master: m}
	}()
	select {
	case res := <-resCh:
		if res.err != nil {
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()
			if closed {
				return nil, ErrConsoleClosed
			}
			return nil, res.err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			res.master.Close()
			return nil, ErrConsoleClosed
		}
		c.master = res.master
		c.mu.Unlock()
		return res.master, nil
	case <-ctx.Done():
		// Wake the blocked receive and wait for it to unwind so the
		// file descriptor cannot leak past the caller.
		c.socket.Close()
		<-resCh
		return nil, ctx.Err()
	case <-c.done:
		<-resCh
		return nil, ErrConsoleClosed
	}
}

// Close closes the console's pty master, if received, and its socket,
// waking any blocked ReceiveMaster. It is idempotent and never affects
// other consoles in the set.
func (c *ConsoleHandle) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.mu.Lock()
		master := c.master
		c.closed = true
		c.mu.Unlock()
		if master != nil {
			c.err = master.Close()
		}
		if serr := c.socket.Close(); c.err == nil {
			c.err = serr
		}
	})
	return c.err
}
