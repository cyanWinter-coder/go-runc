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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSocketClose(t *testing.T) {
	t.Run("close is idempotent and removes the temp socket", func(t *testing.T) {
		s, err := NewTempConsoleSocket()
		if err != nil {
			t.Fatal(err)
		}
		path := s.Path()
		if err := s.Close(); err != nil {
			t.Fatalf("first close: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("second close: %v", err)
		}
		if _, err := os.Stat(path); err == nil {
			t.Fatal("path still exists after close")
		}
	})

	t.Run("receive after close is rejected", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReceivePidfd(); !errors.Is(err, ErrSocketClosed) {
			t.Fatalf("expected ErrSocketClosed, got %v", err)
		}
		if err := s.SetReceiveDeadline(time.Now()); !errors.Is(err, ErrSocketClosed) {
			t.Fatalf("expected ErrSocketClosed, got %v", err)
		}
	})

	t.Run("close unblocks a pending receive", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		errCh := make(chan error, 1)
		go func() {
			_, err := s.ReceivePidfd()
			errCh <- err
		}()
		// Give the receive a chance to block in Accept before closing.
		time.Sleep(50 * time.Millisecond)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-errCh; !errors.Is(err, ErrSocketClosed) {
			t.Fatalf("expected ErrSocketClosed, got %v", err)
		}
	})
}

func TestSocketReceiveErrors(t *testing.T) {
	t.Run("peer disconnect is distinguishable", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		go func() {
			conn, err := net.Dial("unix", s.Path())
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
		}()

		if _, err := s.ReceivePidfd(); !errors.Is(err, ErrPeerClosed) {
			t.Fatalf("expected ErrPeerClosed, got %v", err)
		}
	})

	t.Run("accept timeout is distinguishable", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := s.SetReceiveDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}

		if _, err := s.ReceivePidfd(); !errors.Is(err, ErrReceiveTimeout) {
			t.Fatalf("expected ErrReceiveTimeout, got %v", err)
		}
	})

	t.Run("read timeout is distinguishable", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := s.SetReceiveDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}

		go func() {
			conn, err := net.Dial("unix", s.Path())
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			// Say nothing until after the receive deadline has passed.
			time.Sleep(200 * time.Millisecond)
		}()

		if _, err := s.ReceivePidfd(); !errors.Is(err, ErrReceiveTimeout) {
			t.Fatalf("expected ErrReceiveTimeout, got %v", err)
		}
	})

	t.Run("truncated ancillary data is distinguishable and releases fds", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		// The receive buffer holds at most two fds (CmsgSpace(4) is padded
		// to 24 bytes on 64-bit), so sending three truncates the ancillary
		// data while still delivering the descriptors that fit.
		fds := openTempFiles(t, 3)
		before := openFdCount(t)
		go sendFd(t, s.Path(), "standard", fds...)

		if _, err := s.ReceivePidfd(); !errors.Is(err, ErrTruncated) {
			t.Fatalf("expected ErrTruncated, got %v", err)
		}
		if got := openFdCount(t); got != before {
			t.Fatalf("received fd was not released on truncation: %d fds open, want %d", got, before)
		}
	})

	t.Run("message without an fd fails and leaks nothing", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		before := openFdCount(t)
		go sendFd(t, s.Path(), "standard")

		if _, err := s.ReceivePidfd(); err == nil || errors.Is(err, ErrPeerClosed) {
			t.Fatalf("expected a descriptor-count error, got %v", err)
		}
		if got := openFdCount(t); got != before {
			t.Fatalf("fd leaked on failed receive: %d fds open, want %d", got, before)
		}
	})
}

func TestSocketReceiveOwnership(t *testing.T) {
	t.Run("received file survives socket close", func(t *testing.T) {
		s, err := NewPidfdSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}

		fds := openTempFiles(t, 1)
		go sendFd(t, s.Path(), "standard", fds...)

		f, err := s.ReceivePidfd()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		// Closing the socket must not close the descriptor it handed off.
		if _, err := f.Stat(); err != nil {
			t.Fatalf("received fd was closed with the socket: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed console handoff releases the descriptor", func(t *testing.T) {
		s, err := NewConsoleSocket(testSocketPath(t))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		// A regular file is not a tty, so ConsoleFromFile rejects it.
		fds := openTempFiles(t, 1)
		before := openFdCount(t)
		go sendFd(t, s.Path(), "console", fds...)

		if _, err := s.ReceiveMaster(); err == nil {
			t.Fatal("expected ReceiveMaster to fail for a non-console fd")
		}
		if got := openFdCount(t); got != before {
			t.Fatalf("fd leaked on failed console handoff: %d fds open, want %d", got, before)
		}
	})
}

// openTempFiles opens n temporary files and returns their fds, closed when
// the test finishes.
func openTempFiles(t *testing.T, n int) []int {
	t.Helper()
	fds := make([]int, 0, n)
	for i := 0; i < n; i++ {
		f, err := os.CreateTemp(t.TempDir(), "fd")
		if err != nil {
			t.Fatal(err)
		}
		fd := int(f.Fd())
		t.Cleanup(func() { f.Close() })
		fds = append(fds, fd)
	}
	return fds
}

// openFdCount returns the number of file descriptors the process has open.
func openFdCount(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

// testSocketPath returns a path to bind a unix socket to. t.TempDir() is not
// used because it derives the directory name from the test's name, which is
// long enough here to exceed the 108 byte sun_path limit on some versions of
// Go.
func testSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gorunc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	path := filepath.Join(dir, "sock")
	if len(path) >= 108 {
		t.Fatalf("socket path %q does not fit sun_path", path)
	}
	return path
}

// sendFd stands in for runc: it connects to the socket and sends fds along with
// name as the payload, the way runc's init process does. "standard" is the name
// runc uses for the pidfd of an init process, "setns" for an exec'd one.
func sendFd(t *testing.T, path, name string, fds ...int) {
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	uc, ok := conn.(*net.UnixConn)
	if !ok {
		t.Error("expected a unix connection")
		return
	}
	var rights []byte
	if len(fds) > 0 {
		rights = unix.UnixRights(fds...)
	}
	if _, _, err := uc.WriteMsgUnix([]byte(name), rights, nil); err != nil {
		t.Error(err)
	}
}
