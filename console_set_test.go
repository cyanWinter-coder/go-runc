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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/console"
	"golang.org/x/sys/unix"
)

// sendConsoleMaster acts as runc: it creates a pty and sends the master fd
// to the console socket at path. The returned master is the sender's own
// copy and remains the caller's responsibility.
func sendConsoleMaster(t *testing.T, path string) console.Console {
	t.Helper()
	master, slaveName, err := console.NewPty()
	if err != nil {
		t.Fatal(err)
	}
	addr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		master.Close()
		t.Fatal(err)
	}
	uc, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		master.Close()
		t.Fatal(err)
	}
	defer uc.Close()
	if _, _, err := uc.WriteMsgUnix([]byte(slaveName), unix.UnixRights(int(master.Fd())), nil); err != nil {
		master.Close()
		t.Fatal(err)
	}
	return master
}

func fdIsClosed(fd uintptr) bool {
	_, err := unix.FcntlInt(fd, unix.F_GETFD, 0)
	return err == unix.EBADF
}

func waitForMaster(t *testing.T, h *ConsoleHandle) console.Console {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m := h.Master(); m != nil {
			return m
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for console master")
	return nil
}

func TestConsoleSetCreateRollback(t *testing.T) {
	dir := t.TempDir()
	good1 := filepath.Join(dir, "a.sock")
	good2 := filepath.Join(dir, "b.sock")
	bad := filepath.Join(dir, "missing", "c.sock")

	set, err := NewConsoleSet(good1, good2, bad)
	if err == nil {
		set.Close()
		t.Fatal("expected an error when a socket path is invalid")
	}
	for _, p := range []string{good1, good2} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("socket %s was not rolled back", p)
		}
	}
}

func TestTempConsoleSetLifecycle(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	set, err := NewTempConsoleSet(3)
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 3 {
		t.Fatalf("expected 3 consoles, got %d", set.Len())
	}
	paths := set.Paths()
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("socket %s does not exist: %v", p, err)
		}
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("socket %s was not reclaimed", p)
		}
	}
	// Closing the set again is a no-op.
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConsoleHandleIndependentClose(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	set, err := NewTempConsoleSet(3)
	if err != nil {
		t.Fatal(err)
	}
	paths := set.Paths()

	h := set.Get(1)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	// Repeated close of the same handle is a no-op.
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths[1]); !os.IsNotExist(err) {
		t.Fatal("closed console socket still exists")
	}
	// Siblings are unaffected and can still deliver their consoles.
	for _, i := range []int{0, 2} {
		if _, err := os.Stat(paths[i]); err != nil {
			t.Fatalf("sibling socket %s was closed: %v", paths[i], err)
		}
	}
	sender := sendConsoleMaster(t, paths[0])
	defer sender.Close()
	m, err := set.Get(0).ReceiveMaster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fdIsClosed(m.Fd()) {
		t.Fatal("received master is closed")
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveMasterContextCancel(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	set, err := NewTempConsoleSet(1)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := set.Get(0).ReceiveMaster(ctx)
		errCh <- err
	}()
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("receive was not woken by cancellation")
	}
}

func TestConsoleSetCloseWakesReceiver(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	set, err := NewTempConsoleSet(1)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := set.Get(0).ReceiveMaster(context.Background())
		errCh <- err
	}()
	// Give the receive a chance to block on the socket before closing.
	time.Sleep(100 * time.Millisecond)
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConsoleClosed) {
			t.Fatalf("expected ErrConsoleClosed, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("receive was not woken by close")
	}
}

func TestReceiveMastersPublishesCompleteSet(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	set, err := NewTempConsoleSet(2)
	if err != nil {
		t.Fatal(err)
	}
	paths := set.Paths()
	s0 := sendConsoleMaster(t, paths[0])
	defer s0.Close()
	s1 := sendConsoleMaster(t, paths[1])
	defer s1.Close()

	masters, err := set.ReceiveMasters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(masters) != 2 {
		t.Fatalf("expected 2 masters, got %d", len(masters))
	}
	fds := make([]uintptr, 0, len(masters))
	for _, m := range masters {
		if fdIsClosed(m.Fd()) {
			t.Fatal("published master is closed")
		}
		fds = append(fds, m.Fd())
	}
	// Closing the set reclaims the published pty masters.
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if !fdIsClosed(fd) {
			t.Fatal("master was not reclaimed on close")
		}
	}
}

func TestReceiveMastersRollbackOnCancel(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	set, err := NewTempConsoleSet(2)
	if err != nil {
		t.Fatal(err)
	}
	paths := set.Paths()
	// Only the first console is delivered; the second never arrives.
	sender := sendConsoleMaster(t, paths[0])
	defer sender.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		masters, err := set.ReceiveMasters(ctx)
		if err == nil {
			t.Errorf("expected an error, got %d masters", len(masters))
		}
		if masters != nil {
			t.Error("a partial set of masters was published")
		}
		errCh <- err
	}()

	// Wait until the first master has been received, then cancel as if the
	// process expected to deliver the second console had exited.
	m := waitForMaster(t, set.Get(0))
	fd := m.Fd()
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReceiveMasters was not woken by cancellation")
	}
	// The master received before the failure was rolled back.
	if !fdIsClosed(fd) {
		t.Fatal("received master was not reclaimed on rollback")
	}
	// The whole set was reclaimed, not just the failed console.
	for _, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("socket %s was not reclaimed on rollback", p)
		}
	}
}

func TestSocketCloseIdempotent(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")

	c, err := NewTempConsoleSocket()
	if err != nil {
		t.Fatal(err)
	}
	path := c.Path()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("socket path still exists")
	}
}
