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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// lockSuffix is appended to a socket path to derive the path of the lock file
// that owns it.
const lockSuffix = ".lock"

// ErrSocketInUse is returned when a socket path is already owned by a live
// listener. The caller must not remove the path.
var ErrSocketInUse = errors.New("socket path is in use by a live listener")

// Socket is a unix socket that accepts a file descriptor sent by runc, such as
// the pty master for the container's console or the pidfd of the process runc
// creates.
//
// A Socket owns exactly one path on disk for its whole lifetime. Ownership is
// established by an exclusive flock on a lock file next to the socket and an
// owner record written into that lock file. The same record controls creation,
// publication (Path) and cleanup (Close), so a path left behind by a process
// that crashed can be safely taken over while a path owned by a live listener
// or by another user is refused rather than removed.
type Socket struct {
	l *net.UnixListener

	// path is the published socket pathname; lockPath is the lock file that
	// holds the exclusive flock and the owner record.
	path     string
	lockPath string
	lockFile *os.File

	// socketInode and lockInode identify the exact filesystem objects this
	// Socket created. Close only unlinks objects whose inode still matches,
	// so a replacement created after a crash is never removed.
	socketInode uint64
	lockInode   uint64

	// rmdir marks sockets created inside a private temporary directory that
	// must be removed on Close.
	rmdir bool

	closeOnce sync.Once
	closeErr  error
}

// socketOwner is the ownership record stored in the lock file. It records who
// created the socket and which filesystem objects belong to it, allowing a
// later startup to distinguish a safely reclaimable stale path from a path it
// must not touch.
type socketOwner struct {
	// Pid is the pid of the process that created the socket.
	Pid int `json:"pid"`
	// StartTime is the boot-based start time of Pid (field 22 of
	// /proc/<pid>/stat on Linux); zero on platforms that cannot report it.
	// Together with Pid it identifies the process across pid reuse.
	StartTime uint64 `json:"start_time"`
	// UID is the effective user id that owns the socket.
	UID int `json:"uid"`
	// SocketInode is the inode of the bound socket pathname.
	SocketInode uint64 `json:"socket_inode"`
	// LockInode is the inode of the lock file holding this record.
	LockInode uint64 `json:"lock_inode"`
}

// newSocket creates a new unix socket listening at the provided path.
//
// If the path already exists it is never removed blindly: it must be a unix
// socket owned by the caller's uid with no live listener, and the lock file's
// owner record must prove that the process which created it is gone. Such a
// stale path is taken over; a live listener, a foreign or non-socket path, or
// a path without a valid owner record is rejected with an error and left
// untouched.
func newSocket(path string) (_ *Socket, retErr error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveUnixAddr("unix", abs)
	if err != nil {
		return nil, err
	}

	lockPath := abs + lockSuffix
	lockFile, err := acquireSocketLock(lockPath)
	if err != nil {
		return nil, err
	}
	// On any failure before the Socket is published the lock is released. The
	// lock file itself is kept so a live owner's state is never disturbed.
	defer func() {
		if retErr != nil {
			_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
			_ = lockFile.Close()
		}
	}()

	if err := prepareSocketPath(abs, lockFile); err != nil {
		return nil, err
	}

	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return nil, err
	}
	// We own unlinking the pathname ourselves, gated on the inode recorded
	// below. Go's default unlink-on-close would bypass that check.
	l.SetUnlinkOnClose(false)

	socketInode, err := pathInode(abs)
	if err != nil {
		// The bind succeeded, so the pathname is ours: undo it. With the
		// socket gone, a later startup finds no path and starts clean instead
		// of tripping over a recordless socket.
		_ = l.Close()
		_ = os.Remove(abs) // #nosec G703 -- abs is the socket this call just bound while holding the exclusive lock.
		return nil, err
	}
	lockInode, err := pathInode(lockPath)
	if err != nil {
		_ = l.Close()
		_ = os.Remove(abs) // #nosec G703 -- abs is the socket this call just bound while holding the exclusive lock.
		return nil, err
	}

	record := socketOwner{
		Pid:         os.Getpid(),
		StartTime:   processStartTime(os.Getpid()),
		UID:         os.Geteuid(),
		SocketInode: socketInode,
		LockInode:   lockInode,
	}
	if err := writeOwnerRecord(lockFile, record); err != nil {
		_ = l.Close()
		_ = os.Remove(abs) // #nosec G703 -- abs is the socket this call just bound while holding the exclusive lock.
		return nil, err
	}

	return &Socket{
		l:           l,
		path:        abs,
		lockPath:    lockPath,
		lockFile:    lockFile,
		socketInode: socketInode,
		lockInode:   lockInode,
	}, nil
}

// acquireSocketLock creates (if needed) and exclusively locks the lock file at
// lockPath. The lock is non-blocking: a path another live process holds results
// in an error instead of waiting, which keeps concurrent startups from
// queuing up behind each other. The kernel releases the flock if the holder
// process dies, including on SIGKILL, so a crashed holder's lock can always be
// acquired again.
func acquireSocketLock(lockPath string) (*os.File, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G703 -- lockPath is derived from the caller-provided socket path made absolute by newSocket.
	if err != nil {
		return nil, fmt.Errorf("open socket lock %q: %w", lockPath, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("socket path %q: %w", strings.TrimSuffix(lockPath, lockSuffix), ErrSocketInUse)
		}
		return nil, fmt.Errorf("lock socket path %q: %w", lockPath, err)
	}
	return f, nil
}

// prepareSocketPath validates an existing socket path and removes it only when
// it is provably a stale socket created by a process of ours that no longer
// exists. Any other case returns an error without touching the path. The
// caller must hold the exclusive lock on lockFile.
func prepareSocketPath(path string, lockFile *os.File) error {
	info, err := os.Lstat(path) // #nosec G703 -- path is the absolute socket path the caller asked to bind, stat-tested before any removal.
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("socket path %q exists but is not a socket", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot inspect owner of socket path %q", path)
	}
	if int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("socket path %q is owned by uid %d, refusing to remove another user's path", path, stat.Uid)
	}
	if socketIsLive(path) {
		return fmt.Errorf("socket path %q: %w", path, ErrSocketInUse)
	}

	record, err := readOwnerRecord(lockFile)
	if err != nil {
		return fmt.Errorf("socket path %q is stale but has no usable owner record, refusing to remove it: %w", path, err)
	}
	if record.SocketInode != stat.Ino {
		return fmt.Errorf("socket path %q does not match its owner record (inode %d != %d), refusing to remove it", path, stat.Ino, record.SocketInode)
	}
	if ownerAlive(record) {
		return fmt.Errorf("socket path %q: %w", path, ErrSocketInUse)
	}
	if err := os.Remove(path); err != nil { // #nosec G703 -- removal happens only after the socket type, uid, liveness and owner-record checks above prove this path is our own stale socket.
		return fmt.Errorf("take over stale socket path %q: %w", path, err)
	}
	return nil
}

// socketIsLive reports whether something accepts connections on path. A
// connection (or an in-progress handshake) means a live listener owns the path;
// ECONNREFUSED means the pathname is stale. Anything ambiguous is treated as
// live so a working listener is never removed.
//
// This uses a raw connect on purpose: net.Dial retries ECONNREFUSED until its
// timeout elapses, which would report every stale socket as live. A single
// connect gives the kernel's immediate verdict, the same one runc gets as a
// client.
func socketIsLive(path string) bool {
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return true
	}
	defer unix.Close(fd)

	// Bound the connect so a path that never responds cannot stall startup.
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO,
		&unix.Timeval{Sec: 0, Usec: 200_000})

	connErr := unix.Connect(fd, &unix.SockaddrUnix{Name: path})
	if connErr == nil {
		return true
	}
	// ECONNREFUSED is the definitive sign of a stale pathname. EINPROGRESS
	// means the handshake was accepted by a live listener; everything else is
	// ambiguous and leaves the path untouched. ENAMETOOLONG also cannot be a
	// verdict about liveness: the socket could be perfectly healthy, so treat
	// it as live and let bind report the real failure.
	return !errors.Is(connErr, unix.ECONNREFUSED)
}

// writeOwnerRecord truncates lockFile and writes the ownership record as JSON.
func writeOwnerRecord(lockFile *os.File, record socketOwner) error {
	data, err := json.Marshal(&record)
	if err != nil {
		return err
	}
	if err := lockFile.Truncate(0); err != nil {
		return err
	}
	if _, err := lockFile.WriteAt(data, 0); err != nil {
		return err
	}
	return lockFile.Sync()
}

// readOwnerRecord reads the JSON ownership record from lockFile. An empty lock
// file (for example one created by hand or by an older version) is reported as
// an error so the caller refuses rather than guesses.
func readOwnerRecord(lockFile *os.File) (socketOwner, error) {
	data, err := os.ReadFile(lockFile.Name()) // #nosec G703 -- the lock file was opened by acquireSocketLock next to the absolute socket path and is held under an exclusive flock.
	if err != nil {
		return socketOwner{}, err
	}
	var record socketOwner
	if len(data) == 0 {
		return socketOwner{}, errors.New("empty owner record")
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return socketOwner{}, err
	}
	return record, nil
}

// pathInode returns the inode of the filesystem object at path.
func pathInode(path string) (uint64, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return 0, err
	}
	return st.Ino, nil
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

// Close closes the unix socket and releases its path.
//
// Cleanup is identity-checked: the socket pathname and lock file are unlinked
// only while this Socket still holds the exclusive lock and only if their
// current inodes match the ones recorded at creation. A listener that replaced
// this one after a crash is therefore never removed. It is safe to call Close
// multiple times.
func (c *Socket) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.close()
	})
	return c.closeErr
}

func (c *Socket) close() error {
	var err error

	// Stop accepting first. The flock is still held, so no other process can
	// take over the path until the unlinks below are done. A nil listener
	// means this Socket only carries a stale cleanup record (see tests).
	if c.l != nil {
		if lErr := c.l.Close(); lErr != nil {
			err = lErr
		}
	}

	if inode, iErr := pathInode(c.path); iErr == nil && inode == c.socketInode {
		if rErr := os.Remove(c.path); rErr != nil && err == nil { // #nosec G703 -- the inode match proves the path is still the socket this Socket created and bound.
			err = rErr
		}
	}
	if inode, iErr := pathInode(c.lockPath); iErr == nil && inode == c.lockInode {
		if rErr := os.Remove(c.lockPath); rErr != nil && err == nil { // #nosec G703 -- the inode match proves the lock file still belongs to this Socket.
			err = rErr
		}
	}
	if c.rmdir {
		// The directory was created privately by newTempSocket and only ever
		// contains this socket's files.
		if rErr := os.RemoveAll(filepath.Dir(c.path)); rErr != nil && err == nil { // #nosec G703 -- rmdir is set only for sockets created in a private temporary directory.
			err = rErr
		}
	}

	if c.lockFile != nil {
		if uErr := unix.Flock(int(c.lockFile.Fd()), unix.LOCK_UN); uErr != nil && err == nil {
			err = uErr
		}
		if cErr := c.lockFile.Close(); cErr != nil && err == nil {
			err = cErr
		}
	}
	return err
}

// receive blocks until the socket receives a file descriptor
func (c *Socket) receive() (*os.File, error) {
	conn, err := c.l.Accept()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, fmt.Errorf("received connection which was not a unix socket")
	}
	return recvFd(uc)
}

// recvFd waits for a file descriptor to be sent over the given AF_UNIX
// socket. The file name of the remote file descriptor will be recreated
// locally (it is sent as non-auxiliary data in the same payload).
func recvFd(socket *net.UnixConn) (*os.File, error) {
	const MaxNameLen = 4096
	oobSpace := unix.CmsgSpace(4)

	name := make([]byte, MaxNameLen)
	oob := make([]byte, oobSpace)

	n, oobn, _, _, err := socket.ReadMsgUnix(name, oob)
	if err != nil {
		return nil, err
	}

	if n >= MaxNameLen || oobn != oobSpace {
		return nil, fmt.Errorf("recvfd: incorrect number of bytes read (n=%d oobn=%d)", n, oobn)
	}

	// Truncate.
	name = name[:n]
	oob = oob[:oobn]

	scms, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, err
	}
	if len(scms) != 1 {
		return nil, fmt.Errorf("recvfd: number of SCMs is not 1: %d", len(scms))
	}
	scm := scms[0]

	fds, err := unix.ParseUnixRights(&scm)
	if err != nil {
		return nil, err
	}
	if len(fds) != 1 {
		return nil, fmt.Errorf("recvfd: number of fds is not 1: %d", len(fds))
	}
	fd := uintptr(fds[0])

	return os.NewFile(fd, string(name)), nil
}
