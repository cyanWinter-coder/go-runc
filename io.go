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
	"io"
	"os"
	"os/exec"
	"sync"
)

// IO is the terminal IO interface
type IO interface {
	io.Closer
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Set(*exec.Cmd)
}

// StartCloser is an interface to handle IO closure after start
type StartCloser interface {
	CloseAfterStart() error
}

// StreamOwner is implemented by IO values whose Stdin, Stdout and Stderr
// endpoints are process-side pipe handles owned by the IO implementation.
//
// A [Copier] releases the endpoints it copies through only when the IO reports
// ownership by implementing this interface and returning true. For every other
// IO value — including [NewSTDIO] and caller-supplied implementations — the
// copier never closes the returned descriptors, because they may refer to the
// caller's own files.
type StreamOwner interface {
	OwnsStreams() bool
}

// IOOpt sets I/O creation options
type IOOpt func(*IOOption)

// IOOption holds I/O creation options
type IOOption struct {
	OpenStdin  bool
	OpenStdout bool
	OpenStderr bool
}

func defaultIOOption() *IOOption {
	return &IOOption{
		OpenStdin:  true,
		OpenStdout: true,
		OpenStderr: true,
	}
}

func newPipe() (*pipe, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	return &pipe{
		r: r,
		w: w,
	}, nil
}

type pipe struct {
	r *os.File
	w *os.File

	// closeOnce and the fields below make Close idempotent and safe to call
	// concurrently from the copier, CloseAfterStart and the caller's own
	// Close. The handles are released exactly once and every caller observes
	// the same error.
	closeOnce sync.Once
	closeErr  error
}

// Close releases both ends of the pipe exactly once. The write end is closed
// first so an outstanding relay reading the read end observes EOF; any error
// from the write end takes precedence over one from the read end, and
// failures from both are joined.
func (p *pipe) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		var errs []error
		if p.w != nil {
			if err := p.w.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				errs = append(errs, err)
			}
		}
		if p.r != nil {
			if err := p.r.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				errs = append(errs, err)
			}
		}
		p.closeErr = errors.Join(errs...)
	})
	return p.closeErr
}

// NewPipeIO creates pipe pairs to be used with runc. It is not implemented
// on Windows.
func NewPipeIO(uid, gid int, opts ...IOOpt) (i IO, err error) {
	return newPipeIO(uid, gid, opts...)
}

type pipeIO struct {
	in  *pipe
	out *pipe
	err *pipe
}

func (i *pipeIO) Stdin() io.WriteCloser {
	if i.in == nil {
		return nil
	}
	return i.in.w
}

func (i *pipeIO) Stdout() io.ReadCloser {
	if i.out == nil {
		return nil
	}
	return i.out.r
}

func (i *pipeIO) Stderr() io.ReadCloser {
	if i.err == nil {
		return nil
	}
	return i.err.r
}

// OwnsStreams reports that the endpoints returned by pipeIO are pipe handles
// owned by this value, so a copier may release them.
func (i *pipeIO) OwnsStreams() bool { return true }

func (i *pipeIO) Close() error {
	var err error
	for _, v := range []*pipe{
		i.in,
		i.out,
		i.err,
	} {
		if v != nil {
			if cerr := v.Close(); err == nil {
				err = cerr
			}
		}
	}
	return err
}

// CloseAfterStart releases the process-side write ends of the output pipes so
// the consumer side observes EOF. It is idempotent and safe to call
// concurrently with Close: an already-closed write end is not an error, and
// any real close failure is reported instead of being discarded.
func (i *pipeIO) CloseAfterStart() error {
	var errs []error
	for _, f := range []*pipe{
		i.out,
		i.err,
	} {
		if f == nil || f.w == nil {
			continue
		}
		if err := f.w.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Set sets the io to the exec.Cmd
func (i *pipeIO) Set(cmd *exec.Cmd) {
	if i.in != nil {
		cmd.Stdin = i.in.r
	}
	if i.out != nil {
		cmd.Stdout = i.out.w
	}
	if i.err != nil {
		cmd.Stderr = i.err.w
	}
}

// NewSTDIO returns I/O setup for standard OS in/out/err usage
func NewSTDIO() (IO, error) {
	return &stdio{}, nil
}

type stdio struct{}

func (s *stdio) Close() error {
	return nil
}

func (s *stdio) Set(cmd *exec.Cmd) {
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
}

func (s *stdio) Stdin() io.WriteCloser {
	return os.Stdin
}

func (s *stdio) Stdout() io.ReadCloser {
	return os.Stdout
}

func (s *stdio) Stderr() io.ReadCloser {
	return os.Stderr
}

// NewNullIO returns IO setup for /dev/null use with runc
func NewNullIO() (IO, error) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		return nil, err
	}
	return &nullIO{
		devNull: f,
	}, nil
}

type nullIO struct {
	devNull *os.File

	closeOnce sync.Once
	closeErr  error
}

// close releases /dev/null exactly once. Close and CloseAfterStart share the
// same state, so a double close (for example CloseAfterStart followed by
// Close) reports success rather than an already-closed error.
func (n *nullIO) close() error {
	n.closeOnce.Do(func() {
		if err := n.devNull.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			n.closeErr = err
		}
	})
	return n.closeErr
}

func (n *nullIO) Close() error {
	// this should be closed after start but if not
	// make sure we close the file; the error is retained for callers that
	// close explicitly and shared with CloseAfterStart.
	return n.close()
}

func (n *nullIO) Stdin() io.WriteCloser {
	return nil
}

func (n *nullIO) Stdout() io.ReadCloser {
	return nil
}

func (n *nullIO) Stderr() io.ReadCloser {
	return nil
}

func (n *nullIO) Set(c *exec.Cmd) {
	// don't set STDIN here
	c.Stdout = n.devNull
	c.Stderr = n.devNull
}

func (n *nullIO) CloseAfterStart() error {
	return n.close()
}
