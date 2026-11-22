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
	"io"
	"os"
	"sync"
	"time"
)

const (
	// DefaultIOBufferSize is the per-direction buffer size used by [CopyIO]
	// when no override is supplied.
	DefaultIOBufferSize = 32 << 10

	// DefaultIODrainTimeout bounds how long [Copier.Close] waits for the
	// output directions to reach EOF after input has been stopped and the
	// process stdin has been closed.
	DefaultIODrainTimeout = 5 * time.Second

	// ioDrainCloseGrace is how long Close waits, after forcibly closing the
	// output read ends, for the copiers to observe the closure. It is a small
	// fixed bound so Close returns even when a caller-supplied Writer blocks
	// forever.
	ioDrainCloseGrace = 100 * time.Millisecond

	// numStreams is the number of IO directions a Copier tracks.
	numStreams = 3
)

// ErrDrainTimeout reports that [Copier.Close] reached its drain deadline before
// an output direction reached EOF. The direction's [StreamStatus] records this
// error together with the number of bytes actually delivered before the
// deadline.
var ErrDrainTimeout = errors.New("runc: timed out draining io")

// ErrNoIOStreams is returned by [CopyIO] when none of the requested directions
// has an endpoint on the process side.
var ErrNoIOStreams = errors.New("runc: no io streams to copy")

// Stream identifies one direction of a process's standard IO.
type Stream uint8

const (
	// StreamStdin copies caller input into the process.
	StreamStdin Stream = iota
	// StreamStdout copies the process stdout to the caller.
	StreamStdout
	// StreamStderr copies the process stderr to the caller.
	StreamStderr
)

// String returns the lowercase name of the stream.
func (s Stream) String() string {
	switch s {
	case StreamStdin:
		return "stdin"
	case StreamStdout:
		return "stdout"
	case StreamStderr:
		return "stderr"
	default:
		return fmt.Sprintf("stream(%d)", uint8(s))
	}
}

// StreamStatus is a snapshot of one IO direction's copy state.
//
// Bytes counts the bytes actually transferred in that direction: bytes written
// into the process for stdin, and bytes accepted by the caller's Writer for
// stdout and stderr. It is updated as data moves, so it is meaningful even
// while a direction is still running or after a bounded drain gave up.
//
// Err is the first non-EOF error observed for the direction. A clean EOF (on
// either side) and a requested input stop are not errors. Err is set as soon
// as the failure is observed, even if the direction keeps draining discarded
// bytes so the process is not blocked.
type StreamStatus struct {
	Stream Stream
	// Active reports that a copier goroutine was started for this direction.
	Active bool
	// Finished reports that the copier goroutine has returned.
	Finished bool
	// Bytes is the number of bytes successfully transferred.
	Bytes int64
	// Err is the first terminal error for this direction, or nil.
	Err error
}

// CopyStreams holds the caller-side endpoints for [CopyIO]. A nil field
// disables that direction. The process side comes from the [IO] value.
type CopyStreams struct {
	// Stdin is the source of data written to the process stdin. EOF ends the
	// direction cleanly.
	Stdin io.Reader
	// Stdout receives the process stdout.
	Stdout io.Writer
	// Stderr receives the process stderr.
	Stderr io.Writer
}

// CopyOpt sets options for [CopyIO].
type CopyOpt func(*copyConfig)

type copyConfig struct {
	bufSize      int
	drainTimeout time.Duration
	ctx          context.Context
}

// WithCopyContext ties the copier's lifecycle to ctx. When ctx is canceled the
// copier stops accepting new input, drains the output directions within the
// configured deadline, and releases its handles — the same sequence as
// [Copier.Close]. The copier still must be closed by the caller; the second
// close is a no-op that returns the first result.
func WithCopyContext(ctx context.Context) CopyOpt {
	return func(c *copyConfig) {
		if ctx != nil {
			c.ctx = ctx
		}
	}
}

// WithCopyBufferSize sets the per-direction copy buffer size. A non-positive
// size is ignored and the default is used.
func WithCopyBufferSize(size int) CopyOpt {
	return func(c *copyConfig) {
		if size > 0 {
			c.bufSize = size
		}
	}
}

// WithIODrainTimeout sets how long [Copier.Close] waits for the output
// directions to drain before forcing the pipes closed. A non-positive
// duration is ignored and the default is used.
func WithIODrainTimeout(timeout time.Duration) CopyOpt {
	return func(c *copyConfig) {
		if timeout > 0 {
			c.drainTimeout = timeout
		}
	}
}

// onceCloser releases one endpoint at most once. Repeated and concurrent
// Close calls observe the same result. A nil underlying closer makes Close a
// no-op, which is how endpoints the copier does not own are represented.
type onceCloser struct {
	once sync.Once
	c    io.Closer
	err  error
}

// newEndpoint wraps an endpoint for tracking. The copier releases the endpoint
// only when the IO declared ownership of its streams; otherwise the wrapper
// keeps the endpoint usable but never closes it.
func newEndpoint(owns bool, c io.Closer) *onceCloser {
	if owns {
		return &onceCloser{c: c}
	}
	return &onceCloser{}
}

func (o *onceCloser) Close() error {
	if o == nil || o.c == nil {
		return nil
	}
	o.once.Do(func() {
		if err := o.c.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			o.err = err
		}
	})
	return o.err
}

// releasedElsewhere reports whether err is the result of the endpoint having
// already been released by another owner — the caller's IO.Close, a
// CloseAfterStart, or a concurrent copier teardown. That is a requested
// release, not a copy failure.
func releasedElsewhere(err error) bool {
	return errors.Is(err, os.ErrClosed)
}

func (o *onceCloser) owned() bool { return o != nil && o.c != nil }

// Copier copies between a process's [IO] and caller-supplied streams.
//
// Each direction runs in its own goroutine and records its own
// [StreamStatus], so a failure in one direction never discards data already
// produced in another. A failed output direction stops forwarding to the
// caller but keeps reading (and discarding) the process pipe until EOF, so the
// process is not blocked on a broken sink.
//
// Copier owns the process-side endpoints it copies through when the IO value
// implements [StreamOwner] and reports ownership. In that case
// [Copier.Close] releases them exactly once, and [Copier.StopInput] releases
// the stdin endpoint so the process observes EOF. For any other IO value the
// endpoints are assumed to belong to the caller and are never closed.
//
// The Copier does not own the IO value itself nor the caller's Readers and
// Writers; closing the IO remains the caller's responsibility. Because pipe
// handles are idempotent, the IO and the Copier may be closed in any order.
type Copier struct {
	cfg copyConfig

	ends [numStreams]*onceCloser

	stdinDst  io.Writer
	stdoutSrc io.Reader
	stderrSrc io.Reader

	stopInput chan struct{}
	stopOnce  sync.Once

	done [numStreams]chan struct{}

	mu     sync.Mutex
	status [numStreams]StreamStatus
	forced [numStreams]bool

	closeOnce sync.Once
	closeErr  error
	closed    chan struct{}
}

// CopyIO starts copying between the process-side endpoints of term and the
// caller-side endpoints in streams. Directions without both endpoints are
// skipped. The returned Copier must be closed by the caller.
//
// The endpoints are obtained from term.Stdin(), term.Stdout() and
// term.Stderr() when copying starts. Whether the Copier may release them is
// decided by [StreamOwner]; see the Copier documentation. The caller keeps
// ownership of the values in streams.
func CopyIO(term IO, streams CopyStreams, opts ...CopyOpt) (*Copier, error) {
	if term == nil {
		return nil, ErrNoIOStreams
	}
	cfg := copyConfig{
		bufSize:      DefaultIOBufferSize,
		drainTimeout: DefaultIODrainTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	c := &Copier{
		cfg:       cfg,
		stopInput: make(chan struct{}),
		closed:    make(chan struct{}),
	}
	for s := 0; s < numStreams; s++ {
		c.status[s].Stream = Stream(s)
		c.done[s] = make(chan struct{})
	}

	owns := false
	if owner, ok := term.(StreamOwner); ok {
		owns = owner.OwnsStreams()
	}

	if streams.Stdin != nil {
		if w := term.Stdin(); w != nil {
			c.stdinDst = w
			c.ends[StreamStdin] = newEndpoint(owns, w)
		}
	}
	if streams.Stdout != nil {
		if r := term.Stdout(); r != nil {
			c.stdoutSrc = r
			c.ends[StreamStdout] = newEndpoint(owns, r)
		}
	}
	if streams.Stderr != nil {
		if r := term.Stderr(); r != nil {
			c.stderrSrc = r
			c.ends[StreamStderr] = newEndpoint(owns, r)
		}
	}

	active := false
	for s := 0; s < numStreams; s++ {
		c.status[s].Active = c.ends[s] != nil
		if c.status[s].Active {
			active = true
		}
	}
	if !active {
		return nil, ErrNoIOStreams
	}

	if c.ends[StreamStdin] != nil {
		go c.run(StreamStdin, func() error {
			return c.copyInput(streams.Stdin)
		})
	}
	if c.ends[StreamStdout] != nil {
		go c.run(StreamStdout, func() error {
			return c.copyOutput(StreamStdout, streams.Stdout)
		})
	}
	if c.ends[StreamStderr] != nil {
		go c.run(StreamStderr, func() error {
			return c.copyOutput(StreamStderr, streams.Stderr)
		})
	}
	if cfg.ctx != nil {
		go c.watchContext(cfg.ctx)
	}
	return c, nil
}

// watchContext performs the shutdown sequence when the context is canceled.
// Cancellation is advisory: Close remains the owner of the release, so the
// caller is still required to close the copier (a no-op if this already ran).
func (c *Copier) watchContext(ctx context.Context) {
	select {
	case <-ctx.Done():
		_ = c.Close()
	case <-c.closed:
	}
}

// run executes one direction's copy loop, publishes its terminal state and
// signals completion. Status updates before this point are additive and are
// preserved here.
//
// When the input direction ends — whether the caller's Reader reached EOF, the
// input was stopped, or a read failed — the process stdin endpoint is released
// (if the copier owns it) so the process observes EOF and can finish.
func (c *Copier) run(stream Stream, copyFn func() error) {
	err := copyFn()

	if stream == StreamStdin {
		if cerr := c.ends[StreamStdin].Close(); cerr != nil && err == nil && !releasedElsewhere(cerr) {
			err = cerr
		}
	}
	// A read or write that fails because another owner released the handle is
	// a requested shutdown, not a copy failure. Keep the bytes transferred so
	// far and report the direction as finished without an error.
	if releasedElsewhere(err) {
		err = nil
	}

	c.mu.Lock()
	st := &c.status[stream]
	st.Finished = true
	if st.Err == nil && err != nil {
		st.Err = err
	}
	c.mu.Unlock()

	close(c.done[stream])
}

// stopping reports whether input forwarding has been stopped. Reads and writes
// on the input direction check it so no new data is written after a stop.
func (c *Copier) stopping() bool {
	select {
	case <-c.stopInput:
		return true
	default:
		return false
	}
}

func (c *Copier) addBytes(stream Stream, n int64) {
	if n == 0 {
		return
	}
	c.mu.Lock()
	c.status[stream].Bytes += n
	c.mu.Unlock()
}

func (c *Copier) recordError(stream Stream, err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	if c.status[stream].Err == nil {
		c.status[stream].Err = err
	}
	c.mu.Unlock()
}

func (c *Copier) markForced(stream Stream) {
	c.mu.Lock()
	c.forced[stream] = true
	c.mu.Unlock()
}

func (c *Copier) forciblyClosed(stream Stream) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.forced[stream]
}

// copyInput forwards the caller's Reader into the process stdin. A short write
// is reported as [io.ErrShortWrite]; the byte count still reflects the bytes
// the writer accepted. Once input is stopped, the loop returns cleanly without
// writing more data.
func (c *Copier) copyInput(src io.Reader) error {
	buf := make([]byte, c.cfg.bufSize)
	for {
		if c.stopping() {
			return nil
		}
		nr, rerr := src.Read(buf)
		if nr > 0 {
			if c.stopping() {
				return nil
			}
			nw, werr := c.stdinDst.Write(buf[:nr])
			c.addBytes(StreamStdin, int64(nw))
			if werr != nil {
				if c.stopping() && errors.Is(werr, os.ErrClosed) {
					// Close stopped input and released the endpoint while
					// this write was in flight. That is a requested stop,
					// not a copy failure.
					return nil
				}
				return werr
			}
			if nw < nr {
				return io.ErrShortWrite
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return rerr
		}
	}
}

// copyOutput forwards the process output pipe into the caller's Writer. If the
// Writer fails, the first error is recorded and the remaining data is read and
// discarded until EOF, so a broken sink never stalls the process. Output bytes
// are counted only when the Writer accepts them.
func (c *Copier) copyOutput(stream Stream, dst io.Writer) error {
	src := c.stdoutSrc
	if stream == StreamStderr {
		src = c.stderrSrc
	}
	buf := make([]byte, c.cfg.bufSize)
	var outErr error
	for {
		nr, rerr := src.Read(buf)
		if nr > 0 && outErr == nil {
			nw, werr := dst.Write(buf[:nr])
			c.addBytes(stream, int64(nw))
			switch {
			case werr != nil:
				outErr = werr
				c.recordError(stream, werr)
			case nw < nr:
				outErr = io.ErrShortWrite
				c.recordError(stream, io.ErrShortWrite)
			}
		}
		if rerr != nil {
			switch {
			case errors.Is(rerr, io.EOF):
				return outErr
			case outErr != nil:
				return outErr
			case c.forciblyClosed(stream) && errors.Is(rerr, os.ErrClosed):
				return fmt.Errorf("%w: %s", ErrDrainTimeout, stream)
			default:
				return rerr
			}
		}
	}
}

// StopInput stops forwarding caller input to the process. When the IO value
// owns its streams (see [StreamOwner]) it also releases the process stdin
// endpoint, which delivers EOF to the process; otherwise forwarding stops but
// the caller's descriptor is left open. It is idempotent and does not affect
// the output directions.
func (c *Copier) StopInput() error {
	if c == nil || c.ends[StreamStdin] == nil {
		return nil
	}
	c.stopOnce.Do(func() { close(c.stopInput) })
	return c.ends[StreamStdin].Close()
}

// Close stops new input, drains the output directions within the configured
// deadline, and releases every process-side endpoint exactly once.
//
// Draining happens before the output handles are closed, so output already
// produced by the process is forwarded rather than discarded. If a direction
// does not reach EOF within the deadline, its read end is closed to unblock
// the copier and its [StreamStatus] reports [ErrDrainTimeout] with the bytes
// delivered so far.
//
// Close is safe to call repeatedly and concurrently; every call returns the
// same error, which joins endpoint release failures with the copy errors
// recorded per direction.
func (c *Copier) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeErr = c.close()
		close(c.closed)
	})
	return c.closeErr
}

func (c *Copier) close() error {
	var errs []error
	if err := c.StopInput(); err != nil {
		errs = append(errs, fmt.Errorf("stdin: %w", err))
	}

	outputs := make([]Stream, 0, 2)
	for _, s := range []Stream{StreamStdout, StreamStderr} {
		if c.ends[s] != nil {
			outputs = append(outputs, s)
		}
	}

	timedOut := false
	if len(outputs) > 0 {
		if !waitAll(outputs, c.done, c.cfg.drainTimeout) {
			timedOut = true
			// Stop any remaining copy from blocking forever on the process
			// pipe by closing its read end. Data already forwarded is kept.
			for _, s := range outputs {
				if !c.ends[s].owned() {
					// The descriptor belongs to the caller; the copier can
					// only report the timeout, not unblock the read.
					continue
				}
				c.markForced(s)
				if err := c.ends[s].Close(); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", s, err))
				}
			}
			// Give the unblocked copiers a moment to publish their terminal
			// status before it is read below.
			waitAll(outputs, c.done, ioDrainCloseGrace)
		}
	}

	// Release every endpoint exactly once, including the output ends that
	// drained cleanly and the stdin end if StopInput was a no-op.
	for s := 0; s < numStreams; s++ {
		if c.ends[s] == nil {
			continue
		}
		if err := c.ends[s].Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", Stream(s), err))
		}
	}

	c.mu.Lock()
	// Only an output direction can fail to drain: Close never waits for the
	// input direction, so an unfinished stdin copier is not a drain timeout.
	for _, s := range outputs {
		st := &c.status[s]
		if !st.Active || st.Finished {
			continue
		}
		if timedOut && st.Err == nil {
			st.Err = fmt.Errorf("%w: %s", ErrDrainTimeout, Stream(s))
		}
	}
	for s := 0; s < numStreams; s++ {
		if st := c.status[s]; st.Active && st.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", Stream(s), st.Err))
		}
	}
	c.mu.Unlock()

	return errors.Join(collectReleaseErrors(errs)...)
}

// waitAll reports whether every stream's done channel closed within timeout.
// It uses a single deadline for the whole group and leaves no goroutine behind.
func waitAll(streams []Stream, done [numStreams]chan struct{}, timeout time.Duration) bool {
	if len(streams) == 0 {
		return true
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for _, s := range streams {
		select {
		case <-done[s]:
		case <-deadline.C:
			return false
		}
	}
	return true
}

// validateCopyStreams reports a misconfiguration before a command is built.
// Requesting CopyStreams without an IO is rejected up front rather than
// silently falling back to buffered output.
func validateCopyStreams(term IO, streams *CopyStreams) error {
	if streams == nil {
		return nil
	}
	if term == nil {
		return errors.New("runc: CopyStreams requires a non-nil IO")
	}
	return nil
}

// newCommandCopier starts the copier requested through the CopyStreams option
// of a command entrypoint. It returns a nil copier when no streams were
// requested, so callers can pass the result straight to finishCommandCopier.
func newCommandCopier(term IO, streams *CopyStreams, opts []CopyOpt) (*Copier, error) {
	if err := validateCopyStreams(term, streams); err != nil {
		return nil, err
	}
	if streams == nil {
		return nil, nil
	}
	return CopyIO(term, *streams, opts...)
}

// finishCommandCopier closes the copier started by newCommandCopier and joins
// any copy or drain error with the command's own result. A nil copier leaves
// the result untouched.
func finishCommandCopier(c *Copier, err error) error {
	if c == nil {
		return err
	}
	return errors.Join(err, c.Close())
}

// abortCommandCopier releases a copier without draining. It is used when the
// command never started, or when the post-start IO handoff failed and the
// output pipes therefore can never reach EOF: draining would only stall until
// the deadline. The command's own error is preserved.
func abortCommandCopier(c *Copier, err error) error {
	if c == nil {
		return err
	}
	return errors.Join(err, c.Abort())
}

// Abort stops the copier without draining: it stops new input, releases every
// process-side endpoint it owns, and returns immediately. Use it when the
// process never ran (for example a failed start) or when waiting for the
// output to drain has no value. After Abort, Close returns the same result.
func (c *Copier) Abort() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeErr = c.abort()
		close(c.closed)
	})
	return c.closeErr
}

// collectReleaseErrors appends the non-"already closed" close errors to errs.
// A handle released by another owner is a requested shutdown.
func collectReleaseErrors(errs []error) []error {
	kept := errs[:0]
	for _, err := range errs {
		if !releasedElsewhere(err) {
			kept = append(kept, err)
		}
	}
	return kept
}

func (c *Copier) abort() error {
	var errs []error
	if err := c.StopInput(); err != nil {
		errs = append(errs, fmt.Errorf("stdin: %w", err))
	}
	for s := 0; s < numStreams; s++ {
		if c.ends[s] == nil {
			continue
		}
		if err := c.ends[s].Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", Stream(s), err))
		}
	}
	c.mu.Lock()
	for s := 0; s < numStreams; s++ {
		if st := c.status[s]; st.Active && st.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", Stream(s), st.Err))
		}
	}
	c.mu.Unlock()
	return errors.Join(collectReleaseErrors(errs)...)
}

// Status returns a snapshot of one direction's copy state.
func (c *Copier) Status(stream Stream) StreamStatus {
	if c == nil || stream >= numStreams {
		return StreamStatus{Stream: stream}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status[stream]
}

// Statuses returns a snapshot of all three directions.
func (c *Copier) Statuses() []StreamStatus {
	out := make([]StreamStatus, 0, numStreams)
	if c == nil {
		return out
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for s := 0; s < numStreams; s++ {
		out = append(out, c.status[s])
	}
	return out
}

// Err returns the errors recorded for the active directions, joined together.
// It reflects a snapshot and may be called before or after Close.
func (c *Copier) Err() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for s := 0; s < numStreams; s++ {
		if st := c.status[s]; st.Active && st.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", Stream(s), st.Err))
		}
	}
	return errors.Join(errs...)
}

// Wait blocks until every active direction has finished. A caller-side Reader
// that never returns and a Writer that blocks forever can delay a direction
// indefinitely; use Close for a bounded operation.
func (c *Copier) Wait() {
	if c == nil {
		return
	}
	for s := 0; s < numStreams; s++ {
		if c.ends[s] != nil {
			<-c.done[s]
		}
	}
}
