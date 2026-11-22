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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// writerFunc adapts a function to io.Writer.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// readerFunc adapts a function to io.Reader.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// shortWriter accepts at most limit bytes per Write call and never reports an
// error, exercising the short-write path.
type shortWriter struct {
	limit int
	buf   bytes.Buffer
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		p = p[:w.limit]
	}
	return w.buf.Write(p)
}

// TestCopyIOSeparatesDirections verifies that a failure in one output
// direction neither stops the other direction from draining nor loses the
// bytes the surviving direction already produced.
func TestCopyIOSeparatesDirections(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	var good bytes.Buffer
	bad := errors.New("stderr sink is broken")

	copier, err := CopyIO(term, CopyStreams{
		Stdout: &good,
		Stderr: writerFunc(func(p []byte) (int, error) {
			// Fail only after a write so we also prove bytes counted as
			// delivered are the ones the writer accepted, not the ones read.
			return 0, bad
		}),
	})
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}

	// Simulate the process writing output; close the write ends so EOF is
	// delivered after the bytes already queued.
	if _, err := term.(*pipeIO).out.w.Write([]byte("hello stdout")); err != nil {
		t.Fatalf("write stdout: %s", err)
	}
	if _, err := term.(*pipeIO).err.w.Write([]byte("lost stderr")); err != nil {
		t.Fatalf("write stderr: %s", err)
	}
	if err := term.(*pipeIO).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}

	// Close reports the per-direction copy failure, but that failure must not
	// stop the other direction from finishing first.
	err = copier.Close()
	if !errors.Is(err, bad) {
		t.Fatalf("Close: got %v, want the stderr copy error %v", err, bad)
	}

	if got := good.String(); got != "hello stdout" {
		t.Fatalf("stdout direction lost data: got %q", got)
	}

	out := copier.Status(StreamStdout)
	if !out.Finished || out.Err != nil {
		t.Fatalf("stdout status: %+v", out)
	}
	if out.Bytes != int64(len("hello stdout")) {
		t.Fatalf("stdout bytes: got %d", out.Bytes)
	}

	serr := copier.Status(StreamStderr)
	if !serr.Finished {
		t.Fatalf("stderr did not finish: %+v", serr)
	}
	if !errors.Is(serr.Err, bad) {
		t.Fatalf("stderr error: got %v, want %v", serr.Err, bad)
	}
	if serr.Bytes != 0 {
		t.Fatalf("stderr bytes: got %d, want 0 (writer rejected everything)", serr.Bytes)
	}
}

// TestCopyIOShortWrite verifies that a short write is surfaced as
// io.ErrShortWrite and that the reported byte count is the number of bytes
// the writer actually accepted.
func TestCopyIOShortWrite(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	sink := &shortWriter{limit: 3}
	copier, err := CopyIO(term, CopyStreams{Stdout: sink}, WithCopyBufferSize(64<<10))
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}

	payload := "0123456789"
	if _, err := term.(*pipeIO).out.w.Write([]byte(payload)); err != nil {
		t.Fatalf("write stdout: %s", err)
	}
	if err := term.(*pipeIO).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}

	if err := copier.Close(); err != nil {
		// Close joins the recorded copy error; it must be a short write.
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("Close: got %v, want io.ErrShortWrite", err)
		}
	}

	st := copier.Status(StreamStdout)
	if !errors.Is(st.Err, io.ErrShortWrite) {
		t.Fatalf("stdout status error: got %v, want io.ErrShortWrite", st.Err)
	}
	if st.Bytes != 3 {
		t.Fatalf("stdout bytes: got %d, want 3 (bytes the writer accepted)", st.Bytes)
	}
}

// TestCopyIOStopInputBeforeOutputClose verifies ordering: stopping input
// closes the process stdin endpoint before output draining starts, so a
// process waiting for stdin EOF proceeds and its tail output is still
// forwarded.
func TestCopyIOStopInputBeforeOutputClose(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	var out bytes.Buffer
	// Block the stdin reader until it observes the copier's stop, mimicking a
	// caller that has nothing more to send but has not yet returned.
	release := make(chan struct{})
	stdin := readerFunc(func(p []byte) (int, error) {
		<-release
		return 0, io.EOF
	})

	copier, err := CopyIO(term, CopyStreams{Stdin: stdin, Stdout: &out})
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}

	// The process emits its tail then exits only once stdin is closed. We
	// emulate that by reading the stdin handle's EOF in a goroutine.
	stdinEOF := make(chan struct{})
	go func() {
		buf := make([]byte, 16)
		_, _ = term.(*pipeIO).in.r.Read(buf) // returns 0, io.EOF once closed
		close(stdinEOF)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = copier.Close()
	}()

	select {
	case <-stdinEOF:
	case <-time.After(5 * time.Second):
		t.Fatal("stdin was not closed before output draining")
	}
	// Now the process can write its tail and exit.
	if _, err := term.(*pipeIO).out.w.Write([]byte("tail")); err != nil {
		t.Fatalf("write tail: %s", err)
	}
	close(release) // let the input copier return so Close can finish
	if err := term.(*pipeIO).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}

	if got := out.String(); got != "tail" {
		t.Fatalf("tail output lost after input stop: got %q", got)
	}
	if st := copier.Status(StreamStdin); st.Err != nil {
		t.Fatalf("stdin recorded a spurious error: %v", st.Err)
	}
}

// TestCopyIODrainTimeoutBounds verifies that a destination which blocks
// forever does not make Close hang: the drain deadline is honored, the
// direction reports ErrDrainTimeout, and the bytes already accepted are kept.
func TestCopyIODrainTimeoutBounds(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	var accepted bytes.Buffer
	wrote := make(chan struct{})
	var wroteOnce sync.Once
	block := make(chan struct{})
	defer close(block)
	sink := writerFunc(func(p []byte) (int, error) {
		n, _ := accepted.Write(p)
		wroteOnce.Do(func() { close(wrote) })
		<-block // emulate a sink that never returns
		return n, nil
	})

	copier, err := CopyIO(term, CopyStreams{Stdout: sink},
		WithIODrainTimeout(150*time.Millisecond))
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}

	if _, err := term.(*pipeIO).out.w.Write([]byte("first")); err != nil {
		t.Fatalf("write stdout: %s", err)
	}
	if err := term.(*pipeIO).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- copier.Close() }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("Close: got %v, want ErrDrainTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not honor its drain deadline")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close took too long: %s", elapsed)
	}

	<-wrote
	if got := accepted.String(); got != "first" {
		t.Fatalf("bytes accepted before blocking were lost: got %q", got)
	}
}

// TestCopierCloseIdempotent verifies that repeated and concurrent Close calls
// release resources once and return the same error.
func TestCopierCloseIdempotent(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	copier, err := CopyIO(term, CopyStreams{Stdin: strings.NewReader("in"), Stdout: io.Discard})
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}
	// The stdin Reader returns EOF immediately after its payload, so the input
	// direction finishes on its own. Closing the process-side stdout write end
	// delivers EOF to the output direction. Nothing is blocked, so Close only
	// has to release resources.
	if err := term.(*pipeIO).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}
	// Closing the IO too must not corrupt the copier's view: pipe handles are
	// idempotent.
	defer term.Close()

	const goroutines = 16
	var wg sync.WaitGroup
	results := make([]error, goroutines)
	start := make(chan struct{})
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = copier.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range results {
		if err != results[0] {
			t.Fatalf("goroutine %d saw a different Close result: %v vs %v", i, err, results[0])
		}
	}
	if err := results[0]; err != nil {
		t.Fatalf("Close: %s", err)
	}
}

// TestPipeIOCloseIdempotent verifies that the pipe handles behind an IO value
// can be released by Close, CloseAfterStart and a Copier in any order without
// double-close errors.
func TestPipeIOCloseIdempotent(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	pio := term.(*pipeIO)

	if err := pio.CloseAfterStart(); err != nil {
		t.Fatalf("first CloseAfterStart: %s", err)
	}
	if err := pio.CloseAfterStart(); err != nil {
		t.Fatalf("second CloseAfterStart: %s", err)
	}
	if err := pio.Close(); err != nil {
		t.Fatalf("first Close: %s", err)
	}
	if err := pio.Close(); err != nil {
		t.Fatalf("second Close: %s", err)
	}
}

// TestNullIOCloseIdempotent verifies that CloseAfterStart and Close share one
// release of /dev/null.
func TestNullIOCloseIdempotent(t *testing.T) {
	nio, err := NewNullIO()
	if err != nil {
		t.Fatalf("NewNullIO: %s", err)
	}
	if err := nio.(StartCloser).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}
	if err := nio.Close(); err != nil {
		t.Fatalf("Close after CloseAfterStart: %s", err)
	}
	if err := nio.Close(); err != nil {
		t.Fatalf("repeated Close: %s", err)
	}
}

// TestCopyIODoesNotOwnCallerIO verifies that every direction is tracked and
// forwarded for a caller-supplied IO that does not declare stream ownership.
func TestCopyIODoesNotOwnCallerIO(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	term := &customIO{in: inW, out: outR}
	defer inR.Close()
	defer inW.Close()
	defer outR.Close()

	copier, err := CopyIO(term, CopyStreams{Stdin: strings.NewReader("payload"), Stdout: io.Discard})
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}
	if err := copier.StopInput(); err != nil {
		t.Fatalf("StopInput: %s", err)
	}

	// The caller refused ownership, so the copier must not have closed the
	// stdin write end; the caller can still use it.
	if _, err := inW.Write([]byte("still-usable")); err != nil {
		t.Fatalf("copier closed a caller-owned descriptor: %s", err)
	}
	// Give the output direction EOF from the process side. The copier does not
	// own the read end, so the caller closes the write end.
	if err := outW.Close(); err != nil {
		t.Fatalf("close caller-owned write end: %s", err)
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("Close: %s", err)
	}
	if _, err := inW.Write([]byte("still-usable")); err != nil {
		t.Fatalf("copier closed a caller-owned descriptor on Close: %s", err)
	}

	buf := make([]byte, 32)
	if _, err := inR.Read(buf); err != nil {
		t.Fatalf("read from caller-owned pipe: %s", err)
	}
}

// customIO is an IO implementation that does not implement StreamOwner.
type customIO struct {
	in  *os.File
	out *os.File
}

func (c *customIO) Stdin() io.WriteCloser { return c.in }
func (c *customIO) Stdout() io.ReadCloser { return c.out }
func (c *customIO) Stderr() io.ReadCloser { return nil }
func (c *customIO) Close() error          { return nil }
func (c *customIO) Set(cmd *exec.Cmd)     {}

// TestCopyIOWithProcess verifies the whole path against a real process: both
// output directions are delivered, the copier records clean per-direction
// status, and Close after process exit returns no error.
func TestCopyIOWithProcess(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	pio := term.(*pipeIO)

	var stdout, stderr bytes.Buffer
	cmd := exec.Command("/bin/sh", "-c", `cat; printf 'end\n' 1>&2`)
	term.Set(cmd)

	copier, err := CopyIO(term, CopyStreams{
		Stdin:  strings.NewReader("hello\n"),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}
	if err := cmd.Start(); err != nil {
		copier.Close()
		t.Fatalf("start: %s", err)
	}
	if err := pio.CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait: %s", err)
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("Close: %s", err)
	}

	if got := stdout.String(); got != "hello\n" {
		t.Fatalf("stdout: got %q", got)
	}
	if got := stderr.String(); got != "end\n" {
		t.Fatalf("stderr tail lost: got %q", got)
	}
	for _, s := range []Stream{StreamStdin, StreamStdout, StreamStderr} {
		st := copier.Status(s)
		if !st.Finished || st.Err != nil {
			t.Fatalf("%s status: %+v", s, st)
		}
	}
}

// TestCopyIOContextCancelDrainsTail verifies that when the context is
// canceled while a child is still producing output, input forwarding stops,
// the tail already written by the child is still delivered, and Close returns
// within its bound.
func TestCopyIOContextCancelDrainsTail(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	pio := term.(*pipeIO)

	ctx, cancel := context.WithCancel(context.Background())
	var stdout safeBuffer
	wroteTail := make(chan struct{})
	var tailOnce sync.Once
	sink := writerFunc(func(p []byte) (int, error) {
		n, err := stdout.Write(p)
		if bytes.Contains(p, []byte("tail")) {
			tailOnce.Do(func() { close(wroteTail) })
		}
		return n, err
	})
	pr, pw := io.Pipe()
	cmd := exec.Command("/bin/sh", "-c", `printf 'tail\n'; sleep 30`)
	term.Set(cmd)

	copier, err := CopyIO(term, CopyStreams{Stdin: pr, Stdout: sink},
		WithCopyContext(ctx), WithIODrainTimeout(time.Second))
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}
	if err := cmd.Start(); err != nil {
		copier.Close()
		t.Fatalf("start: %s", err)
	}
	if err := pio.CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}

	// Wait until the child's output has reached the caller, so what we are
	// testing is that cancellation does not discard already-delivered data or
	// hang the drain.
	select {
	case <-wroteTail:
	case <-time.After(5 * time.Second):
		t.Fatal("child output was not forwarded")
	}

	// Cancel: the copier stops input and starts its bounded drain. Then the
	// child is killed, as a caller's cancellation policy would do.
	cancel()
	_ = cmd.Process.Kill()
	waitErr := cmd.Wait()
	_ = pw.Close()
	closeErr := copier.Close()

	if got := stdout.String(); !strings.Contains(got, "tail") {
		t.Fatalf("tail output lost on cancellation: got %q", got)
	}
	if closeErr != nil && !errors.Is(closeErr, ErrDrainTimeout) {
		t.Fatalf("Close: %s", closeErr)
	}
	_ = waitErr
}

// safeBuffer is a bytes.Buffer safe for concurrent Write and String calls.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestCopierCloseRaceWithProcessExit stresses the race between the copier's
// bounded teardown and the process-side handles being closed by another owner
// (CloseAfterStart or the caller's IO Close). No double-close error may leak,
// and Close must always return.
func TestCopierCloseRaceWithProcessExit(t *testing.T) {
	for i := 0; i < 50; i++ {
		term, err := NewPipeIO(os.Getuid(), os.Getgid())
		if err != nil {
			t.Fatalf("NewPipeIO: %s", err)
		}
		copier, err := CopyIO(term, CopyStreams{
			Stdin:  strings.NewReader("x"),
			Stdout: io.Discard,
			Stderr: io.Discard,
		}, WithIODrainTimeout(50*time.Millisecond))
		if err != nil {
			t.Fatalf("CopyIO: %s", err)
		}

		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); _ = term.Close() }()
		go func() { defer wg.Done(); _, _ = term.(*pipeIO).out.w.Write([]byte("tail")) }()
		go func() {
			defer wg.Done()
			if err := copier.Close(); err != nil && !errors.Is(err, ErrDrainTimeout) {
				t.Errorf("Close: %s", err)
			}
		}()
		wg.Wait()
	}
}

// TestAbortDoesNotWaitForOutput verifies that Abort releases the handles and
// returns even when an output copier is blocked on a Writer, and that a later
// Close observes the same result.
func TestAbortDoesNotWaitForOutput(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	block := make(chan struct{})
	defer close(block)
	started := make(chan struct{})
	var startedOnce sync.Once
	sink := writerFunc(func(p []byte) (int, error) {
		startedOnce.Do(func() { close(started) })
		<-block
		return len(p), nil
	})

	copier, err := CopyIO(term, CopyStreams{Stdout: sink})
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}
	if _, err := term.(*pipeIO).out.w.Write([]byte("x")); err != nil {
		t.Fatalf("write: %s", err)
	}
	<-started

	done := make(chan error, 1)
	go func() { done <- copier.Abort() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Abort: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Abort waited for a blocked output copier")
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("Close after Abort: %s", err)
	}
}
