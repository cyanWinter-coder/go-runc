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
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// writeRuntimeScript installs an executable shell script that stands in for
// runc, ignoring the runc arguments and running body instead.
func writeRuntimeScript(t *testing.T, body string) string {
	t.Helper()
	fh, err := os.CreateTemp(t.TempDir(), "fake-runc-*.sh")
	if err != nil {
		t.Fatalf("create temp script: %s", err)
	}
	if _, err := fh.WriteString("#!/bin/sh\n" + body); err != nil {
		fh.Close()
		t.Fatalf("write temp script: %s", err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("close temp script: %s", err)
	}
	if err := os.Chmod(fh.Name(), 0o755); err != nil {
		t.Fatalf("chmod temp script: %s", err)
	}
	return fh.Name()
}

// TestRunCopyStreamsDeliversTail exercises the CopyStreams option through the
// public Run entrypoint with a real process. Both directions, including the
// output produced immediately before exit, must reach the caller.
func TestRunCopyStreamsDeliversTail(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	t.Cleanup(func() { term.Close() })

	var stdout, stderr safeBuffer
	r := &Runc{Command: writeRuntimeScript(t, `cat; printf 'out-tail\n'; printf 'err-tail\n' 1>&2`)}
	status, err := r.Run(context.Background(), "id", "bundle", &CreateOpts{
		IO: term,
		CopyStreams: &CopyStreams{
			Stdin:  strings.NewReader("echoed\n"),
			Stdout: &stdout,
			Stderr: &stderr,
		},
	})
	if err != nil {
		t.Fatalf("Run: %s", err)
	}
	if status != 0 {
		t.Fatalf("status: got %d", status)
	}
	if got := stdout.String(); got != "echoed\nout-tail\n" {
		t.Fatalf("stdout: got %q", got)
	}
	if got := stderr.String(); got != "err-tail\n" {
		t.Fatalf("stderr tail lost: got %q", got)
	}
	if err := term.Close(); err != nil {
		t.Fatalf("Close IO after Run: %s", err)
	}
}

// TestExecCopyStreamsRelays verifies the Exec path relays both directions and
// returns the process exit status alongside a drained copier.
func TestExecCopyStreamsRelays(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	var stdout, stderr safeBuffer
	r := &Runc{Command: writeRuntimeScript(t, `cat; printf 'err-tail\n' 1>&2; exit 7`)}
	err = r.Exec(context.Background(), "id", specsProcessForTest(), &ExecOpts{
		IO: term,
		CopyStreams: &CopyStreams{
			Stdin:  strings.NewReader("in\n"),
			Stdout: &stdout,
			Stderr: &stderr,
		},
	})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Status != 7 {
		t.Fatalf("expected exit status 7, got %v", err)
	}
	if got := stdout.String(); got != "in\n" {
		t.Fatalf("stdout: got %q", got)
	}
	if got := stderr.String(); got != "err-tail\n" {
		t.Fatalf("stderr tail lost: got %q", got)
	}
}

// TestCreateCopyStreamsNilIO rejects a CopyStreams request without an IO rather
// than panicking.
func TestCreateCopyStreamsNilIO(t *testing.T) {
	r := &Runc{Command: "/bin/true"}
	err := r.Create(context.Background(), "id", "bundle", &CreateOpts{
		CopyStreams: &CopyStreams{Stdout: io.Discard},
	})
	if err == nil {
		t.Fatal("expected an error for CopyStreams without IO")
	}
}

// TestCopyContextCancelWakesBlockedInput verifies that canceling a copier
// whose input Reader blocks forever still lets Close return, because Close
// never waits on the input direction.
func TestCopyContextCancelWakesBlockedInput(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	block := make(chan struct{})
	defer close(block)
	pr := readerFunc(func(p []byte) (int, error) {
		<-block
		return 0, io.EOF
	})

	ctx, cancel := context.WithCancel(context.Background())
	copier, err := CopyIO(term, CopyStreams{Stdin: pr, Stdout: io.Discard},
		WithCopyContext(ctx), WithIODrainTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}
	if err := term.(*pipeIO).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}

	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = copier.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked on a blocked input Reader")
	}
	if st := copier.Status(StreamStdin); st.Err != nil {
		t.Fatalf("canceled input recorded an error: %v", st.Err)
	}
}

// specsProcessForTest returns a minimal OCI process spec for Exec.
func specsProcessForTest() specs.Process { return specs.Process{} }

// TestCopyStreamsFailedStartCleansUp verifies that a command which never
// starts aborts the copier instead of draining it, so the call returns at once
// with the start error and the process-side handles are released.
func TestCopyStreamsFailedStartCleansUp(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	// Keep a reference to the read end: it must be released by the aborted
	// copier, so reading it returns "closed" rather than blocking.
	pio := term.(*pipeIO)

	r := &Runc{Command: filepath.Join(t.TempDir(), "does-not-exist")}
	start := time.Now()
	_, err = r.Run(context.Background(), "id", "bundle", &CreateOpts{
		IO: term,
		CopyStreams: &CopyStreams{
			Stdin:  strings.NewReader("never read"),
			Stdout: io.Discard,
			Stderr: io.Discard,
		},
		CopyOpts: []CopyOpt{WithIODrainTimeout(time.Hour)},
	})
	if err == nil {
		t.Fatal("expected a start error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("failed start drained for %s instead of aborting", elapsed)
	}
	buf := make([]byte, 1)
	if _, rerr := pio.out.r.Read(buf); rerr == nil {
		t.Fatal("output handle was not released after a failed start")
	}
}

// TestCopyIOStatusesAndErr verifies the aggregate accessors report each
// active direction and the errors recorded for them.
func TestCopyIOStatusesAndErr(t *testing.T) {
	term, err := NewPipeIO(os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("NewPipeIO: %s", err)
	}
	defer term.Close()

	bad := errors.New("sink down")
	copier, err := CopyIO(term, CopyStreams{
		Stdout: writerFunc(func([]byte) (int, error) { return 0, bad }),
	})
	if err != nil {
		t.Fatalf("CopyIO: %s", err)
	}
	if _, err := term.(*pipeIO).out.w.Write([]byte("x")); err != nil {
		t.Fatalf("write: %s", err)
	}
	if err := term.(*pipeIO).CloseAfterStart(); err != nil {
		t.Fatalf("CloseAfterStart: %s", err)
	}
	copier.Wait()
	if err := copier.Close(); !errors.Is(err, bad) {
		t.Fatalf("Close: got %v, want %v", err, bad)
	}

	statuses := copier.Statuses()
	if len(statuses) != int(numStreams) {
		t.Fatalf("Statuses: got %d entries", len(statuses))
	}
	byStream := map[Stream]StreamStatus{}
	for _, st := range statuses {
		byStream[st.Stream] = st
	}
	if !byStream[StreamStdout].Active || byStream[StreamStdin].Active {
		t.Fatalf("unexpected active set: %+v", statuses)
	}
	if err := copier.Err(); !errors.Is(err, bad) {
		t.Fatalf("Err: got %v, want %v", err, bad)
	}
	if got := StreamStderr.String(); got != "stderr" {
		t.Fatalf("Stream.Stderr.String: got %q", got)
	}
}
