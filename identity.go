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
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// ErrIdentityUnsupported is returned by identity based operations when the
// platform cannot provide a pidfd or another stable process identity.
var ErrIdentityUnsupported = errors.New("runc: stable process identity is not supported on this platform")

// ErrWatchClosed is returned by ProcessWatch operations invoked after the
// watch was closed before the monitored process exited.
var ErrWatchClosed = errors.New("runc: process watch is closed")

// Watch event types.
const (
	// WatchEventStats is an intermediate, operational event such as a
	// statistics sample. It may be merged with another intermediate event
	// when the event channel is full.
	WatchEventStats = "stats"
	// WatchEventExit is the single, terminal event carrying the final exit
	// status. It is never merged or dropped: if it cannot be delivered
	// because the channel is full and no reader is present, it stays
	// available through ProcessWatch.Wait.
	WatchEventExit = "exit"
)

// ProcessIdentity identifies a specific process generation independently of
// numeric pid reuse.
//
// A pid can be recycled by the kernel after a process exits. An identity
// therefore binds the container scope, a per-container generation counter,
// the process start time and, on Linux, a pidfd that keeps referring to the
// same process even after its pid gets reused.
type ProcessIdentity struct {
	// ContainerID is the container, or any other caller defined scope, the
	// process belongs to. Generation counters are scoped by this value.
	ContainerID string

	// Generation is a monotonically increasing counter per ContainerID.
	// Every Watch on the same container gets a new generation, including
	// when a restarted process reuses the numeric pid.
	Generation uint64

	// Pid is the numeric pid the process had when the identity was created.
	// It is informational only: never treat a pid match as identity match.
	Pid int

	// Pidfd refers to the process itself and, unlike a numeric pid, keeps
	// referring to that exact process after the pid has been recycled. It
	// may be nil when pidfd support is unavailable, in which case
	// StarttimeTicks is used.
	//
	// When obtained from ProcessWatch.Identity, the file is owned by the
	// watch and closed by ProcessWatch.Close; the caller must not close it.
	// When passed through WithIdentity, the caller retains ownership: the
	// monitor duplicates the file instead of taking it over.
	Pidfd *os.File

	// StarttimeTicks is the process start time in clock ticks since boot,
	// as reported by /proc/<pid>/stat (field 22). Two processes sharing a
	// pid have different start times, so it identifies a generation when
	// no pidfd is available. It is zero on platforms without procfs.
	StarttimeTicks uint64
}

// MatchesPid is implemented per platform: pidfd is authoritative on Linux,
// the generation registry is the boundary on other platforms.

// WatchStats accounts for the events observed by a watch. The counters make
// event coalescing explicit instead of silently dropping samples.
type WatchStats struct {
	// DeliveredEvents is the number of events handed to the event channel.
	DeliveredEvents uint64

	// MergedEvents is the number of intermediate events that were merged
	// into another intermediate event because the event channel was full.
	// Terminal exit events are never merged.
	MergedEvents uint64
}

// WatchEvent is an event delivered on a watch's event channel. Type is one
// of the WatchEvent* constants: Stats carries an intermediate sample, Exit
// carries the terminal exit status.
type WatchEvent struct {
	Type      string
	Timestamp time.Time

	// Stats is set on WatchEventStats events and carries the counters
	// observed at the time the event was emitted.
	Stats WatchStats

	// Exit is set on the single WatchEventExit event and carries the final
	// exit status, which always remains available through Wait as well.
	Exit *Exit
}

// WatchOption configures Watch.
type WatchOption func(*watchConfig)

type watchConfig struct {
	identity ProcessIdentity
	events   chan WatchEvent
}

// WithIdentity provides an externally obtained identity, typically the
// pidfd runc passed over the pidfd socket of a container init or exec'd
// process. The monitor duplicates the pidfd, so the caller keeps owning the
// provided file. Without this option, Watch opens a pidfd for the started
// command itself when the platform supports it.
func WithIdentity(identity ProcessIdentity) WatchOption {
	return func(c *watchConfig) {
		c.identity = identity
	}
}

// WithEvents provides the channel watch events are delivered on. Use an
// unbuffered or small channel deliberately: when it is full, intermediate
// events are merged with explicit counters while the terminal exit event is
// always retained. Without this option the watch uses a buffered channel of
// size one.
func WithEvents(ch chan WatchEvent) WatchOption {
	return func(c *watchConfig) {
		c.events = ch
	}
}

// IdentityMonitor monitors processes through stable ProcessIdentity values
// rather than numeric pids. It is safe for concurrent use.
type IdentityMonitor interface {
	// Watch starts cmd and monitors it as a new generation of containerID.
	// The returned watch is bound to a pidfd (when available), the process
	// start time and the container identity, so the exit of an old
	// generation can never be attributed to a process that recycled its
	// pid.
	//
	// cmd must not have been started yet. The watch owns the lifecycle of
	// its monitoring state and must be closed with Close.
	Watch(cmd *exec.Cmd, containerID string, opts ...WatchOption) (*ProcessWatch, error)

	// Active reports whether the process referred to by id is still
	// running. It never falls back to a bare numeric pid check when a
	// pidfd or start time is available.
	Active(id ProcessIdentity) (bool, error)

	// Generations returns the identities of the currently registered
	// generations of containerID, oldest first. The returned identities
	// do not carry the monitor-owned pidfd file.
	Generations(containerID string) []ProcessIdentity
}

// DefaultIdentityMonitor is the process identity based monitor.
var DefaultIdentityMonitor IdentityMonitor = newIdentityMonitor()

// NewIdentityMonitor returns a new IdentityMonitor with its own generation
// registry. Most callers should use DefaultIdentityMonitor.
func NewIdentityMonitor() IdentityMonitor {
	return newIdentityMonitor()
}

type identityMonitor struct {
	mu       sync.RWMutex
	counters map[string]uint64
	watches  map[string][]*processRecord
}

type processRecord struct {
	identity ProcessIdentity
	watch    *ProcessWatch
}

func newIdentityMonitor() *identityMonitor {
	return &identityMonitor{
		counters: map[string]uint64{},
		watches:  map[string][]*processRecord{},
	}
}

// Watch implements IdentityMonitor.
func (m *identityMonitor) Watch(cmd *exec.Cmd, containerID string, opts ...WatchOption) (*ProcessWatch, error) {
	if cmd == nil {
		return nil, errors.New("runc: Watch requires a non-nil exec.Cmd")
	}
	if cmd.Process != nil {
		return nil, errors.New("runc: Watch requires a command that has not been started")
	}
	cfg := watchConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	pidfd, targetPID, err := m.resolveIdentity(cmd, cfg.identity)
	if err != nil {
		killAndWait(cmd)
		return nil, err
	}
	starttime, err := processStarttime(targetPID)
	if err != nil && !errors.Is(err, ErrIdentityUnsupported) {
		killAndWait(cmd)
		if pidfd != nil {
			_ = pidfd.Close()
		}
		return nil, err
	}

	m.mu.Lock()
	m.counters[containerID]++
	generation := m.counters[containerID]
	identity := ProcessIdentity{
		ContainerID:    containerID,
		Generation:     generation,
		Pid:            targetPID,
		Pidfd:          pidfd,
		StarttimeTicks: starttime,
	}
	events := cfg.events
	if events == nil {
		events = make(chan WatchEvent, 1)
	}
	w := &ProcessWatch{
		mon:          m,
		cmd:          cmd,
		events:       events,
		identity:     identity,
		pidfd:        pidfd,
		stop:         make(chan struct{}),
		end:          make(chan struct{}),
		terminalDone: make(chan struct{}),
	}
	m.watches[containerID] = append(m.watches[containerID], &processRecord{
		identity: identity,
		watch:    w,
	})
	m.mu.Unlock()

	// The reaper is the single reader of the command exit. Its terminal
	// delivery is guarded by terminalOnce, so a concurrent Close can never
	// make it deliver or stop more than once.
	go w.reap()
	// The watchdog closes end exactly once when either the watch is closed
	// or the terminal state has been reached.
	go w.watchdog()
	return w, nil
}

func (m *identityMonitor) resolveIdentity(cmd *exec.Cmd, provided ProcessIdentity) (*os.File, int, error) {
	if provided.Pidfd != nil {
		dup, err := dupFile(provided.Pidfd)
		if err != nil {
			return nil, 0, fmt.Errorf("runc: failed to duplicate identity pidfd: %w", err)
		}
		pid := provided.Pid
		if pid == 0 {
			if target, err := pidfdPid(dup); err == nil {
				pid = target
			}
		}
		if pid == 0 {
			pid = cmd.Process.Pid
		}
		return dup, pid, nil
	}

	pidfd, err := openProcessPidfd(cmd.Process.Pid)
	if err != nil && !errors.Is(err, ErrIdentityUnsupported) {
		return nil, 0, err
	}
	return pidfd, cmd.Process.Pid, nil
}

// Active implements IdentityMonitor.
func (m *identityMonitor) Active(id ProcessIdentity) (bool, error) {
	if id.Pidfd != nil {
		return pidfdAlive(id.Pidfd)
	}
	if id.StarttimeTicks != 0 {
		starttime, err := processStarttime(id.Pid)
		if err != nil {
			// The pid no longer exists, so this generation is gone.
			return false, nil
		}
		return starttime == id.StarttimeTicks, nil
	}
	return false, ErrIdentityUnsupported
}

// Generations implements IdentityMonitor.
func (m *identityMonitor) Generations(containerID string) []ProcessIdentity {
	m.mu.RLock()
	defer m.mu.RUnlock()

	records := m.watches[containerID]
	out := make([]ProcessIdentity, 0, len(records))
	for _, record := range records {
		identity := record.identity
		// The monitor owns the pidfd file; never hand it out for closing.
		identity.Pidfd = nil
		out = append(out, identity)
	}
	return out
}

// unregister removes only the given generation of the container. An old
// generation exiting can never touch a newer generation registered under
// the same container.
func (m *identityMonitor) unregister(containerID string, generation uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	records := m.watches[containerID]
	for i, record := range records {
		if record.identity.Generation != generation {
			continue
		}
		m.watches[containerID] = append(records[:i], records[i+1:]...)
		if len(m.watches[containerID]) == 0 {
			delete(m.watches, containerID)
		}
		return
	}
}

// ProcessWatch is a single monitored process generation.
type ProcessWatch struct {
	mon    *identityMonitor
	cmd    *exec.Cmd
	events chan WatchEvent
	pidfd  *os.File

	identity ProcessIdentity

	stop      chan struct{}
	stopOnce  sync.Once
	end       chan struct{}
	endOnce   sync.Once
	closeOnce sync.Once

	terminalDone chan struct{}
	terminalOnce sync.Once

	mu       sync.Mutex
	stats    WatchStats
	pending  *WatchEvent
	terminal *Exit
}

// Identity returns the stable identity bound to this watch. The returned
// pidfd, when present, remains owned by the watch.
func (w *ProcessWatch) Identity() ProcessIdentity {
	return w.identity
}

// Done is closed when the watch has ended, either because the process
// exited or because Close was called.
func (w *ProcessWatch) Done() <-chan struct{} {
	return w.end
}

// Stats returns a snapshot of the event counters.
func (w *ProcessWatch) Stats() WatchStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// Emit enqueues an intermediate operational event. When the event channel
// is full, the event is merged into the pending event and the merge is
// accounted in Stats; Emit therefore never blocks. It returns
// ErrWatchClosed once the watch has ended.
func (w *ProcessWatch) Emit(event WatchEvent) error {
	if event.Type == "" {
		event.Type = WatchEventStats
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.stop:
		return ErrWatchClosed
	case <-w.terminalDone:
		return ErrWatchClosed
	default:
	}

	select {
	case w.events <- event:
		w.stats.DeliveredEvents++
		return nil
	default:
	}

	// The channel is full: coalesce into the pending intermediate event
	// and make the merge explicit in the counters. Every emit is
	// accounted for exactly once: the first occupies the pending slot,
	// every further full-channel emit increments MergedEvents.
	if w.pending != nil {
		w.stats.MergedEvents++
	}
	event.Stats = w.stats
	w.pending = &event

	// Opportunistic retry: if space just became available, flush the
	// coalesced event immediately instead of waiting for the next Emit.
	select {
	case w.events <- *w.pending:
		w.pending = nil
		w.stats.DeliveredEvents++
	default:
	}
	return nil
}

// Wait blocks until the monitored process exits or the watch is closed and
// returns the final exit status. The status is retained even when the
// terminal event could not be delivered through a full event channel.
func (w *ProcessWatch) Wait() (Exit, error) {
	select {
	case <-w.terminalDone:
	case <-w.stop:
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal != nil {
		return *w.terminal, nil
	}
	return Exit{}, ErrWatchClosed
}

// Close stops the watch. It is idempotent: closing concurrently with a
// process exit is safe and every internal goroutine transition happens
// exactly once. The monitor-owned pidfd is closed here. Closing does not
// kill the monitored process.
func (w *ProcessWatch) Close() error {
	w.stopOnce.Do(func() {
		close(w.stop)
	})
	<-w.end
	w.closeOnce.Do(func() {
		if w.pidfd != nil {
			_ = w.pidfd.Close()
		}
	})
	return nil
}

func (w *ProcessWatch) reap() {
	status := 0
	if err := w.cmd.Wait(); err != nil {
		status = 255
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if waitStatus, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				status = waitStatus.ExitStatus()
			}
		}
	}

	w.deliverTerminal(Exit{
		Timestamp:   time.Now(),
		Pid:         w.identity.Pid,
		Status:      status,
		ContainerID: w.identity.ContainerID,
		Generation:  w.identity.Generation,
	})
}

func (w *ProcessWatch) watchdog() {
	select {
	case <-w.stop:
	case <-w.terminalDone:
	}
	w.endOnce.Do(func() {
		close(w.end)
	})
}

// deliverTerminal is the only place terminal state is published. Guarded by
// terminalOnce so the exit reader stops exactly once even when Close races
// with the process exit.
func (w *ProcessWatch) deliverTerminal(exit Exit) {
	w.terminalOnce.Do(func() {
		// Unregister before terminal state is published: anyone returning
		// from Wait (which only waits for terminalDone) must already
		// observe the generation gone from the registry. The registry lock
		// is separate from the watch lock, so taking it here cannot
		// deadlock.
		w.mon.unregister(w.identity.ContainerID, w.identity.Generation)

		w.mu.Lock()
		exit.MergedEvents = w.stats.MergedEvents
		w.terminal = &exit
		pending := w.pending
		w.pending = nil
		// Closed under the same lock Emit takes, so once terminal state is
		// published no intermediate event can race past it.
		close(w.terminalDone)
		w.mu.Unlock()

		// Preserve ordering: flush the last coalesced intermediate event
		// before the terminal event.
		if pending != nil {
			w.deliver(*pending)
		}
		w.deliver(WatchEvent{
			Type:      WatchEventExit,
			Timestamp: exit.Timestamp,
			Exit:      w.terminal,
		})
	})
}

// deliver sends an event, blocking while the channel is full until space
// becomes available or the watch is closed.
func (w *ProcessWatch) deliver(event WatchEvent) {
	select {
	case w.events <- event:
		w.mu.Lock()
		w.stats.DeliveredEvents++
		w.mu.Unlock()
	case <-w.stop:
	}
}

func killAndWait(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}
