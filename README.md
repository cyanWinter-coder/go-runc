# go-runc

[![Go Reference](https://pkg.go.dev/badge/github.com/containerd/go-runc.svg)](https://pkg.go.dev/github.com/containerd/go-runc)
[![Build Status](https://github.com/containerd/go-runc/workflows/CI/badge.svg)](https://github.com/containerd/go-runc/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/containerd/go-runc/tree/main/graph/badge.svg)](https://app.codecov.io/gh/containerd/go-runc/tree/main)

This is a module for consuming the [runc](https://github.com/opencontainers/runc) binary in your Go applications.
It tries to expose all the settings and features of the runc CLI.

## Process identity based monitoring

The legacy `Monitor` only knows the numeric pid returned by `exec.Cmd`, and a
container that restarts quickly can recycle that pid. For daemons that need an
unambiguous exit boundary, the package also provides `IdentityMonitor`:

```go
mon := runc.DefaultIdentityMonitor

watch, err := mon.Watch(cmd, containerID, runc.WithIdentity(runc.ProcessIdentity{
    ContainerID: containerID,
    Pidfd:       pidfd, // from Socket.ReceivePidfd(); optional, duplicated internally
}))
if err != nil { /* ... */ }
defer watch.Close()

exit, err := watch.Wait() // the final status is retained even with a full event channel
```

Each `Watch` binds a `ProcessIdentity` made of the container id, a
per-container generation counter, the process start time and a pidfd (Linux).
An old generation exiting only ends its own watch: a new process that reused
the same numeric pid is never mistaken for the original target. Intermediate
events sent through `ProcessWatch.Emit` are coalesced with explicit
`WatchStats` counters when the channel is full, while the terminal exit event
is never merged or dropped. The existing `ProcessMonitor`, `Exit` and pidfd
socket APIs remain unchanged; the new `Exit` fields are additive.

## Project details

**go-runc** is a containerd sub-project, licensed under the [Apache 2.0 license](./LICENSE).
As a containerd sub-project, you will find the:

 * [Project governance](https://github.com/containerd/project/blob/main/GOVERNANCE.md),
 * [Maintainers](https://github.com/containerd/project/blob/main/MAINTAINERS),
 * and [Contributing guidelines](https://github.com/containerd/project/blob/main/CONTRIBUTING.md)

information in our [`containerd/project`](https://github.com/containerd/project) repository.
