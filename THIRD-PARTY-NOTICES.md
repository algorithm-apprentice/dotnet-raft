# Third-Party Notices

## etcd-io/raft

This project is a C# reimplementation derived from the architecture,
protocol model, behavior, and tests of
[`etcd-io/raft`](https://github.com/etcd-io/raft) at commit
`1c0011d2c6b7a0230f87bad38ad4c6e70d810f9e`.

Copyright 2015-2026 The etcd Authors.

The upstream work is licensed under the Apache License, Version 2.0. A copy of
that license is included in this repository as `LICENSE`.

The derived work changes the implementation language to C#, reorganizes the
code around an explicit implementation dependency DAG, and adapts APIs and
tests to .NET conventions.
