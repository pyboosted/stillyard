# Stillyard

*A quiet, local-first scheduler for real processes.*

Stillyard is a lightweight per-user process scheduler for developer workstations and execution hosts. It admits ordinary non-interactive programs from declared CPU, memory, GPU, quiet-window, and compatibility needs; persists lifecycle state and canonical output; and exposes the same contract through a public Rust crate, CLI, and terminal viewer.

The implementation is in active development against the frozen [v0.12 requirements](docs/requirements.md). Product v0.1 targets Windows 10 1809+/Windows Server 2019+. Linux x86_64 follows in v0.2.

## Design boundaries

- host-local scheduling; SSH remains an external transport;
- one per-user daemon and one SQLite lifecycle store;
- ordinary processes rather than containers or a proprietary worker runtime;
- resource admission and honest observation, not hard CPU/GPU partitioning;
- one public `stillyard` crate used by both CLI and TUI;
- no network listener, distributed placement, or hidden remote control plane.

The motivating agent-orchestration consumer and its boundary are documented in [the consumer case](docs/consumer-case-review-fleet.md).

## Current status

The public types and generated JobSpec/BatchSpec schema are the first implementation slice. Daemon, local IPC, SQLite lifecycle, process containment, logs, and the CLI are being implemented in that order, with the acceptance contract in §16 of the baseline as the gate.

## Build

```text
cargo build
cargo test
cargo run -- schema spec
```

Stillyard is licensed under either of Apache License 2.0 or the MIT license, at your option.

