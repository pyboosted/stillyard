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

`0.1.0-alpha.1` contains the first Windows vertical slice:

- a runtime-neutral blocking Rust client with deadlines and cancellation;
- a framed, owner-only local named-pipe protocol and detached singleton daemon;
- a crash-safe SQLite lifecycle store for Submission → Job → Attempt → Invocation → Containment → Lease;
- immediate idempotent submit receipts, status, event-driven wait, recovery, daemon status, and offset log reads;
- born-contained Windows process creation with a kill-on-close Job Object, clean environment, canonical stdout/stderr, timeout cleanup, executable provenance, and restart interruption;
- one `stillyard` CLI binary plus the public generated schema.

This slice deliberately accepts only unconstrained, single-attempt jobs with EOF stdin. Resource admission, Conditions/dependencies/Batch, cancel/drain, profiles/secrets/artifacts, retention/events, and `watch` are still baseline work. Specs declaring an unimplemented policy reject rather than run unenforced. Linux remains the v0.2 target.

## Build

```text
cargo build
cargo test
cargo run -- schema spec
cargo run -- daemon-status
cargo run -- submit --spec job.json --wait
cargo run -- logs JOB_ID
```

Stillyard is licensed under either of Apache License 2.0 or the MIT license, at your option.
