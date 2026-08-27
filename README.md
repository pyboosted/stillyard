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

`0.1.0-alpha.2` contains the first two bounded Windows vertical slices:

- a runtime-neutral blocking Rust client with deadlines and cancellation;
- a framed, owner-only local named-pipe protocol and detached singleton daemon;
- a crash-safe SQLite lifecycle store for Submission → Job → Attempt → Invocation → Containment → Lease;
- immediate idempotent submit receipts, status, event-driven wait, recovery, daemon status, and offset log reads;
- born-contained Windows process creation with a kill-on-close Job Object, clean environment, canonical stdout/stderr, timeout cleanup, executable provenance, and restart interruption;
- one `stillyard` CLI binary plus the public generated schema;
- atomic Batch submission with acyclic success/failure/terminal dependencies;
- one non-preemptive Lease scheduler for CPU, RAM, cargo/GPU slots, custom scalars, and stable shared/exclusive path fences;
- immediate blockers, deterministic queue rank, and honest estimated/lower-bound/unknown start estimates.

This slice deliberately accepts EOF-stdin, single-attempt jobs without Conditions, quiet policies, impacts, profiles, secrets, or artifacts. Cancel/drain, retention/events, and `watch` are still baseline work. Specs declaring an unimplemented policy reject rather than run unenforced. Linux remains the v0.2 target.

The tests cover the increment-2a portions of acceptance rows A-03, A-04, and A-06. Those rows are not claimed complete until retries, observed RAM/VRAM freshness, and external Conditions arrive in the following slice.

## Resource configuration

The daemon reads `config.json` from the fixed store directory reported by `stillyard daemon-status`. Missing capacities are zero, so a constrained job waits with a visible `resource_capacity` blocker instead of running unenforced. Configuration is reloaded on daemon restart.

```json
{
  "cpu_units": 16,
  "ram_mb": 32768,
  "cargo_slots": 1,
  "gpu_slots": 1,
  "custom": {
    "review_slots": 4,
    "vram_mb:gpu-uuid": 16384
  }
}
```

The committed schemas are available under `schema/`; `stillyard schema spec` and `stillyard schema config` print the exact same documents.

## Build

```text
cargo build
cargo test
cargo run -- schema spec
cargo run -- schema config
cargo run -- daemon-status
cargo run -- submit --spec job.json --wait
cargo run -- submit --batch batch.json --wait
cargo run -- logs JOB_ID
```

Stillyard is licensed under either of Apache License 2.0 or the MIT license, at your option.
