# Stillyard

*A quiet, local-first scheduler for real processes.*

Stillyard is a lightweight per-user process scheduler for developer workstations and execution hosts. It admits ordinary non-interactive programs from declared CPU, memory, GPU, quiet-window, and compatibility needs; persists lifecycle state and canonical output; and exposes the same contract through a public Rust crate, CLI, and terminal viewer.

The implementation is in active development against the frozen [v0.12 requirements](docs/requirements.md). Product v0.1 targets Windows 10 1809+/Windows Server 2019+. Linux x86_64 follows in v0.2.

## Design boundaries

- host-local scheduling; SSH remains an external transport;
- one per-user daemon and one SQLite lifecycle store;
- explicit pinned daemon instances may use an isolated store and owner-only endpoint for tests;
- ordinary processes rather than containers or a proprietary worker runtime;
- resource admission and honest observation, not hard CPU/GPU partitioning;
- one public `stillyard` crate used by both CLI and TUI;
- no network listener, distributed placement, or hidden remote control plane.

The motivating agent-orchestration consumer and its boundary are documented in [the consumer case](docs/consumer-case-review-fleet.md).

## Current status

`0.1.0-alpha.8` contains the delivered Windows slices through operational diagnostics and
containment recovery:

- a runtime-neutral blocking Rust client with deadlines and cancellation;
- a framed, owner-only local named-pipe protocol and detached singleton daemon;
- a crash-safe SQLite lifecycle store for Submission → Job → Attempt → Invocation → Containment → Lease;
- immediate idempotent submit receipts, status, event-driven wait, recovery, daemon status, and offset log reads;
- born-contained Windows process creation with a kill-on-close Job Object, clean environment, canonical stdout/stderr, timeout cleanup, executable provenance, and restart interruption;
- one `stillyard` CLI binary plus the public generated schema;
- atomic Batch submission with acyclic success/failure/terminal dependencies;
- one non-preemptive Lease scheduler for CPU, RAM, cargo/GPU slots, custom scalars, and stable shared/exclusive path fences;
- immediate blockers, deterministic queue rank, and honest estimated-or-unknown start estimates.
- immutable staged file stdin for Jobs and atomic Batches, uploaded before `received` in bounded chunks;
- a small clean Windows environment with explicit per-Job set/unset operations and an exact PATH;
- fresh atomic result files, recovery by retained operation identity, and canonical-log passthrough.
- authenticated managed child submission from an explicitly enabled primary Invocation;
- server-derived parent Job/Attempt/Invocation identity from named-pipe PID membership in the
  Invocation's Windows Job Object;
- exact same-Attempt `not_received` recovery and result-file-authorized restage/resubmit, with
  `unknown`, foreign caller/store/endpoint, changed key/hash, and terminal parent all failing closed.
- atomic managed submit-and-wait admission for one Job or Batch, authenticated again by OS
  containment on every wait request;
- conservative deadlock rejection across the complete unfinished predecessor closure when the
  waiter or any authenticated ancestor retains an incompatible scalar or path-fence Lease;
- explicit rejection of managed waits whose closure exceeds total configured scalar capacity,
  plus durable machine-readable rejection reasons across replay, recovery, and result files;
- detached managed children remain ordinary durable Jobs, and disconnecting a safe wait never
  cancels its child or creates a durable wait edge.
- executable postconditions run in their own born-contained Invocations after primary cleanup
  while the Attempt Lease remains held; accepted/retryable/failed exit classifications drive
  bounded Job-owned retry with a fresh Attempt after finite backoff;
- public snapshots expose every ordered Attempt, primary/postcondition Invocation, root exit,
  bounded diagnostic tail, executable hash, daemon generation, and Containment incident;
- plain cancel is available through the public crate and CLI for explicit Job IDs, terminates a
  running Containment, suppresses retry, and never selects children or dependency successors;
- host-configured impact incompatibilities are enforced symmetrically by ordinary admission and
  managed-wait deadlock checks;
- receipts preserve the accepting daemon generation, while daemon status exposes its current
  generation, capacities, and a canonical configuration fingerprint.
- one durable bounded invalidation stream with store-scoped cursors, explicit `Gap`
  resynchronization, exact Job/Batch/label selectors, and stable paged Job summaries;
- owned blocking observation, settlement, and canonical-log streams in the public crate, with
  deadlines, cancellation, bounded membership, and no async-runtime requirement;
- read-only `list`, `events`, and `logs --follow` CLI surfaces plus an event-driven disposable
  `watch` TUI built only on the public crate.
- a bounded public `DoctorSnapshot` reports exact daemon, host/boot, store, loaded configuration,
  containment capability, and unresolved-incident evidence without exposing configuration values
  or child output;
- every resumed Windows root and daemon generation carries exact host + boot + PID + creation-time
  identity, while missing capability blocks before Lease grant;
- uncertain containments reconcile in bounded 32-item turns without idle polling or unchanged
  SQLite writes, and every clearance uses one Attempt-wide durable Lease predicate;
- `doctor clear-containment ID --force` is audited, idempotent risk acceptance: it rejects managed
  callers and live/uninspectable roots or boundaries, never kills work, and remains visible through
  ordinary status snapshots.

This slice deliberately accepts EOF or staged-file stdin, impacts, bounded retries, and executable postconditions without Conditions, quiet policies, secrets, or artifacts. The Windows clean base is limited to `SystemRoot`, `WINDIR`, `TEMP`, and `TMP`; PATH and every application/account variable must be explicit in the Job. Priority, aging, finite-held reservations, and a host-wide default concurrency cap are also later scheduler work; callers should declare a scalar, fence, or configured impact for every bounded kind of work rather than submit an unbounded fleet of zero-claim jobs. General wait graphs, cascade cancellation, drain/force, and configurable Job/log/input retention are still baseline work. Specs declaring an unimplemented policy reject rather than run unenforced. Linux remains the v0.2 target.

The tests cover the increment-2a portions of acceptance rows A-03, A-04, and A-06; the increment-2b staged-input, explicit-environment, result-file, and process-handle controls; the bounded managed-submission recovery and safe-wait slices of A-11/A-18; and the alpha.6 consumer lifecycle slice of A-04, A-08, A-11, A-14, and A-18. The full baseline rows are not claimed complete until cascade cancellation, drain/force, observed RAM/VRAM freshness, and external Conditions arrive.

The current delivered increment is
[alpha.8 operational diagnostics and containment recovery](docs/phase-8-operational-diagnostics.md).
Its public doctor snapshot, durable reconciliation evidence, audited force-clear transaction, and
greenfield host-bound schema build on [alpha.7 live observation](docs/phase-7-live-observation.md).
The external `moot` config-drift adapter remains a cross-repository acceptance item and is not
silently claimed here.

The isolated-instance foundation for that implementation is documented in
[Phase 8a](docs/phase-8a-isolated-daemon-instances.md). It lets Stillyard and external consumers run
a pinned foreground daemon on a temporary store and unique pipe without contacting the default
per-user daemon. The CLI's global `--endpoint` and the public `ClientBuilder` select that instance;
custom endpoints remain connect-only.

An uncertain Containment deliberately retains its real Lease until automatic proof or an explicit,
audited `doctor clear-containment ID --force` succeeds. Moving or editing individual store files is
not a supported recovery path.

Stillyard is still greenfield. Before the first stable release it has one current SQLite schema epoch and no database migrations: when that epoch or the required schema does not match, daemon startup silently replaces the database and creates a new store identity. The reset is deliberately all-or-nothing and does not delete `config.json` or canonical log files. Old job IDs, cursors, result files, and idempotency history are not recoverable across it.

## Resource configuration

The daemon reads `config.json` from the fixed store directory reported by `stillyard daemon-status`. Missing capacities are zero, so a constrained job waits with a visible `resource_capacity` blocker instead of running unenforced. Configuration is reloaded on daemon restart.

```json
{
  "resources": {
    "cpu_units": 16,
    "ram_mb": 32768,
    "cargo_slots": 1,
    "gpu_slots": 1,
    "custom": {
      "review_slots": 4,
      "vram_mb:gpu-uuid": 16384
    }
  },
  "impact_incompatibilities": {
    "measurement": ["cpu_heavy", "gpu_heavy"]
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
cargo run -- doctor --json
cargo run -- doctor clear-containment CONTAINMENT_ID --force --json
cargo run -- --endpoint \\.\pipe\stillyard-test daemon --store C:\temp\stillyard-test
cargo run -- --endpoint \\.\pipe\stillyard-test daemon-status
cargo run -- submit --spec job.json --wait --passthrough --silent --result-file operation.json
cargo run -- recover --result-file operation.json --wait --passthrough --silent
cargo run -- wait JOB_ID --passthrough
cargo run -- submit --batch batch.json --wait
cargo run -- logs JOB_ID
cargo run -- list --json
cargo run -- events --json
cargo run -- watch
```

Stillyard is licensed under either of Apache License 2.0 or the MIT license, at your option.
