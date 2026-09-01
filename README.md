# Stillyard

*A local scheduler for expensive Windows processes.*

Stillyard coordinates programs that share one workstation: builds, tests, GPU work, benchmarks,
and automation launched by tools or coding agents. Callers submit ordinary executables with their
resource and quiet-host requirements; one per-user daemon decides when they may start, keeps their
state and logs, and exposes visible reasons while they wait.

Stillyard is useful when independent processes need one scheduling authority without moving into
containers, a CI service, or a remote worker platform.

Current release: **0.1.0-alpha.14 for Windows 10 1809+ and Windows Server 2019+**.

## What works today

- **Durable execution.** Atomically ensure one Job or Batch from a stable key, receive a typed
  accepted/pending/final/conflict decision, wait without exit-code ambiguity, and read canonical
  stdout and stderr.
- **Resource scheduling.** Declare CPU, RAM, Cargo slots, GPU slots, custom scalar resources,
  shared or exclusive path fences, and host-configured incompatible impacts.
- **Fair priority scheduling.** Each Job has immutable priority `-3..=3` (neutral `0`). A
  deterministic one-minute aging quantum prevents starvation; finite durable scalar reservations
  protect otherwise-admissible work without preemption or global head-of-line blocking.
- **Observed admission.** RAM uses fresh physical and commit headroom. NVIDIA VRAM and GPU load
  use fresh NVML evidence tied to the configured GPU UUID. Missing, stale, or changed evidence
  blocks work instead of being treated as safe.
- **Quiet-host admission.** A Job can require a stable window for CPU, GPU, disk, foreign GPU
  compute, or configured process rules. Stillyard rechecks quiet immediately before releasing a
  born-suspended child.
- **Contained processes.** Windows Job Objects contain the full process tree. Timeouts and cancel
  clean it up; uncertain containment retains its resource Lease until safety is proven or an
  operator explicitly accepts the risk.
- **Job lifecycle.** Success/failure dependencies, immutable file stdin, explicit environments,
  bounded retries, executable postconditions with immutable primary results, managed child
  submission, and safe managed waits are supported.
- **External readiness.** An AND-set of path levels, acceptance-anchored path transitions,
  not-before times, and contained executable probes can gate launch. Evidence is durably
  attributed and authoritatively rescanned immediately before the born-suspended primary is
  released; deadlines and finite anti-livelock deferrals survive restart.
- **Managed capability policy.** A primary may authorize bounded child claims, impacts, fences,
  observed/quiet admission, mandatory labels, and narrower delegation without reserving those
  capabilities in the parent's Lease. Denials are durable Submission decisions.
- **Operator interfaces.** The CLI provides ensure, submit, recover, status, list, events, logs, wait,
  cancel, daemon status, schema, doctor, and bounded tree commands. `stillyard watch` is an
  event-driven parent/child forest. The Rust crate exposes the same public protocol through a
  blocking client.
- **Invocation events.** `InvocationChanged` identifies its Attempt and Invocation and records the
  provider-reported `started` or `exited` transition without requiring an immediate full snapshot.

Resource declarations are admission reservations, not hard CPU, RAM, or GPU limits on a running
process. Callers should declare every scarce resource or conflicting impact they rely on.

## Quick start

The examples below assume `stillyard.exe` is installed or available on `PATH`. A request to the
default endpoint starts the per-user daemon when necessary.

Create `hello.json`:

```json
{
  "spec_version": 4,
  "priority": 0,
  "executable": "C:\\Windows\\System32\\cmd.exe",
  "args": ["/d", "/c", "echo hello from Stillyard"],
  "working_directory": "C:\\"
}
```

Submit it and stream its retained output:

```powershell
stillyard submit --spec .\hello.json --wait --passthrough
```

For automation, use one operation that owns submit/recovery and reports typed exit provenance:

```powershell
stillyard --endpoint \\.\pipe\stillyard ensure --spec .\hello.json --idempotency-key 018f70d5-9b42-7f4e-8c38-4f86ca7bf5b1 --wait
```

Inspect the scheduler:

```powershell
stillyard daemon-status
stillyard doctor
stillyard list
stillyard list --tree
stillyard tree JOB_ID
stillyard watch
```

`daemon-status` includes authoritative `{ capacity, granted, reserved }` accounting for every
built-in and configured custom scalar. Granted totals include retained uncertain-containment
Leases; reserved totals are the exact sum of active durable full-vector scalar reservations.

JobSpec and host-config documents are strict and versioned. Print the authoritative schemas with:

```powershell
stillyard schema spec
stillyard schema config
stillyard schema managed-execution
```

The same schemas are checked in under [`schema/`](schema/).

## Host policy

The daemon reads `config.json` from the store directory reported by `stillyard daemon-status`.
Capacities default to zero, so a Job that requests an unconfigured resource waits with a visible
`resource_capacity` blocker. Restart the daemon after changing host policy.

A small CPU, RAM, Cargo, and impact configuration looks like this:

```json
{
  "resources": {
    "cpu_units": 16,
    "ram_mb": 32768,
    "cargo_slots": 1
  },
  "impact_incompatibilities": {
    "measurement": ["cpu_heavy"]
  },
  "observation": {
    "ram_safety_margin_mb": 2048,
    "process_rules": {
      "block": ["cargo.exe", "rustc.exe", "obs*"]
    }
  }
}
```

GPU slots or `vram_mb:<gpu-uuid>` capacity additionally require a configured `gpu_slot_uuid` and
a positive VRAM safety margin. `stillyard doctor` reports provider coverage, freshness, placement,
configuration identity, and unresolved containment incidents.

## Boundaries

Stillyard currently supports one host-local, per-user Windows daemon. It has no network listener,
distributed placement, container runtime, Linux runner, secrets, artifacts,
cascade cancellation, or drain mode. Explicit isolated daemon instances exist for tests and
special-purpose tools; custom endpoints are connect-only.

This is an alpha with one current SQLite schema epoch and no database migrations. An incompatible
or damaged database is replaced as a whole on daemon startup, producing a new store identity.
Configuration and canonical log files are preserved, but old Job IDs, cursors, receipts, and
idempotency history do not survive that reset.

For the exact contract, see [requirements](docs/requirements.md). Evidence for observed-resource
and quiet admission is recorded in the
[Windows verification report](docs/observed-resource-quiet-admission-verification.md). The
alpha.9 managed-policy and tree contract is recorded in the
[frozen implementation brief](docs/managed-child-policy-and-tree-views.md). Priority, aging, and
reservation semantics are normative in the
[R-RES-6 contract](docs/priority-aging-reservations.md). External readiness is normative in the
[R-COND-1..5 contract](docs/conditions-and-readiness.md).

## Developing Stillyard

On the canonical Windows development host, every Rust command must be scheduled through the
installed system Stillyard daemon:

```powershell
& .\scripts\run-stillyard-job.ps1 fmt
& .\scripts\run-stillyard-job.ps1 fmt-write
& .\scripts\run-stillyard-job.ps1 check
& .\scripts\run-stillyard-job.ps1 test
& .\scripts\run-stillyard-job.ps1 msrv-check
& .\scripts\run-stillyard-job.ps1 msrv-test
& .\scripts\run-stillyard-job.ps1 clippy
& .\scripts\run-stillyard-job.ps1 schema-update
& .\scripts\run-stillyard-job.ps1 build-release
```

The two MSRV definitions are checked-in templates. The launcher discovers the current checkout,
user Rust installation, and x64 Visual Studio/Windows SDK environment before submitting a temporary
JobSpec, so they do not need path edits in another Windows checkout.

See [AGENTS.md](AGENTS.md) for the build invariant and [CONTRIBUTING.md](CONTRIBUTING.md) for
repository conventions.

Stillyard is licensed under either Apache License 2.0 or the MIT license, at your option.
