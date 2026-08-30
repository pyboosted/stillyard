# Stillyard

*A local scheduler for expensive Windows processes.*

Stillyard coordinates programs that share one workstation: builds, tests, GPU work, benchmarks,
and automation launched by tools or coding agents. Callers submit ordinary executables with their
resource and quiet-host requirements; one per-user daemon decides when they may start, keeps their
state and logs, and exposes visible reasons while they wait.

Stillyard is useful when independent processes need one scheduling authority without moving into
containers, a CI service, or a remote worker platform.

Current release: **0.1.0-alpha.10 for Windows 10 1809+ and Windows Server 2019+**.

## What works today

- **Durable execution.** Submit one Job or an atomic Batch, receive an idempotent receipt, recover
  an interrupted submission, wait for completion, and read canonical stdout and stderr.
- **Resource scheduling.** Declare CPU, RAM, Cargo slots, GPU slots, custom scalar resources,
  shared or exclusive path fences, and host-configured incompatible impacts.
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
  bounded retries, executable postconditions, managed child submission, and safe managed waits
  are supported.
- **Managed capability policy.** A primary may authorize bounded child claims, impacts, fences,
  observed/quiet admission, mandatory labels, and narrower delegation without reserving those
  capabilities in the parent's Lease. Denials are durable Submission decisions.
- **Operator interfaces.** The CLI provides submit, recover, status, list, events, logs, wait,
  cancel, daemon status, schema, doctor, and bounded tree commands. `stillyard watch` is an
  event-driven parent/child forest. The Rust crate exposes the same public protocol through a
  blocking client.

Resource declarations are admission reservations, not hard CPU, RAM, or GPU limits on a running
process. Callers should declare every scarce resource or conflicting impact they rely on.

## Quick start

The examples below assume `stillyard.exe` is installed or available on `PATH`. A request to the
default endpoint starts the per-user daemon when necessary.

Create `hello.json`:

```json
{
  "spec_version": 2,
  "executable": "C:\\Windows\\System32\\cmd.exe",
  "args": ["/d", "/c", "echo hello from Stillyard"],
  "working_directory": "C:\\"
}
```

Submit it and stream its retained output:

```powershell
stillyard submit --spec .\hello.json --wait --passthrough
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

JobSpec and host-config documents are strict and versioned. Print the authoritative schemas with:

```powershell
stillyard schema spec
stillyard schema config
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
distributed placement, container runtime, Linux runner, secrets, artifacts, external Conditions,
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
[frozen implementation brief](docs/managed-child-policy-and-tree-views.md).

## Developing Stillyard

On the canonical Windows development host, every Rust command must be scheduled through the
installed system Stillyard daemon:

```powershell
& .\scripts\run-stillyard-job.ps1 fmt
& .\scripts\run-stillyard-job.ps1 fmt-write
& .\scripts\run-stillyard-job.ps1 check
& .\scripts\run-stillyard-job.ps1 test
& .\scripts\run-stillyard-job.ps1 clippy
& .\scripts\run-stillyard-job.ps1 schema-update
& .\scripts\run-stillyard-job.ps1 build-release
```

See [AGENTS.md](AGENTS.md) for the build invariant and [CONTRIBUTING.md](CONTRIBUTING.md) for
repository conventions.

Stillyard is licensed under either Apache License 2.0 or the MIT license, at your option.
