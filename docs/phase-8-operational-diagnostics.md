# Phase 8 — Operational diagnostics and containment recovery

Status: alpha.8 design candidate for focused review (2026-08-28)

This increment turns the daemon's existing safety facts into one bounded public diagnostic
snapshot and completes the `uncertain -> cleared` Containment lifecycle. It does not add a second
health database, a generic diagnostics framework, or new scheduling policy. SQLite remains the
authority, the daemon remains its only reader, and the public crate is the only interface used by
the CLI and external consumers.

## Outcome and scope

After alpha.8 an owner can run `stillyard doctor --json` and learn which daemon, store, boot,
configuration, containment capability, and unresolved cleanup incidents are actually in force. A
consumer such as `moot` can compare the loaded profile names, scalar capacities, and canonical
configuration hash without reading Stillyard files. An owner can explicitly clear an uncertain
Containment when automatic proof remains unavailable, without editing SQLite or silently releasing
a Lease.

The increment adds:

- one `DoctorSnapshot` public-crate response, composed around the existing `DaemonSnapshot`;
- durable Windows boot and exact root-process identity for every Containment that may release user
  code;
- bounded reconciliation of uncertain Containments;
- an audited, idempotent `Client::clear_containment` operation and
  `doctor clear-containment ID --force`; and
- machine-readable checks and documented guarantee boundaries.

It does not add Conditions, resource/quiet providers, arbitrary process policing, process killing,
general integrity scanning, cascade cancellation, drain/force daemon shutdown, retention policy,
artifacts, secrets, Linux containment, or TUI redesign. `daemon-status` and
`Client::daemon_status` remain compatible lightweight surfaces. Linux v0.2 will add a Linux
`ProcessIdentity` variant and native capability checks without changing the entities or clearance
contract defined here.

## Strengthened Containment evidence

Alpha.8 does not introduce another lifecycle entity. It completes the existing Containment record
with the evidence needed to distinguish proof, uncertainty, PID reuse, and explicit risk
acceptance.

- `ContainmentState` gains the terminal `cleared` state. Ordinary cleanup still reaches `empty`.
  Only an `uncertain` Containment reaches `cleared`, with a durable resolution of
  `proven_empty`, `reboot`, or `forced_risk_acceptance`.
- The daemon obtains the Windows boot identifier from runtime-linked
  `SystemBootEnvironmentInformation` and records it as an opaque `BootId`. Failure to obtain a
  stable boot identifier is a failing doctor check and blocks release of new user code with the
  explicit `host_capability_unavailable` blocker. Read-only inspection and reconciliation that does
  not rely on boot proof remain available; the daemon never silently falls back to PID-only
  identity.
- Before a suspended primary or postcondition is resumed, the same transaction that records its
  root PID also records the exact Windows process identity: boot ID plus the process creation
  `FILETIME` value. A PID by itself is never identity.
- Each Containment records the daemon generation that created its Job Object and the declared
  strength `windows_job_object`. The existing executable hash and Invocation provenance remain
  unchanged.
- An uncertainty incident records its reason, opened time, last reconciliation time/result, root
  identity when one existed, retained claims, and eventual resolution. Resolution evidence is
  preserved after the Lease releases; a cleared record is never rewritten as naturally empty.
- Entering `uncertain` in a live daemon transfers any still-owned Job Object handle from the runner
  registry to the reconciler; it does not drop the handle. The reconciler owns that handle until it
  proves emptiness or daemon termination closes it under kill-on-close.

The alpha.8 schema is one new greenfield epoch, not a migration. Opening an alpha.7 database under
the singleton lock replaces the SQLite database and creates a new store UUID under the existing
whole-store reset rule. Configuration and canonical log files remain outside that reset; old Jobs,
cursors, result files, and containment IDs become foreign-store history and cannot authorize an
operation in the new store.

## Public crate contract

The exact public shape is intentionally small. All structs are owned, serializable,
`#[non_exhaustive]`, reject unknown fields when deserializing their current representation, and use
the crate's existing deadline and optional `CancellationToken` conventions.

```rust
pub struct BootId(pub String);

#[serde(tag = "platform", rename_all = "snake_case")]
pub enum ProcessIdentity {
    Windows {
        boot_id: BootId,
        pid: u32,
        creation_filetime_100ns: u64,
    },
}

pub enum DoctorCheckStatus { Pass, Warning, Fail, Unknown }
pub enum DoctorOverallStatus { Healthy, AttentionRequired, Unsafe }
pub enum ContainmentResolution { ProvenEmpty, Reboot, ForcedRiskAcceptance }
pub enum ReconciliationResult {
    StillResolves,
    BoundaryNotEmpty,
    IdentityUnavailable,
    ProvenEmpty,
    PriorBoot,
}

pub struct DoctorCheck {
    pub code: String,
    pub status: DoctorCheckStatus,
    pub summary: String,
    pub remediation: Option<String>,
}

pub struct DoctorBoundary {
    pub code: String,
    pub statement: String,
}

pub struct DoctorHostSnapshot {
    pub platform: String,
    pub host_name: String,
    pub boot_id: Option<BootId>,
    pub containment_strength: String,
    pub session_survival: DoctorCheckStatus,
}

pub struct DoctorStoreSnapshot {
    pub store_uuid: Uuid,
    pub schema_epoch: String,
    pub filesystem: String,
    pub sqlite_journal_mode: String,
    pub sqlite_synchronous: String,
    pub foreign_keys_enabled: bool,
}

pub struct ContainmentIncidentSnapshot {
    pub incident_id: ContainmentId,
    pub containment_id: ContainmentId,
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub invocation_id: InvocationId,
    pub state: ContainmentState,
    pub reason_code: String,
    pub detail: String,
    pub opened_unix_millis: i64,
    pub last_reconciled_unix_millis: Option<i64>,
    pub last_reconciliation: Option<ReconciliationResult>,
    pub root_identity: Option<ProcessIdentity>,
    pub retained_claims: ResourceClaims,
    pub resolution: Option<ContainmentResolution>,
    pub resolved_unix_millis: Option<i64>,
}

pub struct DoctorIncidentPage {
    pub total_unresolved: u64,
    pub incidents: Vec<ContainmentIncidentSnapshot>,
    pub truncated: bool,
}

pub struct DoctorSnapshot {
    pub schema_version: u32, // 1 in alpha.8
    pub observed_unix_millis: i64,
    pub overall: DoctorOverallStatus,
    pub daemon: DaemonSnapshot,
    pub host: DoctorHostSnapshot,
    pub store: DoctorStoreSnapshot,
    pub checks: Vec<DoctorCheck>,
    pub incidents: DoctorIncidentPage,
    pub boundaries: Vec<DoctorBoundary>,
}

pub struct ClearContainmentRequest {
    pub containment_id: ContainmentId,
    pub force: bool,
}

pub struct ContainmentClearanceAudit {
    pub requested_unix_millis: i64,
    pub resolved_unix_millis: i64,
    pub requester: ProcessIdentity,
    pub daemon_generation: Uuid,
    pub resolution: ContainmentResolution,
    pub last_reconciliation: ReconciliationResult,
}

pub struct ClearContainmentResult {
    pub containment_id: ContainmentId,
    pub prior_state: ContainmentState,
    pub state: ContainmentState, // cleared
    pub lease_released: bool,
    pub audit: ContainmentClearanceAudit,
}
```

`ProcessIdentity` is an enum rather than an untyped start token so later Linux evidence cannot be
mistaken for Windows evidence. `BootId` is opaque to callers; equality is meaningful only within
one platform implementation. The Windows representation is the canonical lowercase UUID string.

`Client::doctor(deadline, cancellation)` returns `DoctorSnapshot`.
`Client::clear_containment(request, deadline, cancellation)` returns a persisted
`ClearContainmentResult`. A disconnect never makes the result ambiguous: retrying the same
store-scoped Containment ID returns the already committed clearance result. IDs from a replaced or
foreign store reject and never select by the entity UUID alone.

The public JSON representation is exactly the serde representation of these public types.
`stillyard doctor --json` emits one `DoctorSnapshot`; clearance with `--json` emits one
`ClearContainmentResult`. There is no parallel hand-written DTO or CLI-only schema.

## Doctor snapshot semantics

The daemon constructs one snapshot from in-memory loaded configuration, bounded OS probes, and one
SQLite read transaction. Clients never open `config.json`, SQLite, the named-pipe namespace, or
canonical log files to assemble diagnostics. The daemon materializes the bounded owned response and
releases its store mutex/read transaction before writing to the pipe, so a stalled client cannot
hold writers or lifecycle progress.

Checks are sorted by stable `code`; incidents are sorted by `(opened_unix_millis,
containment_id)`. Human summaries and remediation may improve, but consumers branch only on typed
status and documented codes. Alpha.8 defines at least:

| Code | Meaning |
|---|---|
| `host.boot_identity` | a stable boot ID is available |
| `ipc.owner_only` | the active endpoint has the required owner-only boundary |
| `store.schema` | the opened store has the current validated epoch and identity |
| `store.filesystem` | store and SQLite sidecars are on supported local fixed NTFS |
| `store.sqlite_durability` | WAL, synchronous, and foreign-key settings match the contract |
| `configuration.loaded` | the reported profile/capacity/hash view is the loaded configuration |
| `containment.windows_job_object` | born-contained, no-breakaway, kill-on-close support is active |
| `containment.unresolved` | count and retained-capacity impact of uncertain Containments |

`overall` is derived, never independently assigned: any `Fail` is `Unsafe`; otherwise a Warning or
Unknown is `AttentionRequired`; otherwise it is `Healthy`. An unresolved Containment is Warning,
not Fail, because its Lease remains safely unavailable. A missing mandatory platform primitive is
Fail. Documented cooperative-threat and physical-power-loss limits live in `boundaries`, not as
permanent warnings that would make every healthy host yellow.

The response repeats no profile operations, environment values, secret names or values, command
lines, child output, or raw configuration. `daemon.profile_names`, `daemon.capacities`, and
`daemon.config_sha256` are the complete consumer-facing configuration evidence. The canonical hash
continues to cover the loaded non-secret `HostConfig` representation.

The incident page contains at most 256 unresolved incidents and reports the full count and
`truncated`. This keeps the local protocol frame and client memory bounded. When truncated, an
operator clears or resolves visible incidents and repeats doctor; exact known IDs remain available
through Job snapshots and are accepted directly by clearance. Resolved audit history is retained
with its Job/Containment but is not copied into every doctor snapshot.

`incident_id` is the existing compatibility alias for its owning `containment_id`; they are equal in
alpha.8 and do not create a tenth durable entity. `reason_code` is stable machine-readable data;
`detail` and remediation text are explanatory and may improve without changing the code.

Doctor boundaries include stable codes for at least:

- `physical_power_loss_after_ack`: the R-SCOPE-2 storage boundary;
- `same_owner_out_of_boundary_process`: deliberate same-owner handle duplication, WMI, Task
  Scheduler, and equivalent bypasses are outside the cooperative containment guarantee; and
- `no_hard_resource_partition`: resource admission is not CPU/GPU/RAM hard enforcement.

`doctor` is diagnostic, not a policy engine. Successfully returning a complete snapshot exits 0
even when `overall` is `AttentionRequired` or `Unsafe`; callers decide which checks gate their own
work. Transport/store failure before a snapshot uses the existing 69/70 errors. This lets `moot`
read configuration evidence without treating an unrelated retained incident as config drift.

## Reconciliation and proof rules

Uncertainty never expires and elapsed time is never proof. Reconciliation runs at startup and only
while at least one uncertain Containment exists. The daemon uses a finite backoff capped at 30
seconds, wakes promptly on a new incident, and stops the timer when none remain. A healthy idle
daemon therefore gains no polling wakeup and keeps the A-19 budget.

An uncertain Containment may resolve automatically only under one of these proofs:

1. The recorded boot differs from the current stable boot ID. Reboot proves prior-boot Windows
   processes gone; resolution is `reboot`.
2. In the creating/no-root state, durable ordering proves user code was never resumed and the
   creating daemon's Job Object is gone or observed empty.
3. In the creating daemon generation, the daemon-held Job Object reports zero processes and the
   exact recorded root identity no longer resolves.
4. After daemon restart in the same boot, closure of the prior daemon's kill-on-close handle plus
   disappearance of the exact recorded root identity proves the prior tree gone within the declared
   cooperative boundary.

PID absence alone is not queried; exact identity is. If the numeric PID now has a different
creation `FILETIME`, the recorded process is gone and the new occupant is never terminated or
treated as the old root. Access denied, an unavailable boot source, a matching identity, or an
uninspectable boundary preserves uncertainty unless the stronger reboot proof applies.

Every successful automatic resolution atomically changes `uncertain -> cleared`, records its proof,
releases the Attempt Lease only when no creating/live/uncertain sibling Containment remains, and
commits the ordinary Containment/event invalidation. Watchers therefore refresh without a private
doctor channel.

## Explicit clearance

`doctor clear-containment ID --force` is risk acceptance, not proof and not process cleanup. It
never kills, opens for termination, closes a live Job Object handle, or otherwise changes an
external process.

The daemon applies this order:

1. Authenticate the named-pipe peer and reject every caller that is currently proved inside any
   Stillyard-managed Invocation. A Job cannot clear its own, an ancestor's, or a competitor's Lease.
   The request contains no trusted parent/caller identity.
2. Require `force == true`, a current-store ID, and state `uncertain`. A naturally `empty`
   Containment is not rewritten. A previously `cleared` Containment returns its persisted result
   idempotently.
3. Reconcile once. If proof now exists, commit/return the proof resolution rather than calling it
   forced.
4. If the exact recorded root identity still resolves on the current boot, reject with stable code
   `containment_identity_still_resolves`, even under `--force`. If a daemon-held Job Object is known
   nonempty, reject with `containment_boundary_not_empty`; if its owned handle cannot be inspected,
   reject with `containment_owned_boundary_uninspectable`. The remediation says to let cleanup
   finish, terminate the recorded work through its owner, or reboot. Force is available only when
   the current daemon owns no boundary handle and probing the target identity returns absent,
   nonmatching, or unavailable rather than an affirmative exact match. A PID occupied by a
   different creation identity does not trigger the root-identity refusal.
5. Capture the unmanaged requester's exact process identity. Begin an immediate transaction,
   re-read the Containment and root identity, and commit only if they still match the probed record.
   A concurrent proof/clear returns the committed result; any other change retries the bounded
   decision or rejects.
6. Atomically record `forced_risk_acceptance`, the requester, timestamp and daemon generation;
   transition to `cleared`; release the Attempt Lease only when no unresolved sibling remains; and
   emit the normal Containment event.

If requester identity or current boot identity cannot be obtained, forced clearance rejects: an
unauditable operator mutation is worse than a retained Lease. A crash before the transaction leaves
the incident and Lease unchanged. A crash after commit leaves the complete clearance, audit,
conditional Lease release, and event together. No success is printed before commit.

## CLI surface and compatibility

```text
stillyard doctor [--json]
stillyard doctor clear-containment ID --force [--json]
```

Human doctor output starts with `healthy`, `attention required`, or `unsafe`, then daemon/store
identity, configuration evidence, failed/unknown checks, unresolved incidents, and boundaries.
JSON prints only the public response. Clearance success exits 0. Missing `--force`, managed caller,
foreign/not-uncertain target, a still-resolving exact identity, or unauditable requester rejects
with exit 27 and a stable machine-readable error code. Deadline is 25, unavailable daemon/store is
69, and internal/protocol inconsistency is 70.

`daemon-status` remains present because it is the cheap compatibility/readiness surface. Doctor
does not replace watch, list, status, logs, or events, and no TUI pane gains privileged access.

## Acceptance and adversarial evidence

Alpha.8 is implementation-ready only with explicit tests for all rows below. Shipped-path tests use
the public crate and CLI; store tests may construct fault states but cannot be the only evidence.

1. **Public path and consumer evidence.** `Client::doctor` and `doctor --json` deserialize to the
   same public type and expose the daemon's actually loaded profiles, capacities, and configuration
   hash. Changing configured evidence without restarting does not change the loaded snapshot;
   restarting does. `moot`'s config-drift adapter test consumes only this response. A
   read-config-file-in-the-consumer mutant fails.
2. **Redaction and boundedness.** A configuration containing sentinel environment/profile values
   leaks neither those values nor child output through human or JSON doctor output. More than 256
   incidents reports the exact total, a stable first page, and `truncated`, within the protocol
   frame bound. Secret-value and unbounded-incident mutants fail.
3. **Durable identity.** A real suspended Windows child records boot ID, PID, and creation
   `FILETIME` before resume. A same-PID/different-creation fixture is not the recorded identity; a
   matching fixture is. PID-only and record-after-resume mutants fail.
4. **Uncertainty visibility.** Injected cleanup timeout reaches final Job + uncertain Containment,
   retains the complete Lease, appears in doctor with its incident and blocker impact, and updates
   watch through the ordinary event path. A timeout-means-empty mutant fails.
5. **Automatic proof.** Same-daemon Job Object emptiness, prior-daemon kill-on-close plus exact-root
   disappearance, creating/no-root proof, and an actual boot-ID change each clear atomically and
   release only the eligible Attempt Lease. Matching root, unavailable identity, and unavailable
   boot evidence remain uncertain. Root-gone-alone and daemon-generation-is-boot mutants fail.
6. **Forced safety.** An unmanaged owner can force an unprovable/root-gone incident; a managed Job,
   missing force, foreign ID, naturally empty record, matching exact root, or unauditable requester
   cannot. A known-nonempty or owned-but-uninspectable Job Object also refuses; clearance never
   closes a handle as a hidden kill operation. PID reuse does not refuse and is recorded in probe
   evidence. Managed-self-clear, force-clears-live-root, force-drops-owned-boundary, and
   PID-reuse-is-live mutants fail.
7. **Idempotence and concurrency.** Disconnect immediately before/after clearance commit and two
   concurrent clear callers produce either no change or one identical persisted result. No audit,
   state, Lease, or event is duplicated. A check-then-update mutant fails.
8. **Sibling and crash atomicity.** Clearing one incident does not release an Attempt Lease while
   any sibling Containment is creating/live/uncertain. SQLite failure at every clearance write
   boundary exposes the full prior or full new state. Release-before-audit and first-sibling-release
   mutants fail.
9. **Wake and cost discipline.** With no incidents, doctor adds no background thread, helper
   process, or periodic wake. With incidents, reconciliation backs off and cannot block submission,
   log draining, or observation; a slow doctor client cannot block writers. Busy-reconcile and
   viewer-holds-store-lock mutants fail.
10. **Greenfield and provenance.** The new epoch replaces alpha.7 SQLite only under the singleton
    lock, changes store UUID, preserves config/log files, and makes every old Containment ID foreign.
    Doctor reports the new epoch, boot, host, version, generation, and cooperative boundaries.
    Partial-migration and stale-ID-clear mutants fail.

Focused fleet review must attack the public API size, machine-readable compatibility, exact process
and reboot proof, managed-caller authorization, clearance transaction, sibling Lease ownership,
reconciliation wake discipline, and whether any requirement accidentally turns doctor into a
second scheduler or policy engine. Confirmed High/Critical/Blocker findings are fixed and sent back
to the affected reviewer; Medium/Low findings may be corrected silently when they stay within this
frozen scope.
