# Phase 8 — Operational diagnostics and containment recovery

Status: alpha.8 reviewed implementation baseline, frozen 2026-08-28

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
- an audited, idempotent `Client::force_clear_containment` operation and
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

- `ContainmentState` gains the terminal `cleared` state and a response-only `Unknown(String)`
  fallback that preserves the unrecognized wire value.
  Ordinary cleanup still reaches `empty`. Only an `uncertain` Containment reaches `cleared`, with a
  durable resolution of `proven_empty`, `reboot`, or `forced_risk_acceptance`; `unknown` never
  participates in an internal transition or proof.
- At daemon-generation start, before admission can grant a Lease, the daemon obtains and latches a
  Windows host identity and boot identifier. `HostId` is a domain-separated SHA-256 of the local
  MachineGuid; the raw MachineGuid is never exposed. `BootId` comes from runtime-linked
  `SystemBootEnvironmentInformation`. Either source being unavailable is a failing doctor check and
  gives pending work the explicit `host_capability_unavailable` blocker before Lease grant. The
  daemon never re-probes boot as a release-time gate and never silently falls back to PID-only
  identity. A later probe disagreeing with the latched value is a capability failure, not reboot
  proof.
- Before a suspended primary or postcondition is resumed, the same transaction that records its
  root PID also records the exact Windows process identity: boot ID plus the process creation
  `FILETIME` value. A PID by itself is never identity.
- The transaction that creates a `creating` Containment records host ID, boot ID, creating daemon
  generation, and strength `windows_job_object` before any process-creation call. Each daemon
  generation also durably records its own exact process identity. The existing executable hash and
  Invocation provenance remain unchanged.
- The singleton lock is keyed by the canonical fixed-store filesystem identity, not a caller path,
  and is held for the daemon process lifetime. Job Object handles are non-inheritable and are never
  duplicated or passed to contained code. A new daemon may infer closure of an old generation's
  handles only while it holds that lock and the old daemon's exact process identity no longer
  resolves.
- An uncertainty incident durably records its reason, opened time, root identity when one existed,
  retained claims, and eventual resolution. The current daemon's last reconciliation time/result is
  bounded in-memory diagnostic evidence until the final result is committed. Resolution evidence is
  preserved after the Lease releases; a cleared record is never rewritten as naturally empty.
- Entering `uncertain` in a live daemon transfers any still-owned Job Object handle from the runner
  registry to the reconciler; it does not drop the handle. The reconciler owns that handle until it
  proves emptiness or daemon termination closes it under kill-on-close.

For every release decision, the **Lease-blocking Containment set** is the durable set of all
Containments whose Invocations belong to the same Attempt, across primary and every postcondition.
It comes from one SQLite query with no daemon-generation, role, runner-registry, or reconciler-
registry filter. The Attempt Lease may release only when the inherited Attempt lifecycle is closed
to new Invocations and every record in that set is terminal (`empty` or `cleared`); an unknown or
future internal state therefore fails closed. "Sibling" never means merely the same Invocation.

Ordinary empty cleanup, automatic reconciliation, and forced clearance call one store primitive for
that decision. Inside the same immediate transaction that persists the target Containment change,
the primitive re-reads the complete durable set and Attempt settlement eligibility, conditionally
releases the Lease, records `lease_released`, and emits the event. It never derives release safety
from the current-generation enumeration used to authorize a managed peer. Postconditions remain
sequential and start only after preceding natural cleanup reaches `empty`; an uncertain or cleared
Containment never opens a later postcondition. Thus a release transaction is also the serialization
point after which no new Containment for that Attempt can appear.

The alpha.8 schema is one new greenfield epoch, not a migration. Opening an alpha.7 database under
the singleton lock replaces the SQLite database and creates a new store UUID under the existing
whole-store reset rule. Configuration and canonical log files remain outside that reset; old Jobs,
cursors, result files, and containment IDs become foreign-store history and cannot authorize an
operation in the new store.

Host binding is part of current-store identity. A valid store bound to a different current
`HostId`, or an unbound store that somehow contains a Containment/granted Lease, selects the same
whole-database reset under the singleton lock as another invalid store identity. An unbound store
with no Containment and no granted Lease may bind atomically even when it contains accepted pending
Jobs, because no user code could have been released. A simultaneous machine clone has independent
host-local resources and receives a new store UUID; it cannot import or clear the other host's
records.

## Public crate contract

The exact public shape is intentionally small. Public response structs are owned, serializable, and
`#[non_exhaustive]`; they ignore unknown response fields so additive evidence remains readable.
Public enums are `#[non_exhaustive]` and safety-relevant enums have an `Unknown` fallback; unknown
identity/proof values never authorize proof or clearance. Requests still reject unknown fields.
There is no externally constructed clearance request struct: the explicit
`Client::force_clear_containment` method is the risk acknowledgement. All methods use the crate's
existing deadline and optional `CancellationToken` conventions.

```rust
pub struct BootId(pub String);
pub struct HostId(pub String);

pub enum ProcessIdentity {
    Windows {
        host_id: HostId,
        boot_id: BootId,
        pid: u32,
        creation_filetime_100ns: u64,
    },
    Unknown { unknown_platform: String, evidence: serde_json::Value },
}

pub enum DoctorCheckStatus { Pass, Warning, Fail, Unknown(String) }
pub enum DoctorOverallStatus { Healthy, AttentionRequired, Unsafe, Unknown(String) }
pub enum ContainmentResolution { ProvenEmpty, Reboot, ForcedRiskAcceptance, Unknown(String) }
pub enum ReconciliationResult {
    StillResolves,
    BoundaryNotEmpty,
    BoundaryUninspectable,
    IdentityUnavailable,
    IdentityAbsent,
    PidReused,
    ProvenEmpty,
    PriorBoot,
    Unknown(String),
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
    pub host_name: Option<String>,
    pub host_id: Option<HostId>,
    pub boot_id: Option<BootId>,
    pub containment_strength: String,
    pub session_survival: DoctorCheckStatus,
}

pub struct DoctorStoreSnapshot {
    pub store_uuid: Uuid,
    pub schema_epoch: String,
    pub bound_host_id: Option<HostId>,
    pub filesystem: String,
    pub sqlite_journal_mode: String,
    pub sqlite_synchronous: String,
    pub foreign_keys_enabled: bool,
}

pub struct ContainmentIncidentSnapshot {
    pub incident_id: ContainmentId,
    pub incident_sequence: u64,
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
    pub next_cursor: Option<ContainmentIncidentCursor>,
}

pub struct ContainmentIncidentCursor { /* store UUID + durable incident sequence + Containment ID */ }

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

pub struct ForcedClearanceAudit {
    pub requested_unix_millis: i64,
    pub requester: ProcessIdentity,
}

pub enum ClearanceOrigin { Automatic, Forced, Unknown(String) }

pub struct ContainmentResolutionAudit {
    pub resolved_unix_millis: i64,
    pub daemon_generation: Uuid,
    pub resolution: ContainmentResolution,
    pub last_reconciliation: ReconciliationResult,
    pub origin: ClearanceOrigin,
    pub forced: Option<ForcedClearanceAudit>,
    pub lease_released: bool,
}

pub struct ClearContainmentResult {
    pub schema_version: u32, // 1 in alpha.8
    pub containment_id: ContainmentId,
    pub prior_state: ContainmentState,
    pub state: ContainmentState, // cleared
    pub audit: ContainmentResolutionAudit,
}
```

`ProcessIdentity` is an enum rather than an untyped start token so later Linux evidence cannot be
mistaken for Windows evidence. `HostId` and `BootId` are opaque to callers; equality is meaningful
only for the same platform and host binding. The Windows boot representation is the canonical
lowercase UUID string. A cloned machine identity is part of the documented cooperative-host
boundary; it never weakens the requirement to match `HostId` before using boot inequality as proof.

Safe unknown handling is implemented with explicit hand-written `Serialize`/`Deserialize`, not
`#[serde(other)]` or a derive assumption. The scalar enums printed above map every unrecognized wire
string to `Unknown(original)`. `ProcessIdentity` first captures the single `platform` tag; a known
tag decodes its exact fields, while an unknown tag becomes
`Unknown { unknown_platform, evidence }`, where `evidence` is the remaining object without a second
`platform` key. Serialization reconstructs exactly one tag. The same response-only rule applies to
`ContainmentState::Unknown(String)`. Acceptance compiles these implementations and injects future
wire tags; prose alone is not evidence.

String-valued platform, strength, filesystem, SQLite mode, epoch, check, boundary, and reason codes
are open vocabularies with stable documented known values. Consumers preserve/tolerate unknown
values and gate only the specific checks they own; `overall == Healthy` is not a stable substitute
for checking a consumer's required codes.

`Client::doctor(cursor: Option<ContainmentIncidentCursor>, limit: Option<u32>, deadline,
cancellation)` returns `DoctorSnapshot`; `None` limit means 256 and values clamp to 256.
`ContainmentIncidentCursor` serializes as one opaque string, has a public parse/display round-trip,
and follows the same store-scoping rules as the existing observation cursors.
`Client::force_clear_containment(containment_id, deadline, cancellation)` returns a persisted
`ClearContainmentResult`. A disconnect never makes the result ambiguous: retrying the same
store-scoped Containment ID returns the original `prior_state`, audit, and `lease_released` value.
IDs from a replaced or foreign store reject and never select by entity UUID alone.

`DaemonSnapshot` gains the current daemon optional `process_identity`. `InvocationSnapshot` gains
`root_identity`. `ContainmentSnapshot` gains the incident, resolution, and resolution audit, so
forced or automatic clearance remains publicly inspectable through ordinary `status`/TUI detail
after immediate command output is gone. Doctor copies only unresolved incident pages; it is not the
audit-history read path.

The public `Error` enum remains `#[non_exhaustive]` when it gains the clearance rejection variant,
so an external exhaustive match is never required.

The public JSON representation is exactly the serde representation of these public types.
`stillyard doctor --json` emits one `DoctorSnapshot`; clearance with `--json` emits one
`ClearContainmentResult`. There is no parallel hand-written DTO or CLI-only schema.

## Doctor snapshot semantics

The daemon constructs one snapshot from in-memory loaded configuration, bounded OS probes, and one
SQLite read transaction. Clients never open `config.json`, SQLite, the named-pipe namespace, or
canonical log files to assemble diagnostics. The daemon materializes the bounded owned response and
releases its store mutex/read transaction before writing to the pipe, so a stalled client cannot
hold writers or lifecycle progress.

Host/boot probing precedes store admission initialization. If either source is unavailable the
daemon can still open/create the store and serve read-only diagnostics, but leaves an unbound new
store unbound and grants no work Lease. The first later generation with valid evidence may bind an
unbound store atomically when it has no Containment and no granted Lease, including when it has
accepted pending Jobs. An unbound store with containment evidence, or a store bound to a different
`HostId`, is replaced as an invalid store identity under the singleton lock; no record from it is
admitted, auto-cleared, or force-cleared in the replacement store.

Checks are sorted by stable `code`; incidents are assigned a monotonic durable sequence when
uncertainty opens and page in `(incident_sequence, containment_id)` order. Wall-clock adjustment
therefore cannot reorder a cursor window. Human summaries and remediation may improve, but consumers
branch only on typed status and documented codes. Alpha.8 defines at least:

| Code | Meaning |
|---|---|
| `host.machine_identity` | the current host matches the store's latched host binding |
| `host.boot_identity` | a stable boot ID is available |
| `host.session_survival` | the platform's declared detach/session-survival prerequisite holds |
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

The incident page contains at most 256 unresolved incidents and reports the full count,
`truncated`, and a stable next cursor. Subsequent pages cannot be starved by an old unresolvable
first page. Foreign-store, missing-row, future, or malformed cursors reject explicitly. Exact known
IDs remain available through Job snapshots and are accepted directly by clearance. Resolved audit
history is retained with its Job/Containment but is not copied into every doctor snapshot.

`incident_id` is the existing compatibility alias for its owning `containment_id`; they are equal in
alpha.8 and do not create a tenth durable entity. `reason_code` is stable machine-readable data;
`detail` and remediation text are explanatory and may improve without changing the code.
Codes are bounded to 128 ASCII bytes, summaries to 1,024 UTF-8 bytes, and detail/remediation to
2,048 UTF-8 bytes. Incident `retained_claims` is the bounded public `JobSpec.resources` declaration,
not private resolved fence identities. Command lines, paths outside already-public store paths,
environment values, and raw OS error buffers are never copied into these strings. The 256-item
maximum plus these field bounds is tested below the existing 16 MiB protocol limit.

Doctor boundaries include stable codes for at least:

- `physical_power_loss_after_ack`: the R-SCOPE-2 storage boundary;
- `same_owner_out_of_boundary_process`: deliberate same-owner handle duplication, WMI, Task
  Scheduler, and equivalent bypasses are outside the cooperative containment guarantee; and
- `cloned_host_identity`: software cannot distinguish two simultaneously running machine clones
  that carry the same MachineGuid-derived identity; and
- `no_hard_resource_partition`: resource admission is not CPU/GPU/RAM hard enforcement.

The root `doctor` snapshot is diagnostic, not a policy engine. The explicitly named
`clear-containment` remediation is the one mutating operation assigned to doctor by R-RUN-4; it
cannot be triggered by reading a snapshot. Successfully returning a complete snapshot exits 0
even when `overall` is `AttentionRequired` or `Unsafe`; callers decide which checks gate their own
work. Transport/store failure before a snapshot uses the existing 69/70 errors. This lets `moot`
read configuration evidence without treating an unrelated retained incident as config drift.

## Reconciliation and proof rules

Uncertainty never expires and elapsed time is never proof. Reconciliation runs at startup and only
while at least one uncertain Containment exists. Each turn takes at most 32 incidents in stable
round-robin order, snapshots their versioned evidence under the store mutex, performs every OS probe
outside the mutex, and compare-and-commits any resolution through the shared Lease-release store
primitive. Its immediate transaction re-reads both the target versioned evidence and complete
durable Lease-blocking set; the pre-probe snapshot never authorizes release. Per-incident backoff
grows through 1/2/4/8/16/30/60/120/300 seconds; a new incident or owned-boundary empty notification
wakes it promptly. The timer stops when no incidents remain. A healthy idle daemon therefore gains
no polling wakeup and keeps the A-19 budget.

Unchanged probes do not write SQLite, fsync, emit an event, or wake watchers. Their latest time and
result are bounded current-daemon diagnostic memory exposed by doctor; only the durable incident
opening and final resolution/audit are stored. Restart simply probes unresolved durable incidents
again. No OS wait or process-handle query occurs while the store mutex is held.

An uncertain Containment may resolve automatically only under one of these proofs:

1. The record has the same `HostId`, belongs to a prior daemon generation, and its recorded boot
   differs from the current daemon's startup-latched `BootId`. Reboot proves prior-boot Windows
   processes gone; resolution is `reboot`. Host mismatch or any later boot-probe disagreement is a
   Fail and never proof.
2. A same-generation creating/no-root Containment clears only when its reconciler-owned Job Object
   reports zero processes. A prior-generation creating/no-root record clears only after the new
   daemon holds the canonical singleton lock and the recorded prior daemon process identity no
   longer resolves; durable ordering proves user code was never resumed and non-inheritable
   kill-on-close closed the old boundary. An absent/uninspectable same-generation handle is not
   proof.
3. In the creating daemon generation, the daemon-held Job Object reporting zero active processes is
   sufficient born-contained proof. Root identity is still probed and reported as evidence, but an
   exited process object retained by another handle cannot prevent this stronger boundary proof.
4. After daemon restart in the same host/boot, the new daemon holds the canonical singleton lock,
   the recorded old daemon exact identity no longer resolves, and the exact root identity is gone.
   Those facts plus the non-inheritable kill-on-close handle prove the prior tree gone within the
   declared cooperative boundary.

PID absence alone is not queried; exact identity is. A root "still resolves" only when host, boot,
PID and creation `FILETIME` match and the process handle is not signaled as terminated. An exited
process object retained by another observer is gone for this purpose. If the numeric PID has a
different creation `FILETIME`, the probe is `PidReused`; the new occupant is never terminated or
treated as the old root. Access denied/identity unavailable, host mismatch, unavailable boot
evidence, a matching nonterminated identity, or an uninspectable boundary preserves uncertainty
unless the stronger prior-boot proof applies.

Every successful automatic resolution atomically changes `uncertain -> cleared`, records its proof,
and invokes the shared transaction predicate; it releases the Attempt Lease only when every durable
Containment owned by that Attempt is terminal across the primary Invocation and every postcondition,
and commits the ordinary Containment/event invalidation. After a commit that actually releases a Lease,
the daemon signals the admission scheduler as well as the observation condition; the event hook
alone is not an admission wakeup. Watchers therefore refresh without a private doctor channel and
newly eligible pending Jobs do not remain parked.

## Explicit clearance

`doctor clear-containment ID --force` is risk acceptance, not proof and not process cleanup. It
never kills, opens for termination, closes a live Job Object handle, or otherwise changes an
external process.

The daemon applies this order:

1. Capture one exact peer process handle/identity at pipe connection and reuse it for authorization
   and audit; PID is never re-resolved after the decision. For clearance only, test membership
   against the union of runner- and reconciler-owned Job Objects for every current-generation
   creating/live/uncertain Invocation of every role. Reject any match; a missing/uninspectable handle
   for a current-generation candidate rejects authorization rather than downgrading the peer to
   unmanaged. Also reject a peer whose exact identity matches any unresolved recorded root. A Job
   cannot clear its own, an ancestor's, or a competitor's Lease. The request contains no trusted
   parent/caller identity. Under the cooperative non-inheritable-handle rule no prior-generation
   descendant survives after the old daemon exits; deliberate handle escape remains the stated
   R-SCOPE-2 boundary.
2. Require the explicit force-clear operation, a current-store ID, and state `uncertain`. A
   naturally `empty` Containment is not rewritten. A previously `cleared` Containment returns its
   original persisted result idempotently, including `prior_state` and whether that resolution
   released the Lease.
3. Reconcile once. If proof now exists, commit/return the proof resolution rather than calling it
   forced.
4. If the exact recorded root identity still resolves on the current boot, reject with stable code
   `containment_identity_still_resolves`, even under `--force`. If a daemon-held Job Object is known
   nonempty, reject with `containment_boundary_not_empty`; if its owned handle cannot be inspected,
   reject with `containment_owned_boundary_uninspectable`. The remediation says to let cleanup
   finish, terminate the recorded work through its owner, restart the daemon for an owned-but-
   uninspectable boundary, or perform a full Restart (Fast Startup shutdown is insufficient) to
   change the boot identity. Target-identity `IdentityUnavailable` also rejects with
   `containment_identity_unavailable`. Force is available only when the current daemon owns no
   boundary handle and the target identity is affirmatively absent or nonmatching. A PID occupied by
   a different creation identity records `PidReused` and does not trigger the root-identity refusal.
5. Perform the target probe outside SQLite. Begin an immediate transaction, re-read the
   Containment's version and identity plus the complete durable Lease-blocking set and Attempt
   settlement eligibility, and commit only if the target matches the probed record. The shared store
   primitive evaluates that set in this transaction, never from the step-1 authorization registry.
   A concurrent proof/clear returns the committed result; any other change gets one bounded retry or
   rejects.
6. Atomically record `forced_risk_acceptance`, the requester, timestamp and daemon generation;
   transition to `cleared`; invoke the shared transaction predicate; release the Attempt Lease only
   when every durable Containment owned by that Attempt is terminal across the primary Invocation and
   every postcondition; and emit the normal Containment event. If the Lease releases, signal the
   admission scheduler after commit.

If requester identity or current boot identity cannot be obtained, forced clearance rejects: an
unauditable operator mutation is worse than a retained Lease. A crash before the transaction leaves
the incident and Lease unchanged. A crash after commit leaves the complete clearance, audit,
conditional Lease release, and event together. No success is printed before commit.

## CLI surface and compatibility

```text
stillyard doctor [--incident-cursor CURSOR] [--incident-limit N] [--json]
stillyard doctor clear-containment ID --force [--json]
```

Human doctor output starts with `healthy`, `attention required`, or `unsafe`, then daemon/store
identity, configuration evidence, failed/unknown checks, unresolved incidents, and boundaries.
JSON prints only the public response. Clearance success exits 0. Missing `--force` is CLI usage and
exits 64. The public crate gains `Error::Rejected { code, detail }`; managed caller,
foreign/not-uncertain target, still-resolving/unavailable target identity, nonempty/uninspectable
owned boundary, host mismatch, or unauditable requester maps to it and exit 27. The documented codes
are `containment_caller_managed`, `containment_authorization_unavailable`,
`containment_foreign`, `containment_not_uncertain`,
`containment_identity_still_resolves`, `containment_identity_unavailable`,
`containment_boundary_not_empty`, `containment_owned_boundary_uninspectable`,
`containment_host_mismatch`, and `containment_requester_unidentifiable`. Deadline is 25,
unavailable daemon/store is 69, and internal/protocol inconsistency is 70.

`prior_state` always describes the original persisted transition, not the retry caller's observed
state. Human clearance output distinguishes `cleared now` from `already cleared automatically` and
`already force-cleared by PID/start identity at time`; JSON always returns the original audit.

`daemon-status` remains present because it is the cheap compatibility/readiness surface. Doctor
does not replace watch, list, status, logs, or events, and no TUI pane gains privileged access.

## Acceptance and adversarial evidence

Alpha.8 is implementation-ready only with explicit tests for all rows below. Shipped-path tests use
the public crate and CLI; store tests may construct fault states but cannot be the only evidence.

1. **Public path and consumer evidence.** `Client::doctor` and `doctor --json` deserialize to the
   same public type and expose the daemon's actually loaded profiles, capacities, and configuration
   hash. Changing configured evidence without restarting does not change the loaded snapshot;
   restarting does. `moot`'s config-drift adapter test consumes only this response. A
   separate external crate compiles calls to `doctor` and `force_clear_containment`; additive unknown
   response fields deserialize. Fixtures inject an unknown scalar-enum string and an unknown
   `ProcessIdentity.platform`; both preserve the original value through a deserialize/serialize
   round-trip, become `Unknown`, and cannot authorize a transition. Unconstructable-request,
   reject-additive-response, unknown-enum-rejected, unknown-platform-duplicated, and
   read-config-file-in-the-consumer mutants fail.
2. **Redaction and boundedness.** A configuration containing sentinel environment/profile values
   leaks neither those values nor child output through human or JSON doctor output. More than 256
   incidents reports the exact total, stable pages/cursors through the final incident, and
   `truncated`, below 16 MiB even with maximum legal strings. Secret-value, first-page-starves-tail,
   and unbounded-incident mutants fail.
3. **Durable identity.** A real suspended Windows child records boot ID, PID, and creation
   `FILETIME` before resume; its creating record already contains host, boot, generation, and
   strength. Daemon identity is persisted, the canonical singleton lock excludes its exact prior
   process, and the Job handle is non-inheritable. A same-PID/different-creation fixture is not the
   recorded identity; a matching nonterminated fixture is, while a matching exited object is gone.
   PID-only, record-after-resume, path-alias-split-lock, and inheritable-job-handle mutants fail.
4. **Uncertainty visibility.** Injected cleanup timeout reaches final Job + uncertain Containment,
   retains the complete Lease, appears in doctor with its incident and blocker impact, and updates
   watch through the ordinary event path. A timeout-means-empty mutant fails.
5. **Automatic proof.** Same-daemon Job Object emptiness, prior-daemon kill-on-close plus exact-root
   disappearance with the old daemon identity absent, correctly split creating/no-root proofs, and
   a same-host prior-generation boot-ID change each clear atomically and release only the eligible
   Attempt Lease. Current-generation boot inequality, host mismatch, matching root, unavailable
   identity, and unavailable/changed-after-startup boot evidence remain uncertain/failed. A real
   Restart changes boot identity; Fast Startup shutdown need not. No-root-means-empty,
   root-gone-alone, current-generation-reboot, foreign-host-reboot, and
   daemon-generation-is-boot mutants fail.
6. **Forced safety.** An unmanaged owner can force an unprovable/root-gone incident; a managed Job,
   missing force, foreign ID, naturally empty record, matching exact root, or unauditable requester
   cannot. Clearance authentication covers primary/postcondition and runner/reconciler handles with
   one connection-time peer identity. Target identity unavailable, known-nonempty, or
   owned-but-uninspectable boundary also refuses; clearance never closes a handle as a hidden kill
   operation. PID reuse does not refuse and is recorded in probe evidence. Managed-self-clear,
   postcondition-clears-competitor, peer-PID-race, force-clears-unprobeable-root,
   force-clears-live-root, force-drops-owned-boundary, and PID-reuse-is-live mutants fail.
7. **Idempotence and concurrency.** Disconnect immediately before/after clearance commit and two
   concurrent clear callers produce either no change or one identical persisted result. No audit,
   state, Lease, or event is duplicated. Automatic resolution has no fabricated requester; a later
   force attempt returns that same automatic audit. `status` exposes both automatic and forced
   resolution history. Check-then-update, fabricated-automatic-requester, and audit-write-only
   mutants fail.
8. **Attempt-wide containment and crash atomicity.** Ordinary cleanup, automatic resolution, and
   forced clearance exercise the same store predicate. None releases an Attempt Lease until the
   Attempt is closed to new Invocations and every durable Containment whose Invocation belongs to
   that Attempt is `empty` or `cleared`, across the primary Invocation and every postcondition. A
   prior-generation blocker is included. Concurrent attempted Containment creation is serialized:
   it is included before release or rejected after settlement closes. The blocked result durably has
   `lease_released == false`; clearing the final blocker has `lease_released == true`. SQLite failure
   at every automatic and forced write boundary exposes the full prior or full new state.
   Release-before-audit, set-read-outside-transaction, current-generation-only,
   same-Invocation-only, primary-only, automatic-path-bypasses-shared-predicate,
   postcondition-created-during-release, and first-containment-release mutants fail.
9. **Wake and cost discipline.** With no incidents, doctor adds no background thread, helper
   process, or periodic wake. With incidents, each reconciliation turn probes at most 32 outside the
   store mutex, stable round-robin prevents starvation, and unchanged probes cause no SQLite write,
   fsync, event, or watcher refresh. A proof/clear that releases capacity explicitly wakes admission,
   while a slow doctor client cannot block writers. Busy-reconcile, unbounded-reconcile,
   probe-emits-event, missing-scheduler-wake, and viewer-holds-store-lock mutants fail.
10. **Greenfield and provenance.** The new epoch replaces alpha.7 SQLite only under the singleton
    lock, changes store UUID, preserves config/log files, and makes every old Containment ID foreign.
    An unbound store with pending Jobs but no Containment/granted Lease binds without reset. A store
    bound to another `HostId`, or an unbound store with containment evidence, is wholly replaced and
    receives a new store UUID. Doctor reports the new epoch, boot, host, version, generation, and
    cooperative boundaries. Partial-migration, stale-ID-clear, pending-means-unbindable, and
    foreign-host-store-kept mutants fail.

Focused fleet review must attack the public API size, machine-readable compatibility, exact process
and reboot proof, managed-caller authorization, clearance transaction, Attempt-wide Lease ownership,
reconciliation wake discipline, and whether any requirement accidentally turns doctor into a
second scheduler or policy engine. Confirmed High/Critical/Blocker findings are fixed and sent back
to the affected reviewer; Medium/Low findings may be corrected silently when they stay within this
frozen scope.

## Review evidence

The design fleet comprised two independent Claude Opus lenses, Claude Fable xhigh, and Grok 4.6
high. The first pass found gaps in host/boot and process evidence, managed-caller authorization,
creating/no-root proof, scheduler wakeup, forward-readable serde, pagination/audit readback, public
API constructibility, and host-store binding. Corrections were returned only to the affected
reviewers. Targeted closure passes then verified the Attempt-wide durable transaction predicate,
safe unknown-value round trips, externally callable client API, and whole-store reset/bind rules.

All Blocker/High findings are closed. Fable, Grok, both focused Opus lenses, and the Opus reporter
that requested the final transaction clarification returned `PASS`. This document is the alpha.8
implementation baseline; semantic changes require an explicit later design revision rather than an
implementation-only reinterpretation.
