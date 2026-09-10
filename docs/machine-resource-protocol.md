# Machine resource protocol — MR-0 design

Date: 2026-09-09. Status: design contract for the machine-resource amendment;
The full attached protocol is **not yet implemented**. The
[ledger](machine-resource-implementation-status.md) records implementation and
feasibility separately. Installed alpha.16 speaks local protocol 21 and JobSpec 4,
with store epoch `stillyard-conditions-r3-2026-09-02`. Its transitional bootstrap
is implemented and tested; that does not establish the full protocol acceptance.

## 1. Roles, ownership, and identities

One owner explicitly initializes one machine authority. Its Windows daemon
contains the coordinator and the native manager/executor. An attached WSL daemon
contains a local manager/executor and connects to that authority. Standalone
contains all three roles using the same admission core. Attached mode is durable
configuration; disconnect never selects standalone. First delivery supports one
Windows owner and explicitly paired Linux UIDs. Cross-owner arbitration is outside
this amendment.

Job placement is selected by the client endpoint. Submission, Batch, Job, Attempt,
Invocation, logs, result, dependency, and managed-parent graphs stay in the selected
manager store. The coordinator owns scarce-resource allocation. A local Lease
records the manager's obligation to an allocation; it is not a second allocator.

| Identity | Creation and binding | What a change does NOT prove |
|---|---|---|
| `machine_id` | Random UUID persisted in explicitly initialized owner authority registry, bound to OS host identity and Windows owner SID | Same hostname or copied registry does not identify another host |
| `authority_epoch` | Registry UUID, advanced by fenced recovery before opening a new coordinator store | Prior executors are not necessarily dead |
| `domain_id` | Registry-assigned UUID bound to parent domain, runtime registration identity, owner and local installation nonce | Same distro/container name does not restore authority |
| `runtime_incarnation` | Adapter's authenticated runtime lifetime observation, with provenance and coverage | A CLI listing or reconnect is not cleanup |
| `store_uuid` | Existing manager/coordinator SQLite store identity; every reset changes it | Missing old rows do not mean no running work |
| `executor_incarnation` | UUID plus OS process start identity under the domain singleton lock | New daemon does not prove its predecessor's descendants dead |
| `connection_epoch` | Coordinator-incremented counter after authenticated reconnect | Fencing messages cannot retract a previously delivered start ticket |
| `host_boot_id`, `linux_boot_id` | Platform-observed identities, separately recorded | Linux boot/distro/daemon identities are not interchangeable |

The domain tree is acyclic, at most 16 levels and 256 registered domains.
Windows host and WSL VM are distinct nodes. Distro registration uses the actual
Windows WSL registration identity, verified against an explicit distro selection;
neither `/etc/os-release` nor a user supplied name is authentication.

## 2. Durable data and reset gate

Coordinator tables are `domains`, `candidates`, `reservations`, `grants`,
`invocation_tickets`, `peer_sequences`, `operations`, `reconcile_sessions`, and
`resource_events`. Keys include the authority epoch; updates compare row version
and the authenticated domain/store/incarnation. Resource changes, grant state,
operation response, and event append commit in one SQLite immediate transaction.
All SQL values and enum discriminants are validated on load.

Manager additions are `allocation_intents`, `invocation_tickets`, `grant_outbox`,
and `peer_checkpoint`. Their updates commit with existing lifecycle rows. Each
table has one owner; no transaction spans both databases and no network call runs
under a store mutex. SQLite uses the existing crash-safe settings.

Allocation key:

```text
(machine_id, authority_epoch, domain_id, manager_store_uuid, lease_id)
work Lease -> exactly one Attempt -> primary and its ordered postconditions
probe Lease -> exactly one probe Invocation
ticket key -> (allocation key, invocation_id, release_sequence)
```

`GrantId` is coordinator-store-qualified and the allocation key is unique. A
renewed Offer after a safe deferral has a new offer nonce. A retry uses a new
Attempt/Lease; a new probe uses its own Lease. No key can transfer to another store.

The owner-only authority registry is **outside SQLite and its reset deletion set**.
It stores machine/owner binding, initialized marker, authority epoch, coordinator
store UUID, registered participants, affected resource scopes, and a pending-reset
gate. The attached installation has an owner-only pairing anchor outside its
manager SQLite. Both use write/flush/atomic publication with directory durability
on the supported filesystem. Secrets never appear in public evidence.

Before opening admissions, startup takes the authority/endpoint locks and validates
registry and store continuity. On any mismatch it durably publishes the reset gate
*before* resetting SQLite or changing its UUID. A crash on either side of that
publication still starts gated. A new SQLite database receives no implicit free
capacity. All registered domains, including disconnected ones and Windows native
work, must reconcile before affected resources are usable. Initial implementation
may gate the entire machine conservatively.

Missing/corrupt registry, lost pairing anchor, foreign host binding, or unexplained
rollback means `history_unknown`; startup is inspect-only. It MUST NOT generate a
replacement authority automatically. Explicit first initialization requires an
owner action and an empty-work inventory. Reinitialization with lost history
requires audited risk clearance, not an installation retry. A local store reset
does not erase remote grants; the new store cannot claim the old store's releases.

## 3. Wire and trust

Selected design: one persistent Windows `stillyard.exe bridge` process per attached
installation, reached through WSL interop stdin/stdout. It connects to the Windows
owner-only named pipe. Linux clients use an owner-only Unix-domain socket with
`SO_PEERCRED` and process start identity. There is no IP listener. Bridge lifetime
has no resource-release semantics. Feasibility and interop containment remain
MR-0/MR-3 gates; selecting this design does not assert their success.

Pairing is explicit on both installations. The Windows pipe authenticates SID,
instance and executable role. A 256-bit pairing secret binds Linux installation
nonce, UID, domain and machine. Session challenge/response uses HMAC-SHA256 over
both random 256-bit nonces, both installation identities, protocol versions and
connection epoch. A replayed transcript fails. The bridge is trusted for this
owner, but a different isolated instance, runtime-adapter role, or domain is not.
Same-owner hostile tampering remains outside R-SCOPE-2. Capability to report local
cleanup covers only the registered domain; runtime-wide proof needs adapter role.

Machine wire version is `1`, independent of existing local protocol numbering.
Framing is a 4-byte little-endian byte length followed by strict UTF-8 JSON, maximum
1 MiB/frame, 4 MiB queued per connection, 64 in-flight requests. Reject unknown
fields/versions and oversized lengths before allocating the body. No compression.
Hello negotiates exact supported version and capabilities; mismatch permits only
version diagnostics, never allocation. Local API/schema versions must be bumped
when their Rust public types change; MR-0 documentation alone does not bump them.

Every authenticated request carries `machine_id`, `authority_epoch`, `domain_id`,
`manager_store_uuid`, `executor_incarnation`, `connection_epoch`, monotonically
increasing `request_sequence`, operation UUID and normalized payload hash. Replies
echo them with coordinator revision and tagged `ok`, `stale`, `conflict`,
`unavailable`, `unsupported`, `history_unknown`, or `limit_exceeded`. Exact replay
returns the recorded outcome; changed payload conflicts; a forgotten sequence never
becomes a fresh operation. Each operation additionally carries an HMAC-SHA256 tag
under the pairing secret, with the direction-specific `stillyard-machine-operation-v1`
context binding version, full session, sequence, operation UUID and payload hash.
The coordinator verifies this before reading or changing the operation sequence;
public session IDs and possession of the installed CLI are insufficient authority.
Query and retry are connect-only.

| Operation | Preconditions and durable result | Lost reply |
|---|---|---|
| `Register/Pair` | Owner authorization; registry flushed before participant can request work | Query installation nonce; no second domain |
| `Connect` | Matching anchors; advance connection epoch and fence old writers | Reconnect with fresh challenge; query current epoch |
| `CandidateUpsert` | Increasing candidate revision; immutable claims hash and original queue identity | Query key/revision; exact retry |
| `Withdraw` | Mark revision withdrawn; revoke unarmed offer/reservation | Query state; an Arm that won first remains held |
| `Offer` | Atomic complete-vector reservation/conversion into bounded offer | Query allocation key; offer itself permits no launch |
| `Arm` | Matching unexpired offer/config/candidate; persist potentially-used grant | Query/retry same operation; never infer unarmed from no reply |
| `AuthorizeInvocation` | Armed grant, fresh readiness, current session, ordered new Invocation | Query ticket; recovery never blindly launches it |
| `CancelCandidate` | Suppress future offers/tickets; retain all potentially-used rights | Query stop intent; local manager owns Job cancellation |
| `Release` | Sealed manager intent and complete cleanup proof for all tickets | Retry outbox entry until durable released ack |
| `ReconcileBegin/Page/Commit` | Session fence, ordered pages, continuous sequence and complete digest | Resume same snapshot or restart reconciliation |
| `Inspect` | Public projection with source revision and freshness | Return stale/unavailable explicitly |

Coordinator-issued offers/tickets are authenticated replies tied to that session.
Connection epoch fencing rejects mutations from an old bridge. It never erases
previous tickets or independently proves cleanup.

## 4. Start, cancellation, and cleanup ordering

Grant state is `offered -> armed -> released`, with `uncertain` retaining armed
accounting. Only an unarmed `offered` grant can expire. `released` is terminal.
Candidate and reservation state are separate from the grant lifecycle.

1. Manager persists Lease/allocation intent and ready candidate revision, then
   advertises the candidate. It does not create an independently granted scalar
   Lease. Standalone uses the same decision inside one local transaction.
2. Coordinator offers the whole vector in one commit. Manager persists the offer
   identity and **Arm intent before sending Arm**. Arm's coordinator commit is the
   point after which TTL, heartbeat loss, and disconnect cannot free the vector.
3. Manager records the armed response. It records Invocation/Containment creation
   intent, prepares a born-contained process behind an OS barrier, and records its
   actual root/executable identity. A creation failure still requires empty proof.
4. Manager rechecks conditions and cancel state, then requests an Invocation ticket.
   Coordinator samples/rechecks host readiness, config and exclusions and commits
   the single-use ticket before replying. Grant impacts already exclude competing
   managed work. A ticket is bound to the precise Invocation, executable identity,
   offer/config mappings, session and evidence challenge.
5. Manager commits `release_intent` before user code. Under the local cancellation
   boundary it rechecks ticket freshness, local evidence, no disconnect/resume
   notification and no durable stop selection, then releases the OS barrier once.
   Local process release is the start/cancel ordering point. Its result is recorded.
   Recovery after any uncertain release gap interrupts and cleans; it never repeats
   the release based on missing `started` acknowledgement.
6. Root exit is recorded independently of empty proof. Cleanup stops descendants,
   drains output and proves the recorded boundary empty. Primary cleanup does not
   release the work Grant if postconditions remain. Each postcondition has a fresh
   Containment and ticket; no postcondition starts offline. Probe Grant is separate.
7. When no Invocation can start again, manager atomically seals allocation intent,
   records all cleanup evidence and enqueues `Release` with its sequence. Sealed
   intent cannot be reopened, including on retry of an old ticket. The Lease remains
   visibly release-pending until the coordinator commits released. Lost ack is fixed
   by replay. Manager commits ack and Lease release together.

Cancel first commits in the owning manager, even when offline. It prevents local
release immediately, suppresses retries and drives local cleanup plus durable
withdraw/cancel outbox. A concurrent Arm can win on the coordinator; that grant is
then reconciled and released after the manager's sealed no-start proof. Coordinator
receipt alone is never reported as completed Job cancellation.

Freshness uses a manager monotonic challenge timestamp, response round-trip upper
bound and separately sampled local evidence. Host sample must be taken after receipt
of that challenge. Accept a ticket only within the lesser of configured sample age
and 250 ms measured from the manager's challenge send. Reject suspend/clock/provider
generation discontinuities. No direct comparison of Windows and Linux clock values.
Host quiet stability is maintained on the host; local quiet stability is maintained
locally. Their stable intervals must both satisfy policy. Connection failure known
before release prevents release; an undetectable failure after the last check can
coincide with a still-fresh authorized launch. Its armed Grant already retains the
vector. The contract does not promise instantaneous partition detection or policing
unmanaged OS background activity.

## 5. Recovery, snapshots and bounded history

On reconnect, fence the old connection first. Recovering executor may report old
work, but cannot start new work until recovery completes. A replacement executor
must prove the predecessor root dead using OS identity and inspect every recorded
Containment; singleton ownership alone does not prove descendants gone.

A reconciliation snapshot is `(session, manager_store_uuid, begin_sequence,
end_sequence, page_count, digest, config_revision)`. It includes **all** allocations,
unacknowledged operations, issued/consumed tickets, live or uncertain containments
and sealed releases. Pages contain at most 256 records; final digest covers ordering
and full content. Manager freezes new starts/ticket requests during snapshot cut,
then streams outside its mutex. Concurrent completions are subsequent ordered
outbox entries. Coordinator compares its last accepted sequence with the snapshot
and every ticket it issued, then records the watermark atomically. Missing pages,
sequence gaps, a changed store or absent expected grant mean uncertainty. An absent
row in an arbitrary snapshot is never empty proof.

Ordinary reconnect/released-ack loss automatically converges. Reset requires
inventory reconciliation of every registered potentially-used grant. If manager
history was lost, use registered boundary identities, predecessor-death proof and
adapter inventory covering outstanding rights; no proof means retained gate until
audited operator clearance. A runtime stop report is accepted only with the proof
coverage in section 8. The protocol deliberately chooses conservative unavailability
over a fabricated free budget.

Retain every live/uncertain grant/ticket/outbox operation without expiry. Maximum
65,536 unresolved grants per machine, 4,096 per domain and 16,384 queued outbox
entries per domain; reaching a limit blocks new starts, never deletes obligations.
Completed per-operation rows may be compacted only after bilateral acknowledgement
through a contiguous sequence watermark. Keep durable retired sequence floors,
retired store epochs and domain anchors; requests at/below a compacted floor receive
`history_unknown`/`released_through_watermark`, never create work. Identity anchors
are bounded by registration limits; removing an installation requires empty proof
and explicit decommission. The implemented completed event history retains at most 4,096 events / 16 MiB;
a pruned public cursor returns Gap. This bounded projection does not retire Grants. Client timeout never advances a floor.

## 6. Resource accounting and common queue

Canonical scalar identity is `(scope_id, kind, resource_id)`; a token name is
resolved by a registry mapping, not by caller spelling. Machine `cargo_slots` is one
resource in both native and attached requests. Fences default to domain scope;
cross-domain fences require a registered filesystem-object alias mapping. GPU
aliases resolve to one physical device ID and one VRAM budget. Unmapped or ambiguous
resources reject; capacity zero is zero, never unlimited.

For request physical debit `q[r]`, expand one constraint entry for each applicable
ancestor budget. Each entry debits that constraint once; do not sum these entries
again as machine physical usage. Reject overflow. For every constraint:

```text
granted[r] = sum(potentially-used grants' claims[r])
offered[r] = sum(unexpired unarmed offers' claims[r])
fits = claim[r] <= capacity[r] - granted[r] - offered[r]
```

Scalar reservations retain the existing R-RES-6 semantics: their complete vector
may overlap current grants, total reservations alone cannot exceed capacity, an
unreserved candidate subtracts reserved headroom, conversion is atomic and observes
higher-ranked overlapping reservations. Offers consume headroom and appear
separately from reservations. No fence, impact or scalar is acquired partially.
Capacity reduction does not preempt; over-capacity active usage blocks new claims.
Old config/mapping tickets cannot authorize a new launch. Normalize reservations
and cancel unarmed offers transactionally when the config epoch advances.
Changing a mapping first closes ticket issuance and drains or seals all outstanding
start rights against that mapping. Only then may the new mapping become effective;
incrementing an epoch alone cannot retract a delivered ticket. Existing running
grants retain their original canonical resource identities and debits. A pending
capacity decrease is visible while old rights settle and never invents headroom.

Example: machine 16 CPU/32 GiB, WSL VM 8 CPU/12 GiB, distro 6 CPU/10 GiB.
A WSL 4 CPU/6 GiB request uses 4/6 in each relevant constraint, not 12/18 on the
machine. Two such requests fit machine and VM but fail the distro CPU limit.
A Windows 8 CPU/8 GiB request can coexist with the first WSL request (12 CPU/14 GiB
machine usage). GPU aliases `host-card0` and `guest-card0` mapped to a 16 GiB card
cannot each allocate 10 GiB. With two cargo tokens, two otherwise compatible Jobs
must run concurrently; whole-machine serialization is only a bootstrap fallback.

Observed RAM is a further conservative gate. Host physical/commit headroom is
sampled once on Windows and guest availability once in Linux; they are not added.
Without verified attribution of already resident managed memory, debit all armed
claims again from fresh observed headroom plus safety margin (conservative double
debit is visible). A future attributable-resident optimization must document its
formula and negative control before use. vmmem is part of host observation, not a
second physical pool. Default margin is 512 MiB at host and VM constraints.

Coordinator sees every ready candidate, maximum 4,096/domain and 65,536/machine.
Backpressure is explicit and fair per-domain transport servicing cannot hide an
already-advertised candidate behind a domain head. Pending local dependencies and
conditions remain manager-owned. Withdrawal carries increasing revision; stale
upserts cannot restore readiness. Accepted priority is immutable `-3..3`.
The attached manager limits accepted nonfinal Jobs to its candidate budget at
submission, including currently unready Jobs; an oversized Batch rejects atomically.
It does not accept an unbounded hidden ready queue and forward only its first head.

Aging for attached work starts at first durable coordinator registration of its
queue identity; an offline manager cannot backdate authority time. All Attempts
retain that first sequence/time. Sort uses the existing capped 60-second aging
formula, then coordinator acceptance time and global sequence. Native submission
registers in its acceptance transaction. Coordinator clamps scheduling wall time to
the last durable scheduling time across rollback; endpoint-local clocks never
determine cross-domain order. This intentionally amends the cross-domain origin of
R-RES-6, while standalone retains its acceptance order.

Scan the entire ordered set. Candidate ready advertisements expire after 30 s;
batch refresh at most once per 10 s while candidates exist. Expiry withdraws only
unused rights, not armed grants. Offers expire after 5 s; scalar reservations after
60 s with 5 s durable yield. Readiness deferrals retain existing finite limits and
backoffs, including across restart. Disconnected stale candidates cannot hold the
queue forever. Idle connections use OS event waits without heartbeats.

## 7. Failure traces and required negative controls

Each trace is a required executable scenario through the public path in MR-2,
repeated against real Linux containment in MR-3. These are specifications, not test
results. `M-A` and `W-C` IDs are defined by the phase plan.

| Crash/race boundary | Required recovery | Negative control |
|---|---|---|
| Before manager intent | No rights, safe new allocation only under ordinary Job policy | Unrecorded allocation |
| After intent, before Offer | Re-advertise same candidate | Duplicate candidate key |
| Offer issued, reply lost | Query; safe expiry if unarmed | Offer grants launch authority |
| Arm request/reply lost | Query exact operation; held if committed | TTL frees armed grant (M-A04) |
| Containment created before local root commit | Inspect recorded boundary; no user release | Missing root means empty |
| Ticket issued, reply lost | Held until sealed no-start/cleanup proof | Missing local ticket releases grant |
| Release intent committed before/after OS release | Interrupt/clean, never resume again on restart | Replay launch (M-A04) |
| Cancel races Arm or OS release | Local ordering selects no-start or cleanup; keep allocation meanwhile | Cancel response erases rights |
| Primary exit with live grandchild | Kill and prove tree empty before postcondition | Root-only proof (M-A08/09) |
| Release commit, ack lost | Same outbox sequence gets same ack | Permanent token leak (M-A05) |
| Old/new connection overlap | Old mutations rejected; old tickets still covered by accounting | Epoch change implies cleanup (M-A07) |
| Coordinator SQLite reset | Registry gate covers Windows and every guest | Empty DB admits native build (M-A06) |
| Guest reset or pairing history loss | Remote obligation remains; inventory/clearance | Unknown guest is empty (M-A06) |
| Mapping/capacity decrease while offered/armed | Revoke offer; block new tickets; retain used grants | Alias makes free GPU (M-A13) |
| Suspend between quiet sample and release | Invalidate ticket/evidence; clean never-run boundary | Stale quiet release (M-A11) |

## 8. Platform proof and WSL bootstrap gate

| Event/evidence | Windows native | WSL Linux |
|---|---|---|
| Client/bridge exit, lost heartbeat, timeout | Not root/tree proof | Not root/tree proof |
| Daemon restart, old process identity absent | Existing recorded kill-on-close proof within its boundary | Must inspect recorded cgroups; daemon death insufficient |
| Root PID absent/reused | Does not alone prove descendants | Does not alone prove descendants |
| Recorded boundary inspected empty | Windows Job accounting under valid handle | Recorded cgroup identity + `populated=0`, no outstanding start rights |
| cgroup missing/recreated/uninspectable | Not applicable | Uncertain, unless independent proof covers old runtime |
| `wsl --terminate` reports success / distro absent from running list | Does not affect host children | Diagnostic only until verified adapter lifetime proof |
| VM shutdown / Linux boot change | Does not prove Windows children dead | Covers old Linux processes only with authenticated VM/boot continuity and fenced old executor |
| Windows host reboot, same machine, prior boot verified | Prior-boot processes gone | Old VM processes gone; pairing/history continuity still required |
| Logout / WSL suspend/resume | No blanket cleanup inference | Invalidate evidence, reconnect and inspect; no inference from linger |

Supported Linux execution requires a persistent user service, delegated cgroup v2,
record-before-create, born-contained launch, pidfd identity and an OS barrier. The
service owns a supervisor cgroup and persistent per-Invocation leaves. No
process-group fallback. Local store is ext4 within the WSL filesystem for the first
profile; DrvFS is not a supported manager store. Future native Linux and containers
need independent acceptance.

Normal Windows interop is enabled on the surveyed distro. Unsetting `WSL_INTEROP`
is not a proof that execution cannot escape Linux. A supported Linux-only execution
profile must prevent ordinary PE/interop launches within the Invocation boundary,
or use a verified adapter covering both OS trees. Until that profile is implemented
and negatively tested, its containment capability remains unavailable.

**Bootstrap decision:** reject ordinary `wsl.exe`-only containment as a Cargo path.
The acceptable development path needs a Windows-authority-owned durable bootstrap
obligation, written outside resettable job history before Linux release. It gates
the complete claimed machine vector after intermediary/daemon/store loss and is
cleared only on an authenticated sealed Linux boundary-empty report. Installing
that authority guard must itself use a native Windows system Job build and preserve
existing work. A shell promise to wait, a second independent counter, a timeout,
or editing only the submitted JobSpec is insufficient. Alpha.14 lacked this guard;
installed alpha.15 implements the transitional mechanism below. The ledger records
the passed failure controls and the first protected Linux compiler Jobs.

Before any Linux Cargo, the guard/harness must pass: correct explicit distro/UID,
clean environment/cwd, canonical stdout/stderr, exit 0 and exit 25, timeout, cancel,
intermediary loss, authority restart/reset and retained allocation until Linux tree
empty. Fault cases use isolated objects inside a default system Job. Each run has
a manifest, actual Job ID and verified no-leftover-process result.

### MR-0 transitional bootstrap implementation contract

The temporary native entry point is `stillyard bootstrap run --spec <descriptor>`
as a primary of a native Windows Job. It is not an attached-domain Job submission
API. The native primary owns its ordinary Lease; the extra durable authority hold
survives closure of that Windows containment and conservatively blocks all new
admission. This serialization is specific to bootstrap and cannot satisfy M-A02.

`BootstrapArm` binds the OS-authenticated current primary/root identity, local
store/Attempt/Invocation IDs, explicit WSL distribution and user, command/environment,
and SHA-256 of the serialized descriptor. The operation ID is derived from the
Invocation ID, not supplied by an unauthenticated shell. Under the same Store mutex
as local admission, Arm requires the caller's sole work Lease and no other granted
Lease, persists outside SQLite and returns before the native primary invokes WSL.
An existing released operation is a tombstone; a replay cannot launch another command.

Only the executable currently running the selected daemon can attest bootstrap
Arm/Seal. The daemon obtains its peer process/image from the named pipe and OS,
and additionally compares Arm to the current primary root. Public administrator
force clearance remains distinct and cannot be used by a managed process.
This trust boundary is one owner and the installed bridge; it is not isolation
against a malicious account owner replacing files or injecting into its own processes.

The installed native executable embeds the Python supervisor. It publishes a
single-use intent under the selected Linux owner's ext4 state directory, starts a
delegated systemd user unit, and saves a claimed-operation record before any user
release. The child stub starts within the unit containment and waits on a pipe.
The supervisor creates and records an exclusive work cgroup, attaches the stub,
verifies membership, persists release intent and writes the sole release byte.
Parent death before that byte closes the barrier. The child then enters the tested
bubblewrap profile with private PID/mount namespaces, masked `/init` and `/run`,
and read-only cgroup access, before executing the requested program with explicit env.

Root exit, deadline, cancellation or intermediary liveness loss triggers
`cgroup.kill`. Liveness only requests cleanup. It never proves empty. The supervisor
waits for `populated=0`, verifies the recorded cgroup inode, removes that exact leaf
to seal further attachment, flushes stdout/stderr and atomically publishes a
checksummed proof bound to the operation, descriptor hash, Linux boot, UID and cgroup.
Normal output is forwarded through bounded native bridge frames to the canonical
Windows streams. After intermediary loss the durable Linux streams remain evidence.

`bootstrap reconcile <operation-id>` reads the authority's retained binding and
uses the same embedded inspector. It checks the durable intent, claimed record,
boundary identity and proof checksum; absence, corruption or an incomplete operation
remains uncertain. `BootstrapSeal` idempotently retires only that obligation and
retains its proof. An unmanaged installed bridge may perform recovery after the old
native primary or coordinator store is gone. A managed bridge can only seal its own
operation. No missing-process, missing-unit, TTL or arbitrary guest snapshot release
is implemented. Failure before a seal can require explicit operator recovery.

The native IPC revision is 20. JobSpec remains 4, HostConfig remains 2, and this
bootstrap slice does not reset the existing SQLite epoch. The standalone registry
is version 1, capped at 4 MiB and 1024 retained operations; reaching either limit
blocks new bootstrap work rather than evicting an obligation. The Linux manager and
full MR-2 Grant/outbox protocol remain separate implementation work.

WSL lifetime remains externally provided. User manager availability and delegated
write access are prerequisites, not logout/VM lifetime evidence. Current `Linger=no`
and no explicit keepalive configuration do not close R-LINUX-5. Provisioning and
VM-stop acceptance must account for other active work before any disruptive action.

The platform assumptions are grounded in the [kernel cgroup v2 contract](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html),
[WSL configuration reference](https://learn.microsoft.com/en-us/windows/wsl/wsl-config),
and [WSL systemd setup](https://learn.microsoft.com/en-us/windows/wsl/systemd).
These documents do not substitute for local cleanup evidence.

## 9. Public surfaces, compatibility and installation

Planned public types: authority/domain/grant/ticket IDs, scoped resource snapshots
with capacity/granted/offered/reserved, allocation key, local Lease binding,
config/connection epochs, reconciliation state/watermark, stale age and blocker
reason. Inspect and events explain the Windows allocation delaying a WSL Job.
Job finality and Grant release are separately visible. Missing authority observation
is `unavailable` or last-known `stale`, never zero. CLI/TUI use public APIs only.

`doctor` must expose role, peer/owner binding, executable installation path,
scope/alias coverage, reset gates, cgroup/interop/keepalive capabilities, evidence
age and concrete reconciliation/clearance action. Clearance identifies all affected
grants and outstanding tickets and records operator identity plus explicit risk
acceptance; it never silently deletes a registry.

MR-1 public type additions require a local protocol version and schema fixture
change, with explicit rejection by older peers. JobSpec remains version 4 only if
its accepted wire shape is unchanged; adding scoped claims requires version 5.
MR-2 uses the separately versioned additive `machine_*` extension, schema version 1,
while preserving the baseline Job-store epoch and its existing queues. Older
binaries reject the extended authority registry; downgrade is prohibited. A future
incompatible extension change requires an explicitly named epoch/reset decision.
No baseline reset is required by this additive change. A generated schema change must
be produced by a `schema-update` system Job. MSRV remains Rust 1.85.

Upgrade procedure: inspect both installed daemons and queue/uncertainty; stop new
admissions using a verified maintenance gate; let accepted work settle, including
managed children, and reconcile all grants; stop only owned empty instances under
singleton protection; archive old stores/config/logs and registry consistently;
publish a reset gate before any necessary epoch reset; install binaries outside
target directories; start coordinator inspect-only, then attached managers; verify
versions, identities and full reconciliation before opening admissions. If drain or
an admission gate is absent, do not pretend a one-time empty snapshot is race-free.
Old alpha.14 has no drain/force administration API; maintenance gating is required
before an unattended replacement. An older binary cannot be allowed to bypass the
new registry; downgrade while paired/uncertain is rejected by the installer. Never
reset foreign queued Jobs or restore selected rows from incompatible schemas.

MR-3 idle budget, measured for five minutes with no Jobs or subscribers: each
daemon below 0.5% of one logical CPU and 40 MiB private/RSS-anonymous memory excluding
mapped SQLite pages; bridge below 0.1% and 16 MiB; aggregate below 1.1%, 96 MiB and
six timer wakes/minute. Native standalone retains A-19 unchanged. Wakes are measured
per process and summed; active candidate/evidence sampling is reported separately.
The explicit bridge exception amends A-19's no-helper condition only for attached
mode. A polling mutant must fail.

## 10. Consumers and acceptance artifacts

W-C1 uses the native and Linux `check`/`test` launchers against the same
`source-manifest.json`. Run with one then two shared cargo slots and inspect complete
grant/start/release events, not sampled CPU load. W-C2 uses the installed Claude/Grok/
Codex CLI with a recorded explicit profile, route and requested/actual model on this
protocol/diff. Its validator checks a real nonempty verdict, model usage, final
result and postcondition; a stub verdict or canceled/rate-limited run is not pass.
W-C3 uses an authenticated managed adapter to `ensure` a child build in a separate
scratch snapshot while Windows holds the machine token. Exact recovery never
creates a second build; parent has no cargo claim and does not fence the child's root.

W-C4 measures repeated SHA-256 traversal of this repository's manifested source
bytes (256 rounds) with a shipped measurement script, recording source digest,
bytes, wall duration and CPU duration. Success requires all 256 rounds to match
the precomputed digest, positive byte count and finite positive durations. Timing
values are measurements, not a fabricated performance threshold. The Job declares
`measurement`, CPU quiet stability and freshness; Windows/WSL build Jobs declare
incompatible `cpu_heavy`. Deliberately stale host evidence or overlapping incompatible
grant must cause the validator to fail. The shipped `scripts/machine-resource-consumer.py measure` implements the command.
Its fixture controls are separate from live W-C4 scheduler/evidence acceptance.

Each round stores exact specs, source/config/executable hashes, installed versions,
authority/domain/store/Grant/Attempt/Invocation IDs, canonical log chunks/events,
typed outcomes, postconditions and proof. Three live rounds include client loss,
reconnect and daemon restart between rounds. Fault harness results and consumer
results occupy separate ledger columns. Missing adapters/capabilities are `not_run`.

### Selected first-delivery consumer profiles and exact operations

The selected live review profile is installed `~/.local/bin/claude` 2.1.266,
`CLAUDE_CONFIG_DIR=$HOME/.claude2`, first-party `claude.ai` subscription OAuth,
requested model `sonnet`; actual model IDs must be reported by `modelUsage` and
start with `claude-sonnet-`. Discovery confirmed logged-in Max subscription status.
This is a concrete selected available CLI, not a requirement to install a `fleet`
binary. Grok/Codex discovery does not count as a passed consumer run. Additional
backends may be added only with their own real result validator and explicit route.

For every review Job, use `/usr/bin/python3` with these argv (all paths expanded to
absolute paths in the retained JobSpec):

```text
scripts/machine-resource-consumer.py review
  --cli $HOME/.local/bin/claude --brief $ROUND/brief.txt
  --output $ROUND/claude.raw.json --model sonnet
```

The brief contains the identified source diff and the protocol under review. The
adapter invokes `claude -p <brief> --model sonnet --output-format json --safe-mode
--tools "" --setting-sources ""`. It checks auth route without logging credentials,
retains executable/brief hashes and real output, and exposes stderr as canonical
Job stderr. The review Job has `custom.claude2_slots=1`, no `cargo_slots`, explicit
HOME/CLAUDE_CONFIG_DIR/PATH, and a 1800-second timeout. It uses no shell, editing,
other agents, or hidden toolchain work. Postcondition argv is:

```text
scripts/machine-resource-consumer.py validate-review
  --input $ROUND/claude.raw.json --expected-model-prefix claude-sonnet-
```

Only exit 0 is accepted; malformed/error/canceled/empty/wrong-model envelopes fail
without retry. An actual review that finds defects is valid consumer output, with
`verdict=findings` and nonempty findings. It does not mean the reviewed code passed.
Fixture validator tests cannot be substituted for live CLI results.

W-C3 parent uses the same adapter's `managed-build --cli $INSTALLED_LINUX_CLI
--spec $ROUND/child-check.json --operation scratch-check --evidence-directory
$ROUND/child`. The child Job runs the exact installed Linux gate command against a
separate pre-created scratch snapshot, `cargo_slots=1`, `cpu_heavy`, and an exclusive
scratch fence. Parent has no cargo claim and no fence overlapping scratch, with
explicit child policy permitting that build. The adapter derives the operation UUID
from Job/Attempt/Invocation plus operation name, then uses existing `ensure --wait
--passthrough --idempotency-key ... --result-file ...`. Re-execution in the same
Invocation recovers exactly the same submission; unknown never authorizes a second
build. Actual parentage comes from peer authentication, not these environment names.

W-C4 argv is `scripts/machine-resource-consumer.py measure --repository-root
$SNAPSHOT --source-manifest $MANIFEST --output $ROUND/measurement.json`. It hashes
the actual source bytes 256 times and checks each file against the manifest on each
round. It publishes real wall/CPU times and total hashed bytes. Job claims
`impacts=[measurement]`, no cargo token; quiet policy selects CPU, stable_seconds=2,
max_sample_age_seconds=1, wait_budget_seconds=120. Host/local ticket freshness is
still bounded by section 4's 250 ms, so the spec's one-second value cannot weaken it.
The coordinator config explicitly makes `measurement` incompatible with `cpu_heavy`.

W-C1 runs native `scripts/run-stillyard-job.ps1 check/test` and the installed WSL
launcher `check/test` on exactly matched manifests. W-C1..4 each run three rounds;
round two drops a disposable client and reconnects, round three follows an owned
empty daemon restart. Concurrent native/WSL build controls use capacities 1 then 2.
Configuration changes use versioned public APIs, restore the prior configuration,
and preserve foreign work. Complete Grant/Invocation events establish exclusion or
actual overlap. Script success without those events is not W-C1/W-C4 acceptance.

### MR-2 implementation decisions: replacement inventory and acknowledgement

The alpha.17 candidate provides explicit `machine recover` on the selected
coordinator endpoint. With continuous external history and native coverage for the
displaced SQLite UUID, it rebuilds the machine extension from registered domains,
Armed/Uncertain Grants and every issued ticket. It preserves old allocation keys
and Grant IDs while the global gate stays closed. Lost response history receives
an explicit retired floor; the one pending external commit retains its exact
response. Reconnect must cover every restored allocation, including sealed cleanup.
An empty participant snapshot cannot remove a recorded right.

Recovery checks exact Windows creator/root process identities outside the Store
mutex, then compares the full external snapshot before its final commit. It needs
positive predecessor death / PID reuse or same-host prior-boot evidence for every
native permission, completed manager reconciliation, no outstanding attached
Grant, no pending external commit and no active maintenance/bootstrap hold. It
writes the completed native inventory audit before publishing a new authority
epoch and binding the replacement coordinator. Old session epochs then fail.
This is safe recovery, without risk acceptance or lost native Job reconstruction.
Same-UUID SQL rollback, missing/corrupt registry, uncommitted pairing and histories
without native coverage remain gated and are not claimed recoverable by this path.

`Acknowledge { through_sequence }` attests that the paired manager has durably
applied every response through a contiguous watermark. The coordinator journals
the operation, commits its SQL retired floor and response deletion, then commits
the external floor. Crash recovery completes either side before another mutation.
Only operation responses are compacted; Grants, issued tickets, sealed releases,
retired allocation identities and pairing anchors remain. A request at/below the
floor without a retained exact response returns history_unknown, never new work.

### Implemented manager inventory recovery (MR-2)

Manager extension schema 2 adds durable abandoned-intent and staged-inventory
records. A continuous manager store can bind a new authenticated recovery session
when the coordinator has retired a lost response, or after safe authority epoch
rotation. This does not apply an invented response: superseded outbox commands are
retained for local lifecycle reconciliation. New starts and advertisements remain
fenced while ordered `InspectPage` responses are staged. Counts and UTF-8 bytes
are bounded; a changed cursor/configuration or missing local start intent rejects
import. An imported Invocation intent is a cleanup obligation, never a recreated
InvocationTicket. A consumed local ticket retains its durable consumption bit.

Only a complete authenticated inventory can acknowledge the absence of a locally
sealed allocation. An absent unsealed allocation remains an error. The manager
builds the full ReconcileBegin/Page/Commit snapshot from the imported obligations
and its local cleanup seals. A successful ReconcileCommit lifts the local recovery
fence. Caller-owned lifecycle transactions still commit together with every local
protocol transition, and the platform must prove whole-boundary cleanup.

The manager journal checks original payload hashes before signing retries. Unknown
journal versions and serialization changes fail closed; installed attached stores
do not yet exist, so extension schema 2 is an explicit pre-installation change.

### External commit storage and retirement compaction (MR-2)

The 4 MiB external registry stores a checksummed reference to a separately bounded
32 MiB immutable machine-commit blob. The blob is flushed before its pointer is
published; SQL commits after pointer publication, and finishing the external
commit precedes the response. Missing/corrupt referenced blobs retain a closed
history gate. New obligation growth reserves 1 MiB for recovery/retirement,
and additionally checks that 16 KiB per live participant remains available for
bounded retirement metadata and 64 KiB per live bootstrap/maintenance hold for
its future release record, alongside a 256 KiB commit reserve. Every growing
encoded-plus-reserved footprint is checked against the hard cap, including
changes that do not add resource claims. `authority status.storage_budget`
reports the actual encoded bytes, hard cap, recovery reservation and new-admission
headroom; byte bounds can bind before the larger record-count limits;
capacity is preflighted under the SQL command savepoint and rejected durably before
business state commits. Existing release inventories are not forced into the same
space as retained obligations. A retired blob is removed only after its replacement
pointer is durable. Generated unreferenced blobs are safe to collect after loading
a valid authority; unknown authority history does not trigger that collection.

External retired-allocation hashes duplicate full Released records in continuous
coordinator SQL. Exact matching SQL records permit compaction of that external
duplicate. Loss/rollback of the bound SQL instead closes admission; completing
reset recovery rotates authority epoch, fencing every old allocation key before
new work is possible. No sequence acknowledgement alone erases that protection.
The original coordinator queue acceptance time and sequence are retained in each
Grant; rebuilding inventory groups allocations by original local Job owner and
restores that age, preserving the original queue identity in public evidence.

### Audited failed-manager retirement (MR-2)

The owner calls `machine clearance-preview <domain>` and submits
`machine retire-domain --spec <request.json>`. The request requires a stable
operation UUID, domain, exact preview SHA-256, reason and `accept_risk` boolean.
Outstanding Armed/Uncertain rights require true. Empty interrupted registrations
can be retired with false. The coordinator records the actual Windows peer SID
and process identity. The Store lock covers recomputing the complete inventory,
checking the digest and publishing an external identity fence. The separate
pending retirement slot retains all resource debits until SQL retirement commits.
Startup completes that durable decision before native admission. No cleanup proof
is fabricated: SQL/event `risk_clearance` names the audit operation and the Grant
has no SealedRelease. Immutable `domain-retirement-<operation>.json` preserves the
full preview and risk decision; commit-blob collection never removes that file.
Identical request replay returns its original receipt. Old domain, installation
nonce and manager store UUID cannot pair or issue requests again, even after an
authority epoch rotation. Retiring a parent requires retiring its registered
children first. Other participants and native allocations retain their obligations.

Retired identities remain in the bounded registry; they do not consume the live
topology count. New registration stops at the retained identity/byte budget,
while already admitted participants have reserved retirement space. Neither an
ordinary reconnect nor a new epoch deletes these fences.

### Same-UUID rollback recovery (MR-2 follow-up under validation)

For covered native history, `machine recover` can rebuild the machine extension
after an accepted-sequence/Grant rollback while retaining every native Job row.
It cannot do so with unfinished native Jobs, granted native Leases or unresolved
SQL containment. An operator must explicitly cancel the named unfinished Jobs
using normal `cancel`, and resolve their containment, because rollback may have
resurrected previously executed work as pending. Recovery never silently cancels
or resubmits it. All attached participants still reconcile or undergo explicit
retirement. Before rotating the epoch, native quiescence is checked again and OS
proof must cover the recorded coordinator and every external native permission.
If detection pinned the currently running daemon, restart it to establish the
required predecessor-death proof. Missing native coverage or missing/corrupt
authority history remains gated and cannot use this repair.

If a pending retirement encounters a divergent SQL projection, startup retains
its fence and resource debit, records a reset gate and remains inspectable.
Explicit recovery reconstructs that projection from continuous external authority
and the exact immutable retirement audit, preserving unrelated receipts. It then
finishes the already accepted retirement. Missing audit files instead make the
authority unknown and diagnostics name the missing path; restore the exact file
and restart, without a new risk decision or replacement authority.

After full reconciliation and release of all local allocations, the manager's
caller may acknowledge explicit archived operation IDs with
`manager::recovery::acknowledge_abandoned` in the same transaction as local
lifecycle settlement. It rejects active recovery or unresolved allocations and
does not delete Job, Grant, Ticket or cleanup history. Repeated acknowledgement
is harmless; advancing a remote sequence floor alone does not perform this step.


Same-UUID history repair temporarily refuses **new** single/batch submissions with
`authority_repair_pending`, before creating a Received record. Existing durable
receipt replay, recovery, inspection and ordinary cancellation remain available.
This prevents continuing clients from replenishing the queue that explicit repair
must drain; it does not cancel or discard any existing Job. The exception ends
when reconciliation and platform proof clear the durable reset gate.

A returning installation fenced by an unfinished retirement gets
`retirement_pending` with the original operation ID. The completed retirement
receipt is exposed only after the durable operation finishes. Legacy registries
created before cleanup-space reservations may exceed today's future-byte budget;
AuthoritySnapshot reports this condition explicitly alongside `storage_budget`.
It does not erase history or pretend that space has been reserved retroactively.

### MR-3 nested executor-test bootstrap delegation

The transitional BootstrapWork optionally requests `delegate_test_cgroup`.
The default false value is omitted from serialization, preserving pre-extension
request hashes and cleanup bindings. The installed trusted supervisor exposes
only its already-accounted work cgroup at a private mount alias, with depth 16
and 1024 descendant bounds. The host hierarchy remains read-only; the alias is
passed as `STILLYARD_TEST_CGROUP_ROOT`, never accepted from caller environment.
All nested test processes remain descendants of the outer held boundary. Cleanup
kills the outer tree, observes recursive `populated=0`, removes empty descendant
cgroups, verifies the original boundary inode and seals its removal before release.
A failure in any step retains the outer obligation. This is executor-test
infrastructure; installed attached WSL acceptance still replaces the bootstrap.
The kernel delegation and recursive-kill semantics are documented in the
[Linux cgroup v2 specification](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#delegation).


### MR-3 persistent bridge and executor publication

`machine bridge --endpoint <native endpoint>` is an installed native process
using length-prefixed machine frames on stdin/stdout. The global `--endpoint`
option may precede `machine`. Bridge frame version 1 binds an explicit IPC version,
request UUID and per-call deadline (1..30000 ms). There is one in-flight request;
partial, malformed, oversized or incompatible frames end the transport. Replies
preserve the request UUID and bounded typed outcomes. The bridge forwards connect,
exchange, participant and authority observation only; pairing remains an explicit
owner operation. EOF, timeout or bridge death never modifies a Grant or asserts
executor cleanup. Linux pins the installed image digest from its installation
configuration, passes the explicit interop socket and enforces absolute read/write
deadlines. Reconnect uses the durable manager outbox; the bridge never invents a
retry or claims a lost operation was rejected.

The Linux executor journal is separate from the SQLite reset deletion set. Its
explicit installation anchor binds journal/store/domain UUIDs; opening missing or
mismatched history fails. A single owner-only, exclusively locked writer publishes
checksummed state via file fsync, atomic rename and directory fsync. Creation intent
precedes cgroup creation; exact cgroup/root/executable records precede ticket use.
The ticket boundary digest includes creator generation/identity, local IDs, pinned
cgroup and actual prepared root/image. Release intent is durable before SQLite
consumption and actual kernel release. Cleanup removes the exact empty boundary
before publishing its seal; interruption in this gap retains uncertainty. A seal
survives reopen and cannot authorize another launch. The fixed journal bounds
are admission limits, never permission to discard outstanding obligations.


The managed namespace IPC extension uses IPC 24. `STILLYARD_SERVER_ATTESTATION`
contains only a public verification context, tied to the executor-injected
Job/Attempt/Invocation and endpoint. It is not an ancestry credential. The
attested request envelope contains generation, parent, a fresh 256-bit nonce and
one ordinary request. The daemon rejects nesting and proves the actual socket
peer's current containment before dispatch. Its Ed25519 signature binds the
server context, parent, nonce, normalized request hash and complete response.
A forwarded request from outside that containment gets no usable signature.
Signing keys rotate with the daemon and are never exposed to user environments
or persisted in a user-visible file. Existing unmanaged IPC still pins the
kernel process and selected executable directly. Verification uses
[ed25519-dalek 2.2.0 strict verification](https://docs.rs/ed25519-dalek/2.2.0/ed25519_dalek/struct.VerifyingKey.html#method.verify_strict);
its [declared MSRV is 1.81](https://docs.rs/crate/ed25519-dalek/2.2.0/source/Cargo.toml).
Scheduled Rust 1.85 gates must validate the resolved dependency graph.


### MR-3 Linux observation implementation profile

Guest CPU uses the first eight aggregate `/proc/stat` counters, subtracting idle
and iowait. Guest/guest_nice counters are already represented in user/nice and
are not added again. A counter regression, changed CPU inventory or unavailable
procfs fences the sample. Percentages round upward. Field definitions:
[Linux procfs documentation](https://docs.kernel.org/filesystems/proc.html).

Linux memory evidence uses `MemAvailable`, with no fabricated Windows commit
headroom. The coordinator separately checks Windows physical/commit headroom,
including vmmem. Each side applies the existing conservative planned-debit and
margin rule to its own observation; the two available values are never added.
`CommitLimit - Committed_AS` is not substituted for Windows commit headroom.

Guest disk pressure takes the maximum over whole block devices; partitions are
excluded. Each device uses the larger active/weighted I/O delta capped at the
interval, and outstanding I/O conservatively marks that device busy. Counter
regressions, sysfs identity changes or inventory changes invalidate the sample.
This conservative pressure measure is not a throughput or latency estimate.
[Kernel I/O counter definitions](https://docs.kernel.org/admin-guide/iostats.html).

Process rules inspect executable basenames in the visible PID namespace through
pinned proc-directory handles. Kernel threads and zombies do not represent live
user executables. An unreadable executable makes coverage unavailable; truncated
or caller-changeable `comm` text never proves absence. Host process rules remain
a separate coordinator check. GPU/NVML guest support remains explicitly unavailable.

### MR-3 installed diagnostics extension (IPC 25, alpha.20)

The persistent bridge gains `scheduling_status`, returning the coordinator's
existing `MachineSchedulingSnapshot` without inventing capacities from guest
configuration. The attached daemon retains one bounded observation with its
original capture time and session/config identity, exposes it as `mode=attached`,
and marks disconnected or older-than-30-second evidence with a blocker. This
cache has no admission/release authority: Tickets, local readiness and durable
Grant reconciliation retain their existing barriers. A missing observation is
unavailable, never zero usage. Pairing validates the observed machine and epoch
against the authenticated session. A mismatched-identity or disconnected-cache
negative control must fail if it is reported as fresh, and installed acceptance
must show the same captured scoped counts through both coordinator and guest.
Both installed executables must speak IPC 25; JobSpec 4 and HostConfig 2 are
unchanged. The existing public machine scheduling schema shape is retained.

### MR-3 native companion for isolated cross-OS fault acceptance

The installed Windows primary may run `bootstrap run --spec <work>
--native-controller <absolute executable> -- <arguments>`. This hidden transitional
acceptance option is removed with bootstrap; it does not change BootstrapWork,
Arm/Seal, or their IPC/storage contracts. Before spawning, the CLI verifies an
existing absolute controller image and its authenticated current primary role
in a native Job with a finite timeout. The controller uses ordinary inherited
Windows Job containment, working directory, environment and canonical streams,
with null stdin and no breakaway flag. It must use separate isolated Stores and
endpoints and must not submit work to the installed daemon before Arm.

Controller spawn precedes Arm so spawn failure cannot create an uninspectable
Linux obligation. If descriptor validation or Arm later rejects, this remains
ordinary accounted native work; root failure cancels it and the Windows daemon
cleans the enclosing Job. No Linux work starts without the existing durable Arm
and trusted supervisor barrier. Neither image authentication nor the exact
primary/root and sole-Lease checks is relaxed.

The CLI awaits the existing bootstrap path, then the controller, with the latter
wait bounded by the descriptor timeout plus 30 seconds measured from its spawn.
Bootstrap failure takes precedence. Exit zero requires successful bootstrap
termination and controller exit zero. Controller death or abort messages are
never Linux cleanup evidence. CLI death retains the bootstrap obligation until
trusted reconciliation; the root Job timeout remains the final native backstop.
Shared canonical streams are diagnostic text, never a machine protocol.

The harness supplies private mailbox paths through environment variables and
exports only an explicit public-artifact allowlist. It separately retrieves the
authority's actual retained bootstrap proof, requiring released, sealed_empty,
termination=exited and root_exit_code=0. The subject's inner executor seal and the
outer bootstrap proof identify different nested boundaries; neither is substituted
for the other. A result file or controller exit cannot release either boundary.
