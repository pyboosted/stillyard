# Review — MR-2 coordinator protocol (alpha.17 / IPC22, snapshot `o`)
Lens: manager outbox, single-use tickets, release and replay safety.

## Summary

The durable-ordering skeleton is sound in the places that matter most: the coordinator journals a potentially-used permission before its SQL commit and completes either side on restart (`machine_allocation.rs:229-250`, `machine_recovery.rs`), ticket identity is single-use and chained through `release_sequence` + exact `previous_cleanup` (`machine_ticket.rs:31-116`, `machine_allocation.rs:469-504`), and `seal_release` correctly refuses to seal while an `AuthorizeInvocation` for that key is unanswered (`manager.rs:398-407`) — which is the exact defence against the "missing local ticket releases grant" mutant.

The defects I found are concentrated in three places: (a) the manager outbox's capacity arithmetic, which caps outstanding operations at ~14 and can block `Release`; (b) two **one-sided** recovery paths where the coordinator retires history the manager can never catch up to, producing a permanently unbindable pair; and (c) resource-identity mismatches and missing states (`Uncertain` is never written; fences use two incompatible spellings) that break exclusion and leave no clearance path. Several of these are not "MR-3 work" — they are gaps in the MR-2 slice that is being claimed.

I am reviewing the supplied files only. The transport layer, native admission core, and `host_observation` are not in scope, so a few findings are marked as inference and state their assumption.

---

## Findings

### F1 — Manager outbox wedges at ~14 unanswered operations, including `Release` — **High**

`src/machine/manager.rs::enqueue`, lines 134–152.

The byte accounting charges a full `MAX_FRAME_BYTES` (1 MiB) for the *future* outcome of every un-outcomed row:

```sql
COALESCE(SUM(length(command_json)+COALESCE(length(outcome_json),1048576)),0)
```

and then requires `bytes + json.len() + MAX_FRAME_BYTES <= 15 MiB` for a non-ack command.

Arithmetic: 15 MiB = 15,728,640; minus the 1 MiB reserve leaves 14,680,064 = exactly 14 × 1,048,576. With **14** unanswered rows, `bytes >= 14,680,064`, so any 15th enqueue fails.

Minimal trace:
1. Bridge partition; coordinator unreachable.
2. Manager persists Arm/Withdraw/Release intents for 14 allocations (each `enqueue` succeeds).
3. Allocation 15 finishes cleanup; caller invokes `seal_release`.
4. `seal_release` → `enqueue(Command::Release{..})` → `Err("outbox capacity reached; retain obligations and compact acknowledged replies")`.
5. The whole transaction rolls back, so `attached_grants.seal_json` is not written either. The allocation cannot be sealed, and its resources stay debited.

Invariant violated: protocol §5 "16,384 queued outbox entries per domain; reaching a limit blocks new starts, **never deletes obligations**" — and, more seriously, R-MR-4's release path. The implementation is ~3 orders of magnitude below the specified bound and it blocks *releases*, not just starts. The `Acknowledge` reserve (`count >= if is_ack {16_384} else {16_383}`) does not help: `pending()` returns the earliest unanswered row, so an `Acknowledge` enqueued behind a partition backlog is sent last and cannot drain it.

Fix: charge the actual reply size, not `MAX_FRAME_BYTES`, per unanswered row (bound the *reply* at read time, which `read_frame` already does), or bound `count(outcome_json IS NULL)` explicitly at the specified 16,384 and drop the speculative byte reserve. Reserve a slot/byte budget for `Command::Release` the way one is reserved for `Acknowledge`.

Test: enqueue 20 commands with no `accept`, assert all 20 succeed; then assert a `seal_release` on a 21st allocation succeeds while every prior operation is unanswered.

---

### F2 — Coordinator reset recovery permanently unbinds a manager whose `applied_sequence` lags — **High**

`src/store/machine_reset.rs::prepare_machine_reset_recovery` line 81, against `src/machine/manager.rs::bind` lines 64–80.

Recovery writes `retired_sequence_floor = accepted_sequence = anchor.accepted_sequence` (`VALUES (...,?7,?7)`) and calls `retire_machine_sequences(domain, accepted)`. `bind` then rejects any manager whose `applied_sequence` is below that floor:

```rust
|| participant.retired_sequence_floor > applied
```

Minimal trace:
1. Manager enqueues operations 1–5; coordinator accepts all five; replies for 4 and 5 are lost. Manager `applied_sequence = 3`.
2. Coordinator SQLite is displaced; `machine recover` runs; the domain row gets `accepted_sequence = retired_sequence_floor = 5`.
3. Manager reconnects and calls `bind`. `5 > 3` → `Err("peer watermark/session differs from durable manager history")`.
4. There is no manager-side primitive that can advance `applied_sequence` past a permanently lost response: `accept` requires a `Reply` and `applied.checked_add(1) == request_sequence` (`manager.rs:220`). Replaying operation 4 gets `history_unknown` from the coordinator (`machine_allocation.rs:103-107`).

The pair is permanently unbindable, and every armed Grant of that manager stays outstanding forever — which then also permanently blocks `finish_machine_reset_recovery` (see F4).

Invariant violated: §10 "Lost response history receives an explicit retired floor" is implemented only on the coordinator; §5 "Ordinary reconnect/released-ack loss automatically converges" fails.

Fix: add a manager-side `retire_lost_responses(tx, through_sequence, evidence)` that (a) requires the coordinator-reported floor from a bound/authenticated `ParticipantSnapshot`, (b) marks affected outbox rows `history_unknown` rather than deleting them, (c) marks every allocation touched by a retired `Arm`/`AuthorizeInvocation` as requiring inventory reconciliation and forbids consuming any of its tickets, and (d) advances `applied_sequence`/`retired_floor` to the coordinator floor. `bind` should then accept `retired_sequence_floor > applied` only when that retirement record exists.

Test: the fault scenario above, end-to-end with a public fake manager; assert the manager rebinds, that the affected allocation can still `seal_release` with `user_code_released = true` for any ticket whose fate is unknown, and that no ticket from a retired range can be consumed.

---

### F3 — Native and attached fences use incompatible identities, so cross-domain exclusion is not enforced at ticket time — **High**

`src/store/machine_queue.rs::physical_claims` lines 57–66 vs `native_debits` lines 8–15; consumed at `src/store/machine_ticket.rs:160-163, 207`.

`physical_claims` writes fences as `format!("domain:{}:{}", id.scope.0, id.resource_id)`. Native `leases.claims_json` holds bare, canonicalised fence names. In `authorize`:

```rust
let mut active = super::machine_queue::native_debits(tx)?;          // bare names
... machine_grants.physical_claims_json ...                          // "domain:<uuid>:<name>"
let resource_blockers = physical.blockers(&readiness.config.resources, &active, ...);
```

`physical` (the requester) is prefixed; the native entries in `active` are not. A prefixed exclusive fence can never string-match a bare one.

Minimal trace:
1. Native Job N holds `exclusive_fences = ["C:\\work\\scratch"]` (lease state `granted`).
2. Attached allocation A has an alias binding its guest fence to the same physical object; `topology.expand` resolves it to the machine scope, and `physical_claims` stores `domain:<machine-scope-uuid>:C:\work\scratch`.
3. A requests `AuthorizeInvocation`. `resource_blockers` compares `"domain:…:C:\work\scratch"` against `"C:\\work\\scratch"` → no conflict → ticket issued.
4. A releases user code into a directory N holds exclusively.

Invariant violated: R-RES-1 (complete atomic set, no partial acquisition), R-MR-3 "Fences are domain-local unless aliases explicitly bind a shared filesystem object", and the A-04 partial-grant negative control.

Note the *offer* path is correct: `machine_offer_before_native` expands both sides through `topology.expand` into scoped `ScopedClaims` before calling `blockers` (lines 294–313). Only the flattened `ResolvedClaims` comparison is broken. **Inference** (caller not supplied): `machine_queue::remote_debits` returns the same prefixed fences, so if the native admission core consumes it against bare native fences, native admissions also cannot see attached fence holders — that would make the hole bidirectional.

Fix: do the ticket-time recheck in scoped space (`topology.expand` the native leases, as `machine_offer_before_native` already does) rather than flattening to `ResolvedClaims`, or apply the identical `domain:{scope}:{id}` canonicalisation to native lease fences when they are read as debits. Keep exactly one spelling.

Test: A-04 variant — native Job holds `exclusive:X`; attached candidate with alias→`X` reaches Armed; assert `AuthorizeInvocation` is rejected with a fence blocker. Negative control: the current code must fail this test.

---

### F4 — `GrantState::Uncertain` is never written, and there is no audited clearance for an attached Grant — **High (blocks MR-2 closure)**

Grep of all supplied files: `Uncertain` is *read* in `machine_allocation.rs:196-198, 458-467, 565`, `machine_queue.rs:18, 305`, `machine_reset.rs`, `store/machine.rs:151`, `machine_recovery.rs` — and **never written**. The only writers of `GrantSnapshot.state` set `Offered` (`machine_queue.rs:487`), `Armed` (`machine_allocation.rs:581`), `Expired` (`machine_queue.rs:257`, `machine_allocation.rs:700`) and `Released` (`machine_allocation.rs:505`).

Consequence chain:
1. An attached manager is destroyed (VM deleted, distro unregistered) while holding an Armed Grant.
2. Accounting is correctly retained (only `state='offered'` can expire — `machine_queue.rs:251`), so no vector is freed. That part is right.
3. But the Grant is indistinguishable from a healthy Armed Grant in `Inspect`, `AllocationEvent`, and `doctor`.
4. `finish_machine_reset_recovery` requires `NOT EXISTS(machine_grants WHERE state IN ('offered','armed','uncertain'))` (`machine_reset.rs:148`). The reset gate can therefore **never** be opened.
5. `force_release_authority` / `doctor clear-containment` operate on `AuthorityHold`s and native `Containment`s respectively. Neither touches `machine_permissions` or `machine_grants`.

Invariants violated: R-MR-4 "unresolved proof requires retained accounting **or explicit audited risk clearance**"; §9 "Clearance identifies all affected grants and outstanding tickets and records operator identity plus explicit risk acceptance"; R-MR-6 "doctor MUST expose … reconciliation blockers".

Fix (two pieces, both required to close MR-2):
- A transition to `Uncertain` on the conditions §4 names (fenced connection with outstanding armed grants, snapshot absence, runtime-incarnation change), retaining armed accounting and surfacing the reason in `GrantSnapshot`/`AllocationEvent`.
- `machine clear-grant <GrantId> --force` mirroring `force_release`: requires an explicit risk-acceptance reason, records operator `ProcessIdentity`, enumerates every issued ticket in the audit record, writes the audit to the registry, and only then retires the permission and releases the vector.

Test: M-A05/M-A06 variant — arm a fake manager, destroy its anchor, assert the Grant becomes `Uncertain` with retained debits, assert the reset gate stays closed, then assert `clear-grant --force` opens it and leaves an audit file naming every ticket.

---

### F5 — The 250 ms ticket-freshness bound is measured on the wrong endpoint; `consume_ticket` enforces none — **High**

`src/store/machine_ticket.rs::authorize` lines 144–152; `src/machine.rs::InvocationIntent` lines 277–288; `src/machine/manager.rs::consume_ticket` lines 344–365.

```rust
let (unix, monotonic) = crate::host_observation::observation_clock()?;
if monotonic.checked_sub(sample.captured_monotonic_millis).is_none_or(|age| age > 250) { ... }
```

This bounds the age of the coordinator's own host sample at the moment of ticket commit. Protocol §4 requires: "Host sample must be taken **after receipt of that challenge**. Accept a ticket only within the lesser of configured sample age and 250 ms **measured from the manager's challenge send**."

`InvocationIntent` carries only `readiness_challenge: Uuid`, which is used solely for uniqueness (`machine_ticket.rs:54-58`) and echoed into the ticket. There is no manager-side monotonic send timestamp on the wire, so the coordinator cannot bound request-queueing delay, and the manager cannot verify anything on receipt. `consume_ticket` checks session identity and byte-equality of the stored ticket — it never looks at `issued_unix_millis`, `host_sample_unix_millis`, or `host_observation_generation`.

Minimal trace:
1. Manager samples local quiet evidence and sends `AuthorizeInvocation` at T0.
2. The bridge stalls 3 s (64 in-flight requests / 4 MiB queued are both legal per §3).
3. Coordinator receives at T0+3000, samples the host at T0+3000, commits at T0+3050. Age check: 50 ms ≤ 250 ms → **pass**.
4. Manager receives the ticket at T0+3100, `consume_ticket` succeeds (no freshness check), releases user code.
5. Total staleness of the manager's *local* evidence: 3.1 s, versus the 250 ms the protocol promises.

Invariants violated: §4 freshness contract; R-MR-3 "Quiet release requires fresh host and local evidence"; the M-A11 negative control ("Suspend between quiet sample and release → Stale quiet release").

Fix: add `challenge_sent_monotonic_millis` (manager clock) and a coordinator-side echo to `InvocationIntent`/`InvocationTicket`; require `sample.captured_*` to post-date request arrival; and add an explicit `consume_ticket` precondition that rejects when `now - challenge_send > 250 ms`, when `ticket.host_observation_generation` differs from the generation recorded at challenge time, or on any local suspend/clock discontinuity. Per §4, never compare Windows and Linux wall-clock values — use each side's own monotonic delta.

Test: fixture that injects a 3 s delay between challenge send and ticket receipt; assert `consume_ticket` returns a typed staleness error and that the allocation can then be sealed with `user_code_released = false`.

---

### F6 — `Outcome::Rejected` conflates durably-recorded rejections with pre-`apply_command` transport rejections — **High**

`src/store/machine_allocation.rs::machine_exchange` lines 33–128 vs `src/machine/manager.rs::accept` lines 220–338.

`machine_exchange` has two classes of rejection:
- **Recorded**: `rejection(error)` (line 161) produces `Outcome::Rejected`, and the operation *is* written to `machine_operations` with `accepted_sequence` advanced (lines 164–177).
- **Not recorded**: every early `return Err(rejected(...))` — unauthorized peer (37, 41, 47), fenced session (48), stale authority (61), fenced participant (69), retired floor (103), out-of-order sequence (109), `limit_exceeded` (122). The transaction is dropped; nothing is persisted.

Both surface as `StoreError::OperationRejected { code, detail }`. The transport (not supplied) must turn the second class into something the manager sees. If it synthesises `Reply { outcome: Outcome::Rejected { code, detail } }`, `manager.rs::accept` will happily record it and execute:

```rust
tx.execute("UPDATE attached_peer SET applied_sequence=?1 WHERE singleton=1", [request.request_sequence])?;
```

Trace: coordinator returns `history_unknown` "operation is below the retired sequence floor" for sequence 7 (not recorded, `accepted_sequence` stays 6). Manager applies it, `applied = 7`. On the next reconnect `bind` checks `participant.accepted_sequence (6) < applied (7)` → permanently unbindable, same terminal state as F2.

Invariant violated: §3 "Exact replay returns the recorded outcome; … a forgotten sequence never becomes a fresh operation" — the sequence watermarks on the two sides must never diverge.

Fix: split the wire type. Give `Reply` a variant (or a sibling frame) for *non-sequence-consuming* errors, and make `manager.rs::accept` reject any `Reply` whose outcome is not one the coordinator could have recorded for that command. Cheapest correct form: have `machine_exchange` return `Result<Reply, TransportRejection>` where `TransportRejection` cannot be constructed from a recorded outcome, and assert in `accept` that a recorded `Outcome::Rejected` is only accepted when the coordinator's echoed `coordinator_revision` advanced.

Test: fake coordinator returns a synthesised `Rejected` for a sequence it never recorded; assert `accept` refuses and `applied_sequence` is unchanged.

---

### F7 — The manager cannot discover the coordinator's `configuration_sha256`; any host-config change permanently freezes the attached domain — **Medium-High**

`CandidateUpsert` requires `candidate.configuration_sha256 == configuration` (`machine_allocation.rs:408`), `Arm` requires it (line 549), `AuthorizeInvocation` requires it (line 549), and `machine_offer_before_native` silently `continue`s past mismatched candidates (line 393). But no reply carries the current hash: `Reply` has only `coordinator_revision`; `ParticipantSnapshot` and `ConnectChallenge` have no config field; `Outcome::Inspection` returns `GrantSnapshot`s whose embedded `candidate.configuration_sha256` is the *old* value.

Trace: operator changes `HostConfig` (a supported R-RES-1 operation). Every subsequent `CandidateUpsert` from the attached manager returns `Rejected{code:"invalid_candidate"}` with a detail string containing no hash. The manager has no protocol means to obtain the new value, so the domain never schedules again. Existing Armed grants can still drain (`apply_release` has no config check), which is correct, but nothing can restart.

Invariant violated: R-MR-3 "The MR protocol specifies … config epochs"; R-MR-6 "Public APIs, CLI, TUI, events and doctor MUST expose … config/session revisions".

Fix: put `configuration_sha256` (and a monotonic config revision) in `ConnectChallenge`, `ParticipantSnapshot`, and every `Reply`. Add a `config_changed` rejection code that carries the new hash so the manager can re-upsert without a reconnect. Also implement §6's "Changing a mapping first closes ticket issuance and drains or seals all outstanding start rights against that mapping" as an explicit coordinator step — today the config change is instantaneous and only implicitly freezes work.

---

### F8 — `payload_sha256` is recomputed from a JSON round-trip on every send, so any wire-shape change wedges in-flight outbox entries — **Medium**

`src/machine/manager.rs::pending` lines 175–182 reconstructs the command from `command_json` and calls `Request::new`, which recomputes `payload_sha256 = payload_hash(&command)` (`machine.rs:539`). The stored `attached_outbox.payload_sha256` is never used for transmission. The coordinator compares the recomputed hash against the one it recorded on first receipt (`machine_allocation.rs:92-99`) and returns `conflict "operation or sequence has another payload"` on any difference. `Request::validate` also hard-rejects `version != WIRE_VERSION` (`machine.rs:546-548`).

Trace: manager enqueues operation 12; partition; both binaries are upgraded per §9's procedure; the new `Command` shape adds one field (even a `#[serde(default)]` one, which still round-trips *out* differently). On reconnect, `pending()` re-serialises operation 12 with the new shape → new hash → coordinator returns `conflict` forever. `pending()` keeps returning the same row; the manager is stuck.

Invariant violated: §9 "MR-2 uses the separately versioned additive `machine_*` extension … downgrade is prohibited" — but the upgrade procedure never states that the manager outbox must be **fully drained** before replacement, and nothing enforces it.

Fix: either store the exact serialised `Command` bytes and transmit them verbatim (`serde_json::value::RawValue`), so the hash is byte-stable by construction, or add an explicit precondition to the upgrade gate: refuse to start a new manager binary while `attached_outbox` has any row with `outcome_json IS NULL`, and make that a checked `doctor` prerequisite.

Test: serialise an outbox row with binary A, replay it with binary B whose `Command` gained a defaulted field; assert either byte-stability or a clean, actionable `upgrade_requires_drained_outbox` failure — not a silent permanent `conflict`.

---

### F9 — `retired_allocations` grows without compaction inside a 4 MiB registry; overflow hard-stops all admission — **Medium**

`src/authority.rs::finish_machine_commit` lines 396–401 inserts one 64-hex key hash per released allocation into `Registry.retired_allocations`, a `BTreeSet<String>` with no eviction anywhere in the file. `publish` (lines 1117–1131) enforces `MAX_REGISTRY_BYTES = 4 MiB`.

Arithmetic: ~67 JSON bytes per entry → ~62,000 released attached allocations (≈ one per attached Attempt) before `publish` returns `Err`. `machine_permissions` has no count cap at all (the protocol's 65,536/machine is unenforced), and its snapshots are far larger.

Trace at the limit: `machine_exchange` commits SQL, then `finish_machine_commit` → `publish` → `Err`. The pending journal entry survives, so `admission_blocker()` returns `authority_commit_pending` (`authority.rs:899-902`) and **all** admission — native and attached — closes with no operator remedy: there is no compaction API for `retired_allocations` and no way to clear a pending commit except completing it.

The size check correctly precedes the state mutation, so the authority does not become `Unknown` — good. But the machine is stopped.

Fix: bound `retired_allocations` by a durable *retired allocation-key floor* per (domain, store) rather than an unbounded set — the allocation key already contains `authority_epoch` and `lease_id`, so a per-domain monotonic watermark plus the retained set of keys above it is sufficient to reject re-acquisition. Enforce the specified 65,536/4,096 grant caps with a typed `limit_exceeded` rather than letting the registry byte limit be the de facto cap.

Test: drive 70,000 arm/release cycles against a fake manager; assert admission stays open and the registry stays under 4 MiB.

---

### F10 — No manager-side reconciliation-snapshot primitive; `ReconcileAllocation` cannot express an unresolved ticket request — **Medium (blocks A-MR-2)**

`src/machine/manager.rs` exposes `initialize, bind, enqueue, pending, accept, consume_ticket, record_cleanup, seal_release`. There is no function that produces the §5 snapshot `(session, manager_store_uuid, begin_sequence, end_sequence, page_count, digest, config_revision)` from `attached_grants`/`attached_tickets`/`attached_outbox`, and no helper for the digest — which the coordinator computes as `payload_hash(&Vec<Vec<ReconcileAllocation>>)` over the *unwrapped* page vector (`machine_allocation.rs:813-820`), an undocumented wire detail.

Separately, `ReconcileAllocation { key, offer_nonce, tickets: Vec<InvocationIntent>, sealed_release }` (`machine.rs:506-513`) has no way to distinguish:
- a ticket the manager holds (`accept`ed, row in `attached_tickets`), from
- an `AuthorizeInvocation` still unanswered in the outbox whose fate is unknown,

and carries no `consumed`/`cleanup` state. `ReconcileCommit` requires exact vector equality `recorded.tickets != grant.tickets` (line 845), so the manager must reproduce the coordinator's exact ticket *order*, including tickets whose replies it never received, purely by outbox sequence — with no field to flag them.

Invariant violated: §5 "It includes **all** allocations, unacknowledged operations, issued/consumed tickets, live or uncertain containments and sealed releases."

Fix: add `manager::reconcile_snapshot(connection, config_sha256) -> (ReconcileBegin, Vec<ReconcilePage>)` that assembles pages, computes the digest with the exact coordinator shape, and freezes new starts during the cut (§5). Extend `ReconcileAllocation` tickets to `Vec<TicketRecord>` with `intent`, `state: {requested_unknown | issued | consumed | cleaned}`, and the cleanup proof when present; have `ReconcileCommit` compare on `intent` and treat `requested_unknown` as "coordinator's record is authoritative".

Test: M-A07 — issue a ticket, drop the reply, reconnect, run reconciliation; assert the coordinator accepts the snapshot, the manager learns the ticket exists, and no duplicate launch is possible.

---

### F11 — `Inspect` has no pagination, so a manager that lost history cannot enumerate its Grants — **Medium**

`src/store/machine_allocation.rs:707-723`: `Command::Inspect { key: Option<AllocationKey> }` is an *exact-match* filter (`WHERE ... AND (?2 IS NULL OR allocation_key=?2)`), `ORDER BY allocation_key LIMIT 257`, returning `truncated: bool`. A domain may hold up to 4,096 candidates (`MAX_DOMAIN_CANDIDATES`).

Trace: manager store is lost (§5 "If manager history was lost, use registered boundary identities … and adapter inventory covering outstanding rights"). The manager reconnects and calls `Inspect { key: None }`. It receives 256 grants and `truncated: true`, with no cursor and no way to ask for the next page — and it cannot use the `key` filter because it no longer knows its own allocation keys.

Fix: change `Inspect` to `{ key: Option<AllocationKey>, after: Option<AllocationKey>, limit: u32 }` and return the last key in `Outcome::Inspection`. This is a `machine_*` extension schema change, which §9 already anticipates for MR-2.

---

### F12 — Reset recovery loses the attached aging anchor — **Medium (fairness, not safety)**

`src/store/machine_reset.rs:90-96` re-creates queue rows as `format!("attached:{key}")` with `accepted_ms = grant.offered_unix_millis`, whereas `CandidateUpsert` normally uses `format!("remote:{job}")` with `accepted_ms = now` at first registration (`machine_allocation.rs:656-669`, `INSERT OR IGNORE` so all Attempts share it).

After recovery, a Job's recovered allocation is anchored per-*allocation* at its offer time, while any new Attempt of the same Job inserts a fresh `remote:{job}` row at `now`. The two diverge, and the `machine_queue.owner UNIQUE` constraint no longer collapses a Job's Attempts onto one anchor.

Invariant violated: R-MR-3 "Attached aging originates at first durable authority registration, never a backdated guest clock"; §6 "All Attempts retain that first sequence/time."

Fix: persist the queue identity (`owner`, `accepted_ms`, and the original global sequence) in the registry alongside each `machine_permissions` entry, and restore it during recovery instead of synthesising a per-allocation anchor. If the original identity is unavailable, mark the recovered candidate's rank as `unknown` and surface it in `Inspect` rather than silently backdating it to `offered_unix_millis`.

---

### F13 — Smaller, concrete items — **Low**

| # | Location | Issue | Fix |
|---|---|---|---|
| a | `manager.rs::enqueue:107-118`, `seal_release:408-412` | `query_row` on `attached_grants` without `.optional()` → bare `rusqlite::QueryReturnedNoRows` surfaces as `JournalError::Database`, indistinguishable from a real DB fault. | Use `.optional()` and return a typed `History("allocation has no local Grant")`. |
| b | `manager.rs::seal_release:398-407` | The unanswered-operation scan matches only `Command::AuthorizeInvocation`. An unanswered **`Arm`** for the same key is ignored, so sealing an allocation whose Arm reply was lost fails with the opaque error in (a) instead of a typed "replay the Arm first". | Extend the scan to any unanswered command naming the key, with distinct error text per command. |
| c | `machine.rs:92-101` | `PairingRegistration.secret: [u8;32]` derives `Serialize` **and** `JsonSchema` with no redaction. `Debug` is redacted (103-111), but any accidental inclusion in a public projection or generated schema fixture exposes the field. | Wrap in a newtype whose `Serialize` is only reachable from the anchor writer; drop `JsonSchema`. Assert in a test that `schema spec` output contains no `secret` field. |
| d | `machine_reset.rs:65-69` | Recovery deletes `machine_ticket_identities` and rebuilds it only from `machine_obligations` (which excludes Released grants), so global `invocation_id`/`containment_id` uniqueness is no longer enforced against tickets of retired allocations. | Persist a retired-invocation floor or retain the identity rows across reset. |
| e | `store/machine.rs::validate_machine_history:100-136` | `record_reset` is a silent `Ok(())` when `registry.coordinator` is `None` (`authority.rs:693-696`), so an inventory mismatch on a host with no bound coordinator passes validation silently. Currently masked by `authority_commit_pending`/`capable()` fail-closed paths, but the gate itself fails open. | Make `record_reset` return an error (or set `State::Unknown`) when there is no coordinator to gate. |
| f | `machine_ticket.rs::QuietProgress:14-22, 236-252` | `first`/`last`/`evaluated` persist raw monotonic values across daemon restart and reboot. Safety depends entirely on `observation_generation` changing and `quiet_stability` resetting on that change — neither is visible in this snapshot. | Add `boot_id` + `daemon_generation` to `QuietProgress` and reset unconditionally when either changes. See Evidence Gaps. |
| g | `machine_allocation.rs:656` | Work and probe allocations of one Job share `owner = remote:{job}`, so their `ScheduleKey` is fully tied (same `effective_priority`, `accepted_ms`, `rowid`); `outranks` cannot break the tie in the reservation `higher_overlaps` computation (`machine_queue.rs:411-417`). | Add the allocation `lease_id` as a final tiebreaker in `ScheduleKey`. |
| h | `machine_allocation.rs:86-89` | The replay lookup uses `WHERE domain_id=?1 AND (sequence=?2 OR operation_id=?3)`, which defeats both indexes and full-scans up to 16,384 rows per operation. Correctness is fine (either match yields `conflict`), but it is O(n) per request. | Two indexed lookups. |

---

## Evidence Gaps

1. **Transport layer not supplied.** F6 depends on how pre-`apply_command` `StoreError::OperationRejected` values are converted to `Reply`s. If the transport already refuses to synthesise a sequence-consuming `Reply`, F6 is a documentation/type-safety issue rather

than a live bug. **Question:** does the frame writer ever emit a `Reply` for an error path that did not reach `apply_command`?

2. **Native admission core not supplied.** F3's bidirectionality is inference. I can prove the mismatch inside `authorize` from the supplied code (`physical` prefixed vs `native_debits` bare); I cannot prove that native admission consumes `machine_queue::remote_debits` against bare native fences. **Question:** which function calls `machine_queue::remote_debits`, and does it compare against `ResolvedClaims` built from a `JobSpec`?

3. **`host_observation` not supplied.** F13(f) and the quiet-progress carryover across a *successful* ticket depend on `quiet_stability` and `quiet_budget`. Specifically: after a ticket is issued, `progress.first`/`last` are not reset (`machine_ticket.rs:221-252`), so a postcondition requesting a ticket under the same allocation inherits a `first` recorded before the primary ran. If `quiet_stability` does not reset `first` on an `observation_generation` change or a gap exceeding `quiet_max_sample_gap_millis`, the postcondition skips its stability wait entirely — the A-05 / M-A11 stale-quiet mutant. **Question:** does `quiet_stability` reset `first` when `generation` differs, and is `observation_generation` regenerated on daemon start and on resume?

4. **Caller of `machine_offer_before_native` not supplied.** The `break` at `machine_queue.rs:390` stops offering once an attached candidate ranks below the native candidate under consideration. That is only free of head-of-line blocking if the caller invokes it once per native candidate in rank order (and once with `native: None` after the native scan). **Question:** confirm the call sites; if it is called once per scheduler pass with only the native *head*, attached candidates below that head are starved, violating R-MR-3 "no domain head-of-line blocking."

5. **Native publish-before-resume not verifiable here.** `record_suspended_root` and `mark_started_with_identity` are not in scope. `native_start_permission` (`store/authority.rs:79-121`) builds the record and its doc comment asserts the ordering, and `record_native_start` (`authority.rs:235-291`) validates epoch/domain/store/lease binding and `state == Armed` — all consistent with the claim. I take the brief's statement as given but did not verify it.

6. **No manager-side integration test in scope.** `manager.rs:452-453` declares `mod tests` but the file is not supplied. The brief states these are local SQLite reopen/rollback tests only. F1, F2, F5 and F6 are all in the gap between "local SQLite primitives" and "real public fake-manager integration", which is exactly the slice not yet claimed.

---

## Recommendations

**Must fix before MR-2 can be called closed** (each maps to an A-MR-2 negative control):

1. F1 — outbox capacity arithmetic. Cheapest fix in the set; unblocks every partition-fault scenario. Add the "15 unanswered + a Release" test as a permanent regression.
2. F2 + F6 — the two watermark-divergence wedges. These are the same class of bug (a durable floor advancing on one side only) and should be fixed together with a single invariant test: *after any fault, `bind` must succeed and `manager.applied ≤ coordinator.accepted` must hold*. Make that a property test over the fault matrix rather than a scenario.
3. F4 — `Uncertain` + `machine clear-grant --force`. Without this there is no exit from a lost-manager state, and `machine recover` is unreachable in exactly the case it exists for. The "stale-session release" and "TTL-release" mutants must still fail after adding it.
4. F5 — challenge-anchored freshness. Requires a wire change to `InvocationIntent`/`InvocationTicket`; do it now, while `machine_*` schema version 1 is still being introduced, rather than after MR-3.
5. F3 — one fence spelling. Pick scoped form everywhere and delete the flattened comparison in `authorize`.

**Should fix in the same slice** (they are wire/schema changes; deferring them costs a second version bump): F7 (config hash on the wire), F10 (`ReconcileAllocation` ticket states + manager snapshot primitive), F11 (`Inspect` pagination). §9 already requires "a local protocol version and schema fixture change" for MR-1/MR-2 additions — batch these into that one increment.

**Can follow, but must be tracked as known gaps, not silently deferred:** F8 (outbox drain as an upgrade precondition — at minimum add it to the §9 upgrade procedure text and to `doctor` prerequisites now, even if the `RawValue` change waits), F9 (registry compaction), F12 (aging anchor), F13(a)–(h).

**Two process notes:**

- Several of the above are *specification* gaps as much as code gaps: the protocol document asserts behaviour (§4 challenge-anchored freshness, §5 snapshot contents, §5 outbox bounds, §9 clearance) that the wire types cannot express. When fixing F5/F7/F10, update `machine-resource-protocol.md` in the same change so the harness has one authority, and regenerate the schema through the `schema-update` system Job per R-MR-6.
- The digest shape for `ReconcileCommit` (`payload_hash` over `Vec<Vec<ReconcileAllocation>>`, `machine_allocation.rs:819`) is an unwritten wire contract that any public fake manager must match byte-for-byte. Document it explicitly before writing the fake-manager harness, or the harness will encode the implementation's accident rather than a specification.

**Suggested test additions, smallest-first** (all runnable as isolated-instance tests against the existing fault-injection harness in `crash_boundary`):

| Test | Asserts | Currently fails |
|---|---|---|
| `outbox_accepts_specified_queue_depth` | 20 unanswered enqueues succeed; a 21st `seal_release` succeeds | F1 |
| `ticket_time_fence_conflict_blocks_native_holder` | attached alias→`X` is rejected while native holds `exclusive:X` | F3 |
| `bind_converges_after_coordinator_reset_with_unapplied_replies` | manager rebinds; retired range cannot launch | F2 |
| `unrecorded_rejection_never_advances_applied_sequence` | `accept` refuses a synthesised `Rejected` | F6 |
| `stale_challenge_ticket_is_refused_at_release_barrier` | 3 s transport delay → `consume_ticket` typed staleness error | F5 |
| `lost_manager_grant_becomes_uncertain_and_is_clearable` | gate closed → `clear-grant --force` → audited open | F4 |
| `config_change_allows_candidate_resynchronisation` | manager learns new hash and re-upserts | F7 |

---

## Confidence

**High — provable from the supplied files alone, no unsupplied code required:**
F1 (arithmetic is closed-form from `manager.rs:134-152`), F2 (`machine_reset.rs:81` vs `manager.rs:75`), F4 (exhaustive grep of state writers in the supplied set), F5 (`machine_ticket.rs:144-152` compares only coordinator-local values; `InvocationIntent` has no timestamp field), F7 (no reply type carries the hash), F8 (`pending` discards the stored hash), F9 (closed-form byte arithmetic), F10 (`manager.rs` public surface is complete in the file), F11 (`Inspect` SQL is exact-match), F12 (`machine_reset.rs:90-96` vs `machine_allocation.rs:656`), F13(a)(b)(c)(d)(g)(h).

**High for the demonstrated part, Medium for the extrapolation:**
F3 — the prefixed-vs-bare mismatch *inside* `authorize` is certain; whether native admission is symmetrically blind depends on the unsupplied caller of `machine_queue::remote_debits`. Even the certain part is a real exclusion hole at ticket time.

**Medium — depends on the transport layer:**
F6. If the transport already distinguishes the two rejection classes, this reduces to a type-safety recommendation. The `StoreResult<Reply>` signature makes the unsafe conversion the path of least resistance, which is why I am raising it at High severity despite the dependency.

**Low — cannot be resolved from this snapshot:**
F13(e) (masked by other fail-closed paths; the gate itself still fails open), F13(f) (entirely contingent on `quiet_stability`), and the head-of-line question in Evidence Gap 4.

**Things I deliberately did *not* call bugs**, having traced them and found them correct: the journal-before-SQL ordering and both crash sides in `reconcile_pending_machine_commit`; `seal_release`'s unanswered-`AuthorizeInvocation` gate; the `consumed`-implies-`user_code_released` conservatism in `record_cleanup`; `apply_release`'s ticket bijection and `previous_cleanup` cross-check; `complete_machine_reset`'s snapshot-equality TOCTOU guard; `dead()` accepting `PriorBoot`/`PidReused` in `recover_machine_authority`; `Withdraw` expiring an unarmed Offer without journaling; and the event-retention trigger bounds. The manager's refusal to consume a ticket whose session was fenced is conservative but matches §4.5's "no disconnect notification" precondition — I read it as intended, not as a defect.

**What would change my view:** on F3, a caller of `remote_debits` that re-expands through `ResourceTopology` before comparison would downgrade it to "ticket-time recheck only". On F6, a transport that returns a distinct non-`Reply` error frame would downgrade it to Low. On F13(f), a `quiet_stability` that resets on generation change would close it entirely. On F2, an existing manager-side recovery entry point outside `manager.rs` would downgrade it — but I found no such surface in the supplied set, and `bind`'s own doc comment ("the caller must first run full inventory recovery") names a caller obligation with no primitive to discharge it.
