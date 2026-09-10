# MR-2 Coordinator Protocol Review — Reviewer 3 (scheduling, reservations, accounting, limits, recovery liveness)

## Summary

The durable-ordering skeleton is sound: the external journal is written before the SQL commit, `reconcile_pending_machine_commit` closes both crash sides, `Armed` accounting survives TTL/disconnect, and the offer/reservation pass reuses the native `ReservationDecision` core so native and attached candidates share one rank order. The public fault tests cited in the brief cover the boundaries they claim to cover.

What I found instead is a cluster of **bound and limit defects that convert normal operation into permanent stalls**, plus **two accounting namespace gaps** at the ticket barrier. Specifically: the manager outbox can only hold ~14 unanswered operations before it refuses to enqueue a `Release`; `retired_allocations` grows monotonically inside a 4 MiB registry until `finish_machine_commit` fails *after* the SQL commit and gates the whole machine forever; `Outcome::Inspection` is committed and journaled before anyone checks it fits in a 1 MiB frame; and cross-domain fence aliases produce differently-shaped resource identities on the native side and the attached side, so they never exclude each other. None of these need a hostile actor, a crash, or WSL — they are reached by ordinary throughput and by an ordinary disconnect.

Six of these are, in my reading, blocking for MR-2 closure independent of MR-3.

---

## Findings

### F1 — Manager outbox byte budget blocks `Release` after ~14 unanswered operations — permanent stall on disconnect
**Severity: High (liveness + obligation retirement).**
`src/machine/manager.rs:134-152`.

The capacity query charges an unanswered entry `length(command_json) + COALESCE(length(outcome_json), 1048576)` — i.e. a full `MAX_FRAME_BYTES` reservation per unanswered reply — and the candidate entry adds another `MAX_FRAME_BYTES`, against a 15 MiB non-ack budget:

```
bytes.saturating_add(json.len()).saturating_add(MAX_FRAME_BYTES) > 15 * 1024 * 1024
```

Effective ceiling: **~14 unanswered operations**, not the 16,383/16,384 row limits alongside it and not the protocol's "16,384 queued outbox entries per domain" (protocol §5).

Event trace (no crash required):
1. Bridge disconnects; `pending()` cannot drain because replies stop.
2. Attempts settle locally. Each calls `seal_release` → `seal_release` writes `attached_grants.seal_json` (`manager.rs:445-448`) then calls `enqueue` (`:449`).
3. After ~14 pending entries, `enqueue` returns `History("outbox capacity reached…")`.
4. The caller's transaction — which per the module contract commits the seal *with* the Lease lifecycle — rolls back. The allocation stays unsealed, the Lease stays granted, the Grant stays `Armed` at the coordinator.
5. Every retry repeats step 3. Candidate advertisement is also blocked (same `enqueue`).

**Invariant violated:** protocol §5 "reaching a limit blocks new starts, never deletes obligations" — here the limit blocks *obligation retirement*, which is the one operation that must always be enqueueable. R-MR-4's "durable idempotent release outboxes provide recovery" fails closed into an unrecoverable state.

**Fix:** (a) budget by realistic reply size (`Outcome` replies other than `Inspection`/`Grant` are < 4 KiB) or track a separate reserved-reply counter; (b) exempt `Command::Release` and `Command::Withdraw` from the byte budget the same way `Acknowledge` is exempted, since they only retire rights; (c) enforce the row limits (16,383/16,384) as the real bound.
**Test:** disconnect a fake manager, settle 20 attempts, assert every `seal_release` succeeds and `attached_grants.seal_json` is set for all 20; assert the 16,384th enqueue is the first refusal.

---

### F2 — `retired_allocations` grows without bound inside a 4 MiB registry; overflow deadlocks the machine *after* the SQL commit
**Severity: High (permanent machine-wide admission deadlock).**
`src/authority.rs:41-42`, `:396-401`, `:1117-1126`; `src/store/machine_allocation.rs:242-249`.

`finish_machine_commit` inserts one 64-char hash per released allocation key into `retired_allocations`, a `BTreeSet<String>` serialized into `registry.json`, which `publish` refuses above `MAX_REGISTRY_BYTES = 4 MiB`. At ~67 JSON bytes per entry that is **~62,000 lifetime attached allocations** (one per Attempt *and* per retry *and* per probe), shared with `native_permissions` and `machine_permissions` in the same 4 MiB.

Event trace:
1. Registry crosses 4 MiB during a `Release`.
2. `machine_exchange` has already run `tx.commit()` (`machine_allocation.rs:242`). SQL says released.
3. `finish_machine_commit` (`:245-248`) → `publish` → size error → `?` propagates. `pending_machine_commit` is **never cleared**.
4. `admission_blocker` now returns `authority_commit_pending` (`authority.rs:899-902`), which closes native admission (`store/authority.rs:169-177`) and attached offering (`machine_queue.rs:229-231`).
5. Every subsequent call re-enters `reconcile_pending_machine_commit`, finds SQL consistent, commits an empty tx, calls `finish_machine_commit`, fails identically. There is no code path that clears the gate.

**Invariant violated:** R-MR-4 recovery liveness; also the size check is the *only* pre-write guard, so this is a silent cliff with no warning surface in `doctor`.

**Fix:** `lease_id` is already a fresh UUID minted per Attempt in the manager store, and `AllocationKey` is store- and epoch-qualified — the retired-key set is redundant against replay of a *new* key. Replace it with the per-domain retired-sequence floor that already exists (`ParticipantAnchor::retired_sequence_floor`), or at minimum bound it per `(domain, manager_store_uuid)` with the floor and drop entries below it. Separately: make registry-size pressure a `doctor` blocker well before the cliff, and make `finish_machine_commit` failure after SQL commit a distinguishable, clearable state rather than an unconditional permanent gate.
**Test:** synthesize 70,000 released allocation keys, assert `publish` never fails and the machine still admits; negative control: a mutant that keeps the unbounded set must fail.

---

### F3 — A committed operation can produce an unframeable reply — permanent per-domain head-of-line stall
**Severity: High (liveness, per-domain).**
`src/store/machine_allocation.rs:707-723`; `src/machine.rs:575-609`; `src/machine/manager.rs:174` (`pending` returns only the earliest unapplied row).

`Command::Inspect { key: None }` returns up to 256 `GrantSnapshot`s. A single grant may carry 256 `InvocationIntent`s (`machine_ticket.rs:48`), each ≈700 JSON bytes with two store-qualified IDs, two 64-char digests, a challenge UUID and `previous_cleanup` — ≈180 KiB per grant. Six such grants exceed `MAX_FRAME_BYTES`. There is **no size check on the reply anywhere in `machine_exchange`** (line 30 checks the *request* only).

Event trace:
1. Manager sends `Inspect{None}` at sequence *n*.
2. Coordinator runs `apply_command`, inserts `machine_operations` row, updates `accepted_sequence`, `tx.commit()`.
3. Transport calls `write_frame` on the reply → `InvalidData`. Reply never delivered.
4. Manager retries sequence *n*; the `prior` branch (`:90`) returns the identical recorded outcome; step 3 repeats forever.
5. `pending()` returns only the earliest unapplied operation, so **every later operation of that domain — including `Release` and `Acknowledge` — is stuck behind it.**

The same shape applies to `Command::ReconcilePage`: `Request::validate` bounds records (`machine.rs:561-565`) but not bytes, and `ReconcileBegin` caps `page_count` at 16 (`machine_allocation.rs:738`), so a domain whose allocations carry long postcondition ticket chains **cannot construct a valid reconciliation snapshot at all** → `reconciliation_required` stays 1 → permanently gated.

**Invariant violated:** protocol §3 framing bounds and §5 "Ordinary reconnect/released-ack loss automatically converges."

**Fix:** byte-budget the page the way `machine_events` already does (`machine_events.rs:193-202`): compute the encoded size *inside* `apply_command`, truncate with `truncated: true`, and reject with a durable `limit_exceeded` outcome if even one record cannot fit. Do the size check before the operation row is committed. Make `page_count` in `ReconcileBegin` a byte-aware negotiation, not a fixed 16.
**Test:** create a grant with 256 tickets ×8 allocations, `Inspect{None}`, assert the reply frames and `truncated=true`; assert the outbox drains afterwards.

---

### F4 — Ticket-barrier accounting uses a different comparator and a different resource namespace than the offer barrier; cross-domain fence aliases never exclude native work
**Severity: High (resource exclusion correctness, A-04 / M-A13 class).**
`src/store/machine_queue.rs:56-66` and `:8-14` vs `src/store/machine_ticket.rs:160-215`.

Two distinct accounting implementations:

| Barrier | Debit set | Comparator | Fence identity |
|---|---|---|---|
| Offer (`machine_offer_before_native:294-314`) | native leases and attached grants both put through `topology.expand` | `ScopedClaims::blockers(&topology, …)` | `ScopedId { scope, resource_id }` |
| Ticket (`machine_ticket::authorize:160-215`) | `native_debits` = **raw `leases.claims_json`**, plus attached `physical_claims_json` | `ResolvedClaims::blockers(&config.resources, …)` | native: bare `"C:\\build"`; attached: `"domain:<scope-uuid>:C:\\build"` (`machine_queue.rs:59,64`) |

`physical_claims` prefixes fences with `domain:{scope}:{id}`; `native_debits` returns the native lease's claims unmodified. Therefore **no attached fence string can ever equal a native fence string** in the ticket check or in `remote_debits` (`machine_queue.rs:17-23`), which is what native admission consumes.

Event trace:
1. Alias registration maps native `C:\scratch` and attached `/mnt/c/scratch` to one physical object (protocol §6 "cross-domain fences require a registered filesystem-object alias mapping").
2. Attached candidate is offered and armed holding the exclusive fence — the offer-time check correctly excluded a native holder via `topology.expand`.
3. A native Lease is granted for the same aliased fence: native admission subtracts `remote_debits`, whose fence strings are `domain:…`-prefixed and match nothing → **grant proceeds**.
4. The attached manager requests its ticket: `physical.blockers` compares its `domain:…` fence against the native lease's bare fence → **no blocker** → ticket issued.
5. Both trees execute under the same exclusive fence.

**Invariant violated:** R-RES-1 "One Lease grants the complete requested set atomically", R-MR-3 "Fences are domain-local unless aliases explicitly bind a shared filesystem object", protocol §4 step 4 "Coordinator samples/rechecks host readiness, config and **exclusions**".

Note impacts do line up (both bare), so the `measurement`/`cpu_heavy` exclusion in W-C4 is unaffected — this is specifically fences.

**Fix:** have exactly one comparator. The ticket recheck should expand the native leases and this grant through the same `ResourceTopology` used at offer time and call `ScopedClaims::blockers`, rather than flattening to `ResolvedClaims`. If a flattened form is required for `evaluate_request`, canonicalize *both* sides with the same `domain:{scope}:{id}` formatting.
**Test:** register an alias binding a native and an attached fence to one object; assert that arming the attached candidate makes the native admission block, and that a native grant taken first makes `AuthorizeInvocation` return a fence blocker. Negative control: the current prefix-mismatch mutant must fail.

---

### F5 — `record_cleanup` forces `user_code_released == consumed`; a consumed-but-never-released ticket can never be sealed → permanent Grant leak
**Severity: High (accounting retention with no release path).**
`src/machine/manager.rs:344-364` (`consume_ticket`), `:368-392` (`record_cleanup`), `:423-433` (`seal_release`).

`consume_ticket` sets `consumed=1` and the module contract says the runtime must *then* "check ticket session/freshness and local readiness while holding that barrier through actual OS release". So there is a real window where `consumed=1` and user code was never released (OS release failure; `machine.md` §7 "Suspend between quiet sample and release → …clean never-run boundary").

`record_cleanup` rejects `cleanup.user_code_released != consumed`. `seal_release` requires a non-NULL `cleanup_json` for **every** ticket of the allocation.

Event trace:
1. `consume_ticket` commits (`consumed=1`).
2. Resume notification / provider generation change arrives inside the barrier; the manager does not release. The boundary is proven empty with no user code.
3. Honest `record_cleanup { user_code_released: false }` → rejected.
4. Dishonest `record_cleanup { user_code_released: true }` → accepted, but the coordinator's `validate_next` (`machine_ticket.rs:90-105`) now treats the primary as released and only permits a **Postcondition** next, which R-RUN-8 forbids for a never-started primary.
5. Choosing (3): `seal_release` returns "ticket boundary is not proven empty" forever; the Grant stays `Armed`; the whole vector is retained with no path to `Release`.

**Invariant violated:** protocol §4.5/§7 never-run boundary handling; R-MR-4 "Sealed cleanup reports … provide recovery"; §4.7 "Sealed intent cannot be reopened."

**Fix:** split the two facts. Keep `consumed` as "start intent is spent" and add an explicit `TicketCleanup.disposition: { never_released, released }`, with `record_cleanup` accepting `never_released` when `consumed=1` provided the proof digest covers a never-populated boundary. Extend `validate_next`'s role table so `(Primary, Primary, never_released)` is a legal same-role, same-`role_index` successor with `release_sequence+1`.
**Test:** consume a ticket, fail the barrier, prove empty, seal and release; assert the Grant reaches `Released` and the coordinator's accounting drops the vector. Negative control: a mutant that silently reports `user_code_released=true` must fail the postcondition-launch assertion.

---

### F6 — No offer delivery mechanism; 5 s offer TTL against a strictly serial, fsync-per-operation outbox is a plausible offer livelock
**Severity: High (throughput/liveness); partially an evidence gap.**
`src/machine.rs:25` (`OFFER_MILLIS = 5000`), `src/machine.rs:457-504` (no `Offer`/`Poll` command), `src/store/machine_queue.rs:483-501`, `src/machine/manager.rs:166-186` (`pending` returns one row), `src/store/machine_allocation.rs:251-254` (registry publish on *every* operation, including rejections).

The coordinator creates offers unilaterally inside `machine_offer_before_native`. `Command` has no offer notification and no unsequenced query — the only way a manager learns of an offer is `Command::Inspect`, which is a fully sequenced operation that consumes an outbox slot, a `machine_operations` row, and an unconditional `checkpoint_machine_sequence` registry `publish` + `fsync` (`authority.rs:1128`). Protocol §3 says "Query and retry are connect-only."

Trace: offer created at *t* → manager must enqueue `Inspect`, wait for the head of its serial outbox to drain, receive the reply, then enqueue `Arm` and wait again — two full round trips with ≥2 coordinator fsyncs each — inside 5 s. On expiry, `machine_queue.rs:259-266` sets `not_before_ms = now + 5000`, so the candidate is barred for another 5 s, then re-offered, and the cycle repeats. `MAX_IN_FLIGHT = 64` and `MAX_QUEUED_BYTES` (`machine.rs:19-20`) are declared but referenced nowhere in the supplied code.

Compounding: `AuthorizeInvocation` during a quiet wait (`machine_ticket.rs:237-252`) must be *re-issued* per poll, each one a durable sequenced operation with a registry publish. With the W-C4 profile (`stable_seconds=2`, `max_sample_age_seconds=1`) that is several fsync-bearing operations per second for the entire quiet wait — directly at odds with the MR-3 idle budget (§9: six timer wakes/minute, < 0.5% CPU).

**Fix:** add an unsequenced, connect-only query/notification channel for offers and readiness polling (the `ProtocolRecord::Events(EventPage)` variant at `machine.rs:164` suggests one was intended); make `Inspect` connect-only rather than sequenced; make `checkpoint_machine_sequence` batch (publish at most once per N operations or on idle) instead of once per operation.
**Test:** W-C1-style two-slot concurrency run asserting complete Grant/start/release event traces with no `expired` offer transitions under sustained load; a polling mutant must fail the wake-count budget.

---

### F7 — Attached aging identity is destroyed by coordinator reset recovery; native aging is preserved
**Severity: Medium-High (cross-domain fairness, R-MR-3).**
`src/store/machine_reset.rs:65-96`.

Recovery runs against a **new** SQLite database. Native queue identity is regenerated from `jobs` by the backfill in `initialize_schema` (`store/machine.rs:39-40`) with the original `accepted_ms`. Attached queue identity is not: recovered candidates get `owner = "attached:{key}"` with `accepted_ms = grant.offered_unix_millis`, and those rows are permanently `withdrawn`/`released`. The next live `CandidateUpsert` for the same Job creates a fresh `remote:{job}` row with `accepted_ms = now` (`machine_allocation.rs:656-657`).

Result: after any coordinator reset, native work retains its full accumulated aging while every attached Job restarts at zero.

**Invariant violated:** R-MR-3 "Attached aging originates at first durable authority registration… All Attempts retain that first sequence/time"; protocol §6 same. Also "no domain head-of-line blocking" — this systematically favours one domain after every reset.

**Fix:** anchor the attached queue identity (`remote:{job}` → first `accepted_ms` and global sequence) outside the SQLite reset set — the `ParticipantAnchor` is the natural home — and restore it in `prepare_machine_reset_recovery` before any live upsert can mint a new one.
**Test:** register attached candidates, age them 10 minutes, force a reset+recover, assert their `effective_priority` and `accepted_ms` are unchanged relative to a native job accepted at the same original time.

---

### F8 — `readiness_challenge` carries no timestamp, so §4's "sample after challenge receipt" and "250 ms from challenge send" are unenforceable
**Severity: Medium-High (M-A11 stale-quiet-release class).**
`src/machine.rs:286` (`readiness_challenge: Uuid`), `src/store/machine_ticket.rs:144-152`, `:54-58`.

The challenge is a bare UUID used only for single-use uniqueness in `validate_next`. The coordinator's freshness check is *its own* 250 ms window from the host sample's capture:

```rust
monotonic.checked_sub(sample.captured_monotonic_millis).is_none_or(|age| age > 250)
```

Protocol §4 requires something stronger and different: "Host sample must be taken **after receipt of that challenge**. Accept a ticket only within the lesser of configured sample age and 250 ms **measured from the manager's challenge send**." Neither is checkable: there is no manager send-stamp on the wire, the coordinator cannot order its sample against the challenge, and §4 forbids comparing Windows and Linux clock values, so `issued_unix_millis`/`host_sample_unix_millis` in the returned ticket (`machine.rs:406-408`) give the manager nothing it can bound. The entire request→reply round trip is outside the 250 ms budget.

**Fix:** make `readiness_challenge` a struct carrying the manager's monotonic send stamp plus the nonce; have the coordinator record challenge-receipt monotonic time, require `sample.captured_monotonic_millis > receipt`, and echo the manager's stamp in the ticket so the manager can enforce `own_monotonic_now - stamp <= 250 ms` under the release barrier before `consume_ticket`.
**Test:** delay the reply artificially past 250 ms and assert the manager refuses to consume; suspend between sample and release and assert the ticket is invalidated and the never-run boundary is cleaned (this is the F5 case too).

---

### F9 — `GrantState::Uncertain` is unreachable, and there is no audited clearance for an attached Grant or for the reset gate
**Severity: Medium-High (MR-2 closure requirement, R-MR-4 / R-RUN-4 parity).**
`src/machine.rs:311-319`; searched all supplied coordinator paths — `Uncertain` is only ever *matched* (`machine_allocation.rs:565`, `:458-463`, `machine_queue.rs:18`, `machine_ticket.rs:161`), never *assigned*.

Consequences:
- A manager that can neither prove its boundary empty nor seal has no way for the coordinator to record "retained accounting, proof unresolved" distinctly from a healthy `Armed`. The `Inspect`/`doctor` surface therefore cannot name a reconciliation blocker per R-MR-6.
- There is no attached analogue of `doctor clear-containment ID --force`. `force_release_authority` (`store/authority.rs:225-234`) releases *holds* only — it cannot touch `machine_permissions` or `pending_reset`.
- The only exit from `pending_reset` is `finish_machine_reset_recovery`, which requires the full safe-recovery predicate. If any of its preconditions is structurally unsatisfiable (see F10), the gate is permanent with no operator escape.

**Invariant violated:** R-MR-4 "unresolved proof requires retained accounting **or explicit audited risk clearance**"; protocol §9 "Clearance identifies all affected grants and outstanding tickets and records operator identity plus explicit risk acceptance."

**Fix:** add an explicit coordinator transition `Armed → Uncertain` (driven by reconciliation timeout / manager-reported unresolved proof), surface it in `Inspect`/events, and implement an audited `doctor machine clear-grant <GrantId> --force` and `doctor machine clear-reset --force` that write the same style of audit record `complete_machine_reset` already writes (`authority.rs:586-598`) and never silently delete the registry.

---

### F10 — In-place upgrade with running work sets `pending_reset` on a store that can never satisfy the recovery predicate
**Severity: Medium-High (upgrade recovery liveness, R-MR-4).**
`src/store/authority.rs:69-74`; `src/authority.rs:293-316`, `:572-575`; `src/store/machine_reset.rs:18-22`.

`establish_native_coverage` runs at every `attach_authority`. If `native_coverage_store` is `None` (any pre-MR-2 registry) and the store is **not** empty, it calls `record_reset("native start history predates external coverage and is not empty")`.

Now the only exits both require `native_coverage_store == Some(history.store_uuid)`:
- `prepare_machine_reset_recovery` returns early at `machine_reset.rs:20-22`.
- `complete_machine_reset` rejects at `authority.rs:573`.

Since coverage was never established, **neither can ever be satisfied**. The machine is permanently `authority_reconciliation_required` with no implemented clearance (F9).

Event trace: alpha.16 → alpha.17 in place, one job running, daemon restarts → gate set → every subsequent start attempt blocked forever.

**Invariant violated:** R-MR-4 "Upgrade MUST prevent new admission and preserve control over all accepted work before replacement/reset; an empty sampled queue is insufficient" — the implementation instead makes a non-empty upgrade unrecoverable rather than gated-and-drainable.

**Fix:** on first attach with `native_coverage_store == None` and non-empty work, establish coverage from the *existing* SQL inventory (every granted lease + non-empty containment becomes a `NativeStartPermission` reconstructed from the recorded root/creator identities) rather than gating; gate only when those identities are missing. Pair this with the audited clearance from F9 as the backstop. Document the maintenance-gate-then-drain upgrade sequence from protocol §9 as a prerequisite and enforce it in the installer.
**Test:** populate a store with a running job and no `native_coverage_store`, attach, assert either successful coverage reconstruction or a gate with a working documented clearance — never an unsatisfiable predicate.

---

### F11 — Coordinator sequence rollback after reset leaves the manager permanently unable to bind; no resynchronization path
**Severity: Medium-High (recovery liveness).**
`src/machine/manager.rs:59-99`; `src/store/machine_allocation.rs:251-254`; `src/store/machine_reset.rs:72-86`.

`checkpoint_machine_sequence` runs **after** `tx.commit()` for non-`prepared` operations. A crash in that window leaves `anchor.accepted_sequence` behind SQL. `validate_machine_history` repairs this while the SQL survives (`store/machine.rs:132-135`), but after a whole-DB reset the SQL truth is gone and `prepare_machine_reset_recovery` restores `accepted = anchor.accepted_sequence.max(pending…)` — one or more operations *behind* what the manager already applied.

Then:
- Manager `bind` rejects on `participant.accepted_sequence < applied` (`manager.rs:72`).
- Coordinator `machine_exchange` rejects the manager's next request on `participant.accepted_sequence.checked_add(1) != Some(request.request_sequence)` (`:109`).
- `retire_machine_sequences(domain, accepted)` has already pinned a floor at the stale value, so the manager's own `Acknowledge` cannot bridge the gap.

Neither side can move. `bind`'s doc comment says "the caller must first run full inventory recovery", but no such manager-side recovery exists in the supplied module.

**Fix:** journal `accepted_sequence` in the same external write as the operation for *all* operations (make `prepared` unconditional, or write a lightweight sequence-only record before `tx.commit()`), and add an explicit manager-side resynchronization: on a detected coordinator rollback, run `ReconcileBegin/Page/Commit` against the coordinator's advertised `accepted_sequence` and adopt it as the new `applied`, retaining every unacknowledged obligation.
**Test:** crash between `tx.commit()` and `checkpoint_machine_sequence`, reset the coordinator store, run `machine recover`, reconnect the manager, assert convergence with zero obligations lost.

---

### F12 — `prepare_machine_commit` intent can exceed the 4 MiB registry on a multi-release `ReconcileCommit` → reconciliation never commits
**Severity: Medium.**
`src/store/machine_allocation.rs:219-224`, `:232-239`; `src/authority.rs:1117-1126`.

`Outcome::Reconciled { released }` loads a `GrantSnapshot` per released key into `intent.grants`, all of which are serialized into `registry.json`. With up to 4,096 allocations per domain and grants carrying ticket histories, the intent readily exceeds 4 MiB. `publish` fails *before* `tx.commit()` (good — fail-closed), but the manager retries the identical `ReconcileCommit` forever and `reconciliation_required` never clears, so the domain stays permanently gated.

**Fix:** journal released allocations as identity-only records (`grant_id`, `allocation_key`, `sealed_sequence`, seal digest) rather than full snapshots; the full snapshot is already in SQL and the journal only needs to prove the retirement decision. Alternatively chunk `ReconcileCommit` into bounded release batches.
**Test:** reconcile 2,000 sealed allocations in one commit; assert it succeeds and `reconciliation_required` reaches 0.

---

### F13 — Nondeterministic tie order among allocations of the same attached Job causes reservation churn
**Severity: Medium-Low (fairness/stability).**
`src/store/machine_allocation.rs:652-657`; `src/store/machine_queue.rs:272-293`, `:355`.

Queue identity is per-Job (`remote:{job}`), so a Job's work allocation and its probe allocation share `accepted_ms`, `sequence` and `priority` — identical `ScheduleKey`. The `ready` query has **no `ORDER BY`**, and `sort_by(schedule_order)` is stable, so their relative order is whatever SQLite returns that pass and can differ between passes. In the reservation prefix-retention loop (`:358-387`), a flip decides which of the two keeps its reservation, and the loser gets `not_before_ms = now + 5000`. Under contention this alternates.

**Fix:** add `ORDER BY allocation_key` to the ready and reservation queries and make `allocation_key` (or the grant's `lease_id`) the final tiebreaker in the comparison, so the prefix is deterministic across passes.
**Test:** two same-job allocations competing for one scalar; assert 100 consecutive passes select the same retained reservation.

---

### F14 — `enqueue` silently returns success for an already-answered operation
**Severity: Medium (API hazard; can hang a caller).**
`src/machine/manager.rs:120-132`.

The prior-operation branch returns `Ok(sequence)` whenever the payload hash matches, without distinguishing "queued, will be sent" from "already answered, will never be sent" (`pending()` filters on `outcome_json IS NULL`). A caller that retries a durably-rejected operation — e.g. re-requesting an `AuthorizeInvocation` after a `quiet_waiting` rejection — with the same operation UUID gets `Ok`, enqueues nothing, and waits forever.

**Fix:** return an enum (`Queued(seq)` / `Answered(seq, Outcome)`) so the caller must handle the answered case explicitly.
**Test:** enqueue → accept a `Rejected` outcome → re-enqueue the same operation ID; assert the caller observes `Answered` and must mint a new operation ID.

---

## Evidence Gaps

These materially affect two findings and I could not close them from the supplied files:

1. **`crate::admission::ResourceTopology::expand`, `ScopedClaims::blockers`, `ResolvedClaims::blockers`, `reservation_decision`, `outranks`, `schedule_order`, `effective_priority_at`, `machine_capacities`, `scalar_claim_entries`** are all referenced but not supplied. F4 rests on the *shape* difference between `physical_claims`'s `domain:{scope}:{id}` fence strings and `native_debits`'s raw lease claims, which is visible in the supplied lines — but the exact aliasing semantics of `expand` would determine whether the offer-barrier path is also affected or only the ticket/native-admission paths.
2. **The caller of `machine_offer_before_native`** is not supplied. Whether it is invoked once per scheduling pass with the head native job, or iterated in rank order across native candidates, determines whether the `break` at `machine_queue.rs:390-392` is correct scanning or genuine domain head-of-line blocking under R-MR-3. **This is the single highest-value gap** — if it is called only for the head, attached candidates below an inadmissible native head are starved, which is exactly the M-A02/A-MR-1 negative control.
3. **The native admission path** (which consumes `machine_queue::remote_debits` and `machine_reservation::remote_debits`) is not supplied; I inferred the `granted + offered + reserved` composition from the debit helpers' shapes.
4. **The transport/bridge layer** is not supplied, so `MAX_IN_FLIGHT` and `MAX_QUEUED_BYTES` may be enforced there; in the supplied code they are unreferenced constants. Likewise the actual reply-framing site referenced in F3.
5. **The manager runtime** that calls `consume_ticket` / `record_cleanup` is not supplied, so the exact ordering of the freshness recheck relative to `consume_ticket` (F5's window width) is assumed from the module doc comment at `manager.rs:341-343`.
6. **`crate::host_observation::{quiet_budget, quiet_stability, evaluate_request}`** are not supplied; F6's polling-cost argument depends only on the per-operation durability path, which *is* supplied.
7. The brief states the fake-manager public integration slice is not claimed passed. Nothing in the supplied unit tests (`machine.rs:615-755`, `machine_events.rs:223-286`) exercises the queue, reservation, or accounting paths — the offer/reservation loop in `machine_queue.rs` has no supplied test coverage at all.

---

## Recommendations

**Blocking for MR-2 closure**, in dependency order:

1. **F1, F2, F3, F12 — fix the bounds before anything else.** All four are "normal operation reaches a limit and the machine never recovers." Each needs an executable negative control in the A-MR-2 harness: a mutant that keeps the current bound must fail. These are cheap fixes with large blast radius.
2. **F4 — unify the two comparators.** One `ScopedClaims`-based accounting function used at offer, ticket and native admission. This is the invariant A-04 and M-A13 actually test; two implementations will drift again.
3. **F5 + F8 together — the never-run boundary.** They are the same scenario (§7 "Suspend between quiet sample and release"). Add the `disposition` field, the timestamped challenge, and one M-A11 harness covering consume → invalidate → clean → seal → release.
4. **F9 + F10 — implement the clearance and the upgrade path.** Right now several ordinary events produce a gate with no exit. At minimum, ship the audited `doctor machine clear-*` before claiming R-MR-4.
5. **F11 — make the sequence checkpoint atomic with the SQL commit**, and implement the manager-side inventory recovery that `bind`'s doc comment already promises.
6. **F6 — resolve the offer-delivery design** before W-C1. Without a push or connect-only query, the 5 s offer TTL and the per-operation fsync path make the two-cargo-slot concurrency evidence hard to obtain, and the MR-3 idle budget hard to meet.
7. **F7, F13, F14** — smaller, but F7 is a visible cross-domain fairness violation and should land with the reset-recovery work.

**Additional test coverage I would require before calling MR-2 done**, beyond the fault points already passing: a sustained-throughput harness (thousands of allocations) that exercises F1/F2's limits, and a deterministic multi-pass scheduler test asserting the complete ordered scan across native and attached candidates with an inadmissible head (F13 + the gap-2 question).

---

## Confidence

- **High** in F1, F2, F3, F12, F13, F14 — each follows from arithmetic or control flow entirely within the supplied lines, and I have given the exact expressions.
- **High** in F5, F9, F10, F11 — these are structural (an unreachable enum variant, an unsatisfiable predicate, a missing code path), verifiable by reading the supplied files.
- **Medium-High** in F4 — the fence-string shape mismatch is directly visible (`machine_queue.rs:59,64` vs `:8-14`); my claim about which barriers are affected depends on `ResourceTopology::expand`, which I have not seen. If `expand` already returns machine-scope physical fence identities for aliased entries, the offer barrier is fine and only the ticket and native-admission barriers are wrong — the finding stands either way, its scope narrows.
- **Medium-High** in F8 — the protocol text is unambiguous and the wire type plainly cannot carry the required information; I am assuming there is no out-of-band timing channel in the unsupplied transport.
- **Medium** in F6 — the durability cost per operation and the absence of an offer push are established from the supplied code; whether this produces an actual livelock depends on the unsupplied transport and scheduling cadence. I would treat it as a design question to answer rather than a confirmed defect.
- **Medium** in F7 — the reset path clearly regenerates attached queue identity from grant data; I am assuming `machine_queue.accepted_ms` is the sole aging origin, which matches `remote_reservations` and the ready query.

Questions that would most change my view: (a) how `machine_offer_before_native` is driven — once per pass or per native candidate; (b) whether `ResourceTopology::expand` canonicalizes aliased fences to a shared scope; (c) whether the unsupplied transport enforces `MAX_IN_FLIGHT`/`MAX_QUEUED_BYTES` and whether any unsequenced query channel exists.
