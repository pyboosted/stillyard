# MR-2 Domain Retirement — Focused Review (atomicity / auth / crash & SQL-reset safety)

*Read-only review of the supplied excerpts. No tool access; I could not open files outside the curated context, so line numbers are unavailable and I cite function names within files.*

## Summary

The retirement core is sound on its primary axis. The publication order in `Store::retire_machine_domain` matches the design (audit blob → registry fence → SQL commit → external finish → ack), the fence is the `retired_domains` tombstone rather than the debit release, all `machine_permissions` stay charged until `finish_domain_retirement`, and the four crash boundaries are individually replayable and idempotent. The digest binding is genuinely tight: `domain_clearance_preview` refuses to render while a machine commit is pending, so no Grant can enter `machine_permissions` between preview and fence without changing `sha256`, and the preview is recomputed under the same authority lock immediately before `publish`. The new `missing_or_changed_pending_retirement_audit_keeps_authority_closed` test is a real negative control: a missing or byte-shifted audit closes admission with `epoch: None` rather than proceeding.

Three things stand out as needing correction before clearance:

1. The new receipt-space reservation is keyed on the wrong growth predicate and can be silently consumed, so the guarantee it was added to provide ("already admitted obligations retain retirement space") is not actually enforced.
2. `apply_domain_retirement` fails hard from inside `reconcile_pending_domain_retirement`, which is the *first* statement of every Store entry point including `prepare_machine_reset_recovery` — so one divergence class deadlocks the recovery command itself.
3. A retired manager whose store later reappears cannot learn, authenticated, that it was retired; it is told "continuous pairing anchor not found", indistinguishable from "never paired".

On the design's own question: **whole-domain revocation is the smallest defensible scope for manager-store loss**, and **yes, the two open stuck states have safe explicit recovery without a broad force-reset** — by adding a second *clearance scope* built from the machinery already here, not a reset verb. Precise construction in Recommendations.

---

## Findings

### F1 — Receipt reservation is keyed on `admission_bytes`, which omits three growing terms; reserved retirement space can be consumed after admission — **Medium**

`src/authority.rs::check_publication` only evaluates the budget when admission grows:

```rust
if admission_bytes(registry)? > admission_bytes(old)?
    && (encoded.0.len() as u64 > MAX_REGISTRY_BYTES - REGISTRY_COMMIT_HEADROOM
        || encoded.0.len() as u64 + retirement::reserved_retirement_bytes(registry)
            > MAX_REGISTRY_BYTES - 256 * 1024)
```

`admission_bytes` covers only *unreleased* holds, participant registrations, `machine_permissions`, `native_permissions`. Three registry members grow the encoded file without ever tripping the predicate:

- **Released-hold metadata.** `force_release` sets `release_reason` (≤1024 chars, up to ~6 KiB after JSON escaping of control characters) plus `released_by`, and simultaneously *removes* the hold from `admission_bytes` (`.filter(|h| !h.released)`). So `admission_bytes` strictly **decreases** while the file grows ~6 KiB. `MAX_HOLDS = 1024`.
- **`retired_allocations`** — 67 bytes per released allocation key, added by `finish_machine_commit`, removed only by `compact_machine_retirements`, which requires the coordinator SQL to already carry matching Released tombstones. Not in `admission_bytes`.
- **`ParticipantAnchor::session`** — ~250 B per participant, not in `admission_bytes`.

**Failure trace.** Participants are admitted up to the guard's ceiling, so `len + 16 KiB·N ≈ MAX − 256 KiB`. The operator then takes and force-releases ~45 bootstrap holds with long reasons (or accumulates ~4000 uncompacted `retired_allocations`), adding >256 KiB of unbudgeted bytes. A manager is lost. `prepare_domain_retirement` → `check_publication` → `encode_registry` now returns `InvalidInput("authority registry exceeds durable byte budget; retire existing obligations")` at the hard `MAX_REGISTRY_BYTES` cap. There is no implemented way out: released holds cannot be deleted (design §8), `retired_allocations` cannot be compacted without SQL proof from the manager that is gone, and `retired_domains` cannot be deleted. The single operation the reservation exists to guarantee is the one that fails.

Mitigating: `check_publication` errors *before* `self.state = State::Unknown`, so this fails the operation rather than the authority. That is correct fail-closed behaviour, but it is a permanent liveness dead end.

**Minimum correction.** Either (a) add the omitted terms to `admission_bytes` — released holds' `release_reason`/`released_by`/`cleanup_proof`, `retired_allocations`, and `session` — so any growth trips the guard; or (b) change the predicate from "admission grew" to "encoded length grew", exempting only the publications that must always succeed (`record_reset`, `finish_domain_retirement`, `finish_machine_commit`, `retire_native_starts`, `compact_machine_retirements`), all of which shrink or grow by a few hundred bytes covered by the 256 KiB slack. (a) is the smaller change and preserves the existing structure.

**Test.** The existing `saturated_registry_still_journals_large_release_and_detects_missing_blob` never exercises the reservation's *rejection* path: the loop stops at `MAX − 1 MiB − 1 KiB` and there is exactly one participant, so `len + 16 KiB` is nowhere near `MAX − 256 KiB`. Needed control: saturate to just under `MAX − 256 KiB − 16 KiB·N` with N participants; assert (i) `prepare_pairing` for participant N+1 is rejected with the budget error, (ii) force-releasing 64 maximal holds does **not** subsequently prevent retiring all N participants in sequence, each succeeding.

---

### F2 — `apply_domain_retirement`'s hard `InvalidState` deadlocks every entry point, including `machine_recover` — **Medium**

`src/store/machine_clearance.rs::apply_domain_retirement` returns `StoreError::InvalidState` on two divergences:

```rust
} else if grant.state != GrantState::Offered {
    return Err(StoreError::InvalidState(
        "SQL contains start rights outside the accepted retirement inventory".into()));
```
and `"domain retirement SQL audit differs"`.

This runs inside `reconcile_pending_domain_retirement`, which is the **first statement** of `retire_machine_domain`, `machine_clearance_preview`, `pair_machine_domain`, `machine_connect_begin`, `machine_connect_finish`, and — critically — `prepare_machine_reset_recovery`:

```rust
pub(crate) fn prepare_machine_reset_recovery(&mut self) -> StoreResult<crate::AuthoritySnapshot> {
    self.reconcile_pending_domain_retirement()?;
```

**Failure trace.** Crash at the `after_retirement_journal` boundary (pending retirement durable, SQL untouched). Operator restores `stillyard.sqlite3` from a backup taken while grant *G* of that domain was still `armed`, but *G* had since been released and dropped from `machine_permissions`, so it is absent from the pinned preview. On restart, every RPC — `machine-recover`, `clearance-preview`, `retire-domain` — returns `store_error: SQL contains start rights outside the accepted retirement inventory`. The pending retirement can never finish and the reset gate can never be reached, because the reset command is gated behind the same reconciliation.

The window is narrow (requires a crash *and* a rollback), and same-UUID rollback is already listed as separately open — but the specific consequence here is different from that item: it makes the recovery *entry point itself* unreachable, which the separate remediation cannot fix from outside.

**Minimum correction.** These divergences should gate, not abort: replace the `return Err(...)` with `authority.record_reset("SQL retirement inventory diverges from the accepted clearance")` and `Ok(())`, leaving the retirement pending and admission closed but every entry point callable. Alternatively, treat out-of-inventory rows for an already-tombstoned domain as `Expired` with the clearance annotation — defensible, since the tombstone has already ended that registration and it can never acquire rights again — but the gating option preserves the "never silently release what the owner did not see" property.

**Test.** Crash at `after_retirement_journal`; swap in a SQLite file containing one extra `armed` grant for the retiring domain; assert `machine_recover` and `clearance-preview` still return, that the authority reports a reset gate, and that no grant was released.

---

### F3 — No evidence that pending retirement is reconciled at daemon startup; native admission may stay gated until an administrative RPC arrives — **Medium (exit criterion)**

`admission_blocker` returns `authority_retirement_pending` ahead of every other blocker, and the fixture confirms admission really is closed during that window (`assert!(!temp.path().join("after-retirement.txt").exists())`). `reconcile_pending_domain_retirement` is called only from the six Store methods listed in F2 — all administrative or executor-facing. In the crash fixtures, the unblocking call is always an explicit `client.retire_machine_domain(...)` from the test.

If `Store::open` / daemon startup does not call `reconcile_pending_domain_retirement` alongside `reconcile_pending_machine_commit`, then a crash at `after_retirement_journal` leaves a machine whose *native* Job admission is blocked until an operator happens to re-issue an administrative request. That does not violate "a requested clearance is followed through crash recovery without another **risk decision**" (no new `accept_risk` is required), but it does require another administrative round trip to restore unrelated native scheduling, which I read as outside the intended recovery contract.

**Test (indispensable).** Crash at `after_retirement_journal`, restart the daemon, then submit **only** a native Job with no administrative RPC of any kind; assert it is admitted and completes. The current fixture cannot detect this because it always calls `retire_machine_domain` before `client.wait(waiter, ...)`.

*Marked as a gap rather than a confirmed bug: I do not have `src/store/mod.rs` or `src/daemon/mod.rs` startup paths.*

---

### F4 — A retired manager cannot learn, authenticated, that it was retired — **Medium (exit criterion)**

`machine_connect_begin` resolves the registration from `authority.participants()`, which `finish_domain_retirement` has emptied of the retired anchor:

```rust
.ok_or_else(|| rejected("history_unknown", "continuous pairing anchor not found"))?
```

So a manager whose store is later restored from backup and reconnects receives `history_unknown`, byte-identical to the response for a store that was never paired. `is_retired_domain` is consulted in `reserve_machine_connection`, `accept_machine_session` and `proposed_machine_commit`, but those are all reached *after* the anchor lookup already failed.

Why this matters for MR-2 acceptance: design §4 says "Public participant/Grant/event surfaces expose retirement and its audit identity", and §3 says "The old manager remains a historical store; it cannot turn absence from a new inventory into a fresh launch permission." The manager side does fail closed — `manager/recovery.rs::import_inventory` refuses with `"coordinator omitted an unsealed local allocation"` rather than accepting an empty inventory as release — but that message describes a coordinator fault, not a completed owner risk decision. A restored manager cannot durably record "my obligations were risk-cleared by operation X" and therefore cannot suppress local work that assumed its cleanup contract was intact.

**Minimum correction.** Add a distinct rejection code (e.g. `retired`) at the anchor lookup in `machine_connect_begin` (and in `machine_exchange`, if it does not already reject retired sessions before serving `Inspect`), carrying `operation_id`, `audit_sha256` and `retired_unix_millis` from `retired_domain_receipt`. That surface is already implemented for `Request::MachineParticipant`; it just is not reachable on the manager's own connect path.

**Test.** Retire a domain, then run `connect-begin` with the original `ConnectHello` and assert the error carries the retirement operation ID — the fixture currently only asserts `!status.success()`.

---

### F5 — Retrying an already-retired domain under a fresh `operation_id` yields `"paired domain is absent"` — **Low**

In `prepare_domain_retirement`, when the audit file for the new operation does not exist, the `else` branch computes `self.domain_clearance_preview(request.domain_id)?` **before** the `registry.retired_domains.get(&request.domain_id)` check. After `finish_domain_retirement` removed the participant, `domain_clearance_preview` fails with `io::Error::other("paired domain is absent")` → `StoreError::Io` → generic `store_error`. The well-crafted `retirement_conflict` / `"domain was already retired by another operation"` path below it is unreachable in that case.

The fixture only covers the same-`operation_id` conflict (`conflict.reason.push('!')`), which does hit the intended path.

**Minimum correction.** Move the `retired_domains` lookup above the audit-file branch, and short-circuit on `is_retired_domain` with `ErrorKind::InvalidInput` so the caller receives `retirement_conflict` with the original receipt's operation ID.

---

### F6 — Authentication: two unlinked SID formatters, live-token binding, and inconsistent gating across the three clearance-related RPCs — **Low–Medium**

In `src/daemon/rpc.rs::handle_request`, `Request::MachineRetireDomain`:

```rust
let principal = crate::instance::process_user_sid_string(peer.handle)...;
if principal != current_user_sid_string()? { ... }
```

Three observations, all within the stated "cooperative machine owner" model:

- **Format drift.** `process_user_sid_string(handle)` and `current_user_sid_string()` are separate functions compared for exact string equality. If one ever normalises differently (case, `S-1-5-21-…` vs account name), retirement becomes permanently impossible and the failure looks like an authorization denial. There is no unit test pinning `process_user_sid_string(GetCurrentProcess()) == current_user_sid_string()`. The fixture only asserts `receipt.requester_principal.starts_with("S-1-")`.
- **Live-token binding.** The comparison is against the daemon's *current* token user, not a durable owner identity recorded in the authority at `initialize()`. If the daemon is later reinstalled under a different account (e.g. as a service), every existing registration becomes unretirable, and the audit records a principal that is not the one that established the authority. Design wish, not a bug — but it is exactly the situation retirement exists to survive.
- **Asymmetric gating.** `MachineRetireDomain` requires unmanaged peer + SID match. `MachineClearancePreview` — the *input to the risk decision*, returning the full inventory including `SessionIdentity` — requires only `authority_admin` (unmanaged peer), no SID match. `Request::MachineParticipant` has **no** peer check at all: it locks the store directly with no `authority_admin`/`bootstrap_bridge` wrapper. No pairing secret leaks through any of these (`DomainClearanceInventory` excludes `PairingRegistration::secret`), so this is disclosure-consistency rather than a capability escape.

Also worth stating plainly in the acceptance evidence so it is not over-claimed: `authority_admin` rejects peers whose `submission_context().parent` is `Some`, so the effective authorization is *"any same-user process the daemon does not currently identify as managed"* — the same bar as `AuthorityForceRelease`. That is appropriate for the stated threat model; it should just not be described as a stronger boundary than it is.

---

### F7 — `machine_domain_retirements` DDL lives inside the retirement transaction; schema version not advanced — **Low**

`apply_domain_retirement` runs `CREATE TABLE IF NOT EXISTS machine_domain_retirements(...)` inside the `IMMEDIATE` transaction, rather than in `machine::initialize_schema`. Consequences: the table does not exist in a fresh store until the first retirement (any future read path must tolerate that), and `machine_meta.schema_version` stays `'1'` although the shape changed, so an older executable would not reject a database it does not fully understand — the stated protection in the `initialize_schema` comment.

Positively: `prepare_machine_reset_recovery`'s wipe batch does **not** include `machine_domain_retirements`, so receipts correctly survive a coordinator SQL rebuild.

**Minimum correction.** Move the DDL into `initialize_schema` and bump the machine schema version.

---

### F8 — Retirement leaves several unreclaimed and one mislabelled record — **Low**

- `risk_clearance = Some(operation_id)` is stamped on `Offered → Expired` grants that carried no launch authority and therefore no risk. `machine_events` exposes `risk_clearance`, so an auditor reading the event stream cannot distinguish an expired offer from a risked release. Set `risk_clearance` only on the `Armed`/`Uncertain` → `Released` transitions.
- `machine_queue` rows (`remote:{job}`) for the retired domain's candidates are never removed; `machine_readiness` rows for canceled candidates are never removed. The fixture demonstrates that *capacity* is correctly freed (the `cargo_slots: 1` native waiter succeeds after retirement), so this is dead weight rather than a leak of scheduling capacity — but it is unbounded across repeated retirements.
- `machine_domains` rows for retired domains are deliberately retained with `reconciliation_required=0` (correct — `finish_machine_reset_recovery`'s `covered` check would otherwise never pass), and `machine_ticket_identities` are correctly retained as the invocation/containment uniqueness fence. Both worth a comment so a future cleanup does not remove them.
- `domain-retirement-{op}.json` audits are never garbage-collected, and `prune_machine_blobs` correctly skips them (the fixture asserts `path.exists()` after finish). Aggregate audit bytes are bounded only by `MAX_RETIRED_DOMAINS × MAX_MACHINE_JOURNAL_BYTES` = 4096 × 32 MiB. Intentional per design §8, but the total should be documented as an operational disk requirement.

---

### F9 — The 12 KiB receipt ceiling is a permanent bar to retiring a domain — **Low**

```rust
if serde_json::to_vec(&receipt)?.len() > 12 * 1024 {
    return Err(std::io::Error::other("authenticated retirement receipt exceeds reserved metadata budget"));
}
```

The reservation arithmetic checks out: `reason` ≤ 1024 chars worst-cases to 6144 bytes under `\u0001` escaping (which the saturated-registry test correctly exercises), `requester_principal` ≤ 256, hashes fixed — comfortably inside 12 KiB, and 16 KiB covers the receipt plus the `RetiredDomain` wrapper and map key without double-escaping (the value is nested JSON, not a string field, so the comment's "including JSON escaping" is conservative). The one unbounded contributor is `ProcessIdentity::Windows { host_id, boot_id, .. }`, which is not length-validated anywhere in `authority.rs`. If either exceeds the slack, that domain becomes permanently unretirable — a hard rejection where truncation-with-hash would be safe, since the full identity is preserved in the immutable audit file regardless.

**Minimum correction.** Bound `host_id`/`boot_id` at `load`/`hold` time, or truncate them in the *receipt* (keeping the audit authoritative) rather than refusing the operation.

---

## Evidence Gaps

Items where I could not reach a verdict from the supplied context. Each would change a finding.

1. **Startup reconciliation order (blocks F3).** I do not have `Store::open` or the daemon startup path. Two questions: (a) is `reconcile_pending_domain_retirement` invoked there? (b) does `validate_machine_history` run *before* it? If (b) is yes, then a restart after the `after_retirement_sql` boundary would compare registry `machine_permissions` (still charged, `Armed`) against SQL (`released`), mismatch, and call `record_reset("SQLite Grants and external start permissions are not the same inventory")` — spuriously gating the coordinator during ordinary retirement recovery. The fixture covers that boundary and the brief states it passed, so the order is presumably safe; I am flagging it because it is a one-line ordering dependency with a severe consequence and it is not asserted anywhere in the tests I can see. A direct assertion (`authority_status().blocker` is `None`, not `authority_reconciliation_required`, after restarting from `after_retirement_sql`) would pin it.
2. **`machine_exchange` handling of retired domains.** Not supplied. `proposed_machine_commit` rejects mutating commands from a retired domain, but I cannot tell whether a read-only `Command::Inspect` from a retired session is rejected or served an empty inventory. Bears directly on F4.
3. **`peer.handle` provenance.** Whether it is bound to the connecting pipe client with creation-time verification (as `ProcessIdentity::Windows { pid, creation_filetime_100ns }` suggests) or opened by PID. Bears on F6.
4. **Authority mutex sharing.** `Store::authority: Option<Arc<Mutex<Authority>>>` is locked separately three times inside `retire_machine_domain`. If nothing outside the Store mutex ever holds it, the sequence is effectively atomic; if a reconciliation or scheduler thread can take it independently, the gap between `prepare_domain_retirement` and `finish_domain_retirement` is interleavable. The brief asserts serialization by the Store mutex, so I assume the former.
5. **Scheduler queries over retired-domain rows** (`machine_queue.rs`, `machine_ticket.rs` not supplied) — bears on F8's severity.
6. **`current_user_sid_string` / `process_user_sid_string` implementations** — bears on F6.

---

## Recommendations

### Answer to the design's explicit question

**Is whole-domain revocation the smallest defensible scope?** Yes, for manager-store loss specifically. Every narrower unit — Grant, Ticket, allocation key — is namespaced by `manager_store_uuid` and can only be authenticated by the `PairingSecret` continuity that is, by hypothesis, gone. A per-Grant clearance would ask the owner to attest to an inventory that nothing can corroborate. The domain is the smallest object with an independently verifiable identity (`InstallationIdentity` + `manager_store_uuid` + the anchor), and the code enforces exactly that: `domain_clearance_preview` requires a participant anchor, and retirement removes the whole anchor rather than editing it. This should be stated as the rationale in §1 of the design, because it is currently asserted rather than justified.

### Can MR-2 exit safely without a broad force-reset? — precise answer

**Yes.** The two remaining stuck states are the ones `prepare_machine_reset_recovery` bails out of:

```rust
if history.store_uuid == self.store_uuid
    || snapshot.native_coverage_store != Some(history.store_uuid)
{ return Ok(snapshot); }
```

Neither needs a reset verb. Both are the same *shape* as manager-store loss and should reuse the retirement machinery as a second and third clearance **scope**:

**(a) Native-obligation clearance** (for `native_coverage_store` mismatch / uncovered native history). Add `machine clearance-preview --native` returning the exact `native_permissions` inventory — `invocation_id`, `containment_id`, `allocation.key`, and the recorded root identity — digest-bound exactly as `DomainClearancePreview` is. Add `machine clear-native --spec` taking `{operation_id, expected_inventory_sha256, invocation_ids: [...], reason, accept_risk}`. It must be **per-InvocationId and digest-pinned**, so it structurally cannot "silently clear unrelated native uncertainty" (design §7). It drives the existing `retire_native_starts` path but annotates each cleared invocation `RiskCleared`, never `ProvenEmpty` — the same distinction `apply_domain_retirement` already draws by setting `sealed_release = None` and `risk_clearance = Some(op)`.

**(b) Coordinator continuity-break acknowledgement** (for same-UUID rollback). Add an audited operation that records the observed rollback evidence (the specific `validate_machine_history` predicate that fired, the SQL `accepted_sequence`/`connection_epoch` versus the anchor values), retains the predecessor `CoordinatorHistory` in the immutable audit, and then permits `complete_machine_reset` by replacing its `history.store_uuid == store` rejection with "*either* a different store UUID *or* a continuity-break receipt naming this store UUID". **Every other guard in `complete_machine_reset` stays untouched** — `machine_permissions` empty, no pending commit, no pending retirement, all holds released, all participants committed, native coverage matched, epoch rotated. That is what makes it not a force-reset: it accepts only the *identity discontinuity*; it still cannot succeed while a single obligation is outstanding.

Why this is safe and a `force-reset` is not: a broad reset collapses three independent decisions — identity discontinuity, native cleanup uncertainty, and per-domain launch authority — into one undifferentiated `accept_risk`, so the immutable audit could not afterwards say *which* obligations were risked. The scoped composition preserves the property that every obligation is discharged by either a real platform proof or a named, digest-pinned, individually audited risk acceptance, and epoch rotation still fences every old allocation key. Nothing in `Authority` needs a new escape hatch; `force_release` (per-hold, audited, explicitly "not a runtime empty proof") is already the precedent for this shape.

### Fix order

1. F1 (reservation predicate) and F2 (recovery deadlock) — both are corrections to the current root's new code and both are small.
2. F3 — verify, then add the startup-reconciliation assertion; if startup does not reconcile, add the call.
3. F4 — retired-manager rejection code; needed for the exit criteria, not just polish.
4. F5, F7, F9 — small, low-risk.
5. F6, F8 — record as accepted limits with the threat model stated explicitly, or fix opportunistically.

### Negative controls I consider indispensable before acceptance

Beyond the two named inline in F1 and F2:

- **Reservation binds on the pairing side.** Saturate to just under `MAX − 256 KiB − 16 KiB·N`; assert `prepare_pairing` for participant N+1 is rejected with the budget error while all N existing participants remain retirable in sequence. The current saturated-registry test never reaches the rejection branch.
- **Digest changes for every registry-visible grant mutation.** The fixture covers *grant added* and *ticket added* (via `old_preview` rejection). Add `Armed → Uncertain` (`uncertainty_reason` set) and an authority-epoch rotation, asserting the stale digest is rejected in both.
- **Retirement clears no peer's uncertainty.** Two participants, each with an `Armed` grant and issued tickets; retire one; assert the other's grant is byte-identical in both SQL and the registry and its preview digest is unchanged. The `reset` branch currently only preserves an *empty* peer.
- **Corrupt/absent audit through the Store, not just the Authority.** `missing_or_changed_pending_retirement_audit_keeps_authority_closed` is Authority-only and calls `finish_domain_retirement` without ever running `apply_domain_retirement`. Add the same corruption through `reconcile_pending_domain_retirement` so the `StoreError` mapping and the gating behaviour of every entry point are covered — this is the same code path as F2.
- **Deleted audit for a *completed* retirement.** `validate_retirements` reads the audit file only for the pending entry, so deleting a completed retirement's audit does *not* close admission — the receipt is self-contained and carries `audit_sha256`. I believe that is the right decision (do not brick a machine over a lost audit file), but it is currently implicit; assert it, so a future change to `validate_retirements` cannot silently make it fatal.
- **SID formatter alignment.** Unit test: `process_user_sid_string(current process) == current_user_sid_string()`.

---

## Confidence

**High** on F1, F5, F7, F9 — these are fully determined by the supplied code and I traced the arithmetic and control flow directly.

**Medium-high** on F2 and F4 — the code paths are supplied and the traces are exact, but the severity depends on unsupplied callers (`machine_exchange` for F4) and on how likely the operator-rollback precondition is judged to be (F2).

**Medium** on F3 — the mechanism is clear, but whether it manifests depends entirely on the unsupplied startup path; I have marked it as a gap rather than a defect.

**Medium** on F6 and F8 — dependent on unsupplied implementations (`process_user_sid_string`, `peer.handle` provenance, scheduler queries).

**High** on the answer to the design's recovery question and on the "smallest defensible scope" verdict — those follow from the identity/authentication structure visible in `clearance.rs`, `retirement.rs` and `machine_reset.rs`, and do not depend on the gaps above.

Assumptions I made throughout: request processing is serialized by the Store mutex (per the brief), the authority mutex is not taken independently by any background thread, and `payload_hash` is a stable canonical serialization (its use as both the envelope checksum and the digest binding requires this). If the last assumption is wrong — e.g. non-deterministic map ordering anywhere in `GrantSnapshot` — the digest binding in F-summary would need re-examination; `BTreeMap`/`BTreeSet` usage throughout suggests it holds.