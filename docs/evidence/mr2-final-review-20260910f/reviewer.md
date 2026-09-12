# Summary

I re-derived the byte-budget invariant from the code rather than from the design note, and traced the stalled-retirement / same-UUID repair state machine through the fault stages the fixture injects.

**Both new fixes are confirmed.** The reservation arithmetic closes: every cleanup publish is monotone-shrinking in the quantity `check_publication` actually guards, so no cleanup can ever be budget-rejected, and the retirement/hold-release costs fit strictly inside their reservations. The stalled-retirement path is fail-closed and convergent: the fence and debit survive, the repair rebuild reconstructs the SQL projection from the same source the audit inventory was drawn from, so the retried `apply_domain_retirement` cannot fail for the reason that stalled it.

I found **no reproducible safety defect** in either delta. I found **one demonstrated liveness mechanism** (repair starvation of the same-UUID gate), one low-severity diagnostic regression that lands exactly in the stalled window, and one invariant-hygiene gap that only bites on upgrade. Two targeted evidence gaps have real decision impact.

---

# Findings

## 1. CONFIRM — metadata reservation is sound; cleanup can never be budget-rejected
**Severity: none (accept). Functions: `authority::check_publication`, `reserved_recovery_bytes`, `retirement::reserved_retirement_bytes`, `admission_bytes`.**

Define `future(R) = |encode_registry(R)| + reserved_recovery_bytes(R)`.

- Check 1 rejects only when `future_new > 4 MiB && future_new > future_old`, i.e. `future_new ≤ max(4 MiB, future_old)`. From an initialized registry (`future` small), induction gives **`future ≤ 4 MiB` at all times**.
- Check 2 fires only when `admission_bytes` grows, and then requires `encoded ≤ 3 MiB` and `future ≤ 3.75 MiB`. So every admission re-establishes `future ≤ 3.75 MiB`.

Cleanup costs against reservations:

| Operation | Δencoded | Δreserved |
|---|---|---|
| `prepare_domain_retirement` | `+RetiredDomain(receipt ≤ 12 KiB, checked) + PendingRetirement (~150 B) + key` ≤ ~12.3 KiB | −16 KiB (the domain enters `retired_domains`, so the `!retired_domains.contains_key` filter drops it in the same publish) |
| `force_release` / `seal_bootstrap` | `+release_reason (≤1024 B → ≤6144 escaped) + released_by + cleanup_proof (cgroup ≤4096 → ≤24 KiB escaped)` ≈ ≤31 KiB | −64 KiB |
| `finish_domain_retirement`, `finish_machine_commit`, `retire_native_starts`, `compact_machine_retirements` | monotone shrink | ≤ 0 |

So every cleanup publish strictly reduces `future`, meaning check 1 cannot reject it, and check 2 is skipped because `admission_bytes` never grows on those paths. The hard `encode_registry` cap is also safe: a live participant guarantees `reserved ≥ 16 KiB`, hence `encoded ≤ 4 MiB − 16 KiB` before a `+12.3 KiB` retirement; a live hold guarantees `reserved ≥ 64 KiB` before a `≤31 KiB` release.

Two non-obvious properties that make this hold, both of which I verified explicitly because they are easy to break later:

- **Release commits never trip check 2.** In `proposed_machine_commit`, `if grant.state == Released { continue; }` skips the `machine_permissions.insert` as well as the retired/duplicate checks, so a `ReconcileCommit` releasing a saturated inventory leaves `admission_bytes` unchanged and the intent body goes to the 32 MiB external blob. This is what makes `saturated_registry_still_journals_large_release_and_detects_missing_blob` pass and is load-bearing for liveness at the ceiling.
- **Unreserved post-admission growth is bounded well inside the 256 KiB slack.** Session identities (~300 B × ≤240 participants, since `16 KiB·P ≤ 3.75 MiB` binds first), connection epochs/sequences (~18 KiB), the `ResetGate` reason (≤6.2 KiB escaped), and `retired_allocations` (67 B/hash, but every hash is preceded by an admission-checked arm that re-establishes `future ≤ 3.75 MiB`). Total ≈ 96 KiB. In particular **`record_reset` can always publish**, which matters because `Store::validate_machine_history` and `reconcile_pending_domain_retirement` propagate its error with `?`.

The new test's assertions are consistent with this: `reserved_recovery_bytes == 0` after all holds released and all participants retired is correct because `finish_domain_retirement` removes the participant entirely.

## 2. CONFIRM — stalled retirement is fail-closed and the repair converges
**Severity: none (accept). Functions: `Store::reconcile_pending_domain_retirement`, `Store::retire_machine_domain`, `Store::apply_domain_retirement`, `Store::prepare_machine_reset_recovery`.**

Trace for the `divergent` fixture (fault at `after_retirement_journal`, extra SQL Armed row injected):

1. Startup → `attach_authority` → `reconcile_pending_domain_retirement` first (before `reconcile_pending_machine_commit` / `validate_machine_history` / native admission). `apply_domain_retirement` hits `"SQL contains start rights outside the accepted retirement inventory"` (`InvalidState`) → `record_reset` → `Ok(())`. Fence + debit retained; `admission_blocker` returns `authority_retirement_pending`; the daemon stays inspectable. `snapshot.pending_machine_operation` surfaces the operation id (matches the fixture's `gated.pending_machine_operation` assertion).
2. `machine_recover` → `prepare_machine_reset_recovery`. The marker check does **not** early-return because `pending_machine_operation.is_some()`, so the rebuild runs; it deletes only the pending domain's `machine_domain_retirements` row and preserves every unrelated receipt.
3. The rebuild sources grants from `snapshot.machine_obligations`, which is the identical `registry.machine_permissions` that `domain_clearance_preview` filtered when the audit was written. `proposed_machine_commit` and `check_retirement_pairing` both refuse while `pending_domain_retirement.is_some()`, and `validate_retirements` rejects a load with both a pending commit and a pending retirement — so that inventory provably cannot have drifted. The injected extra row is destroyed by the `DELETE FROM machine_grants` batch.
4. Trailing `reconcile_pending_domain_retirement()` therefore matches every row against `audit.preview.inventory.grants` and commits, then `finish_domain_retirement` drops the debit and the participant.

The replay path is also correct: after prepare, `retired_domain_receipt` returns `None` while the pending slot names that domain, so `retire_machine_domain` does not short-circuit before the SQL commit; after completion it returns the immutable original receipt. A repeated stalled call re-enters `prepare_domain_retirement`, matches the on-disk audit and the retained receipt, returns without re-publishing, fails apply again, and returns `retirement_stalled` — idempotent, no fence churn.

I also checked the direction that would be a genuine safety hole: the rebuild writing an Armed grant that a manager has already sealed Released. It cannot happen, because `finish_machine_commit` removes Released grants from `machine_permissions`, so the reset-independent registry — not the rolled-back SQL — is the rebuild's source of truth. `import_inventory`'s `old.state == GrantState::Released` guard would hard-fail such a case anyway.

## 3. FINDING — same-UUID repair gate can be re-armed indefinitely by post-gate submissions (liveness, not safety)
**Severity: medium. Decision impact: affects the claim that covered same-UUID repair converges; not a safety blocker. Functions: `Store::rollback_native_history_quiescent`, `prepare_machine_reset_recovery`, `finish_machine_reset_recovery`.**

`rollback_native_history_quiescent` requires `NOT EXISTS(SELECT 1 FROM jobs WHERE state!='final')` — i.e. *any* non-final Job row, regardless of whether it predates the reset gate or was ever admitted.

Evidence that new rows can appear while the gate is up, from the fixture itself: in the `reset` branch of `retirement_fixture`, `client.machine_recover(...)` asserts `blocker.is_some()`, and the very next statements submit `waiter` with `.unwrap()` and later assert `!after-retirement.txt.exists()`. So submission is **accepted** while the authority blocker is set; only admission/execution is gated. (The `reset` branch survives this only because the store UUID differs, which short-circuits the quiescence conjunct entirely.)

Concrete trace in the same-UUID path:

```
gate set → client submits J (accepted, state != 'final', never admissible)
machine_recover → prepare_machine_reset_recovery
  → history.store_uuid == self.store_uuid && !rollback_native_history_quiescent()
  → return Ok(snapshot)          // rebuild never runs
operator cancels J → another submit lands → repeat
```

The divergent fixture masks this by canceling `waiter` with no competing client. The mechanism is demonstrated by the code plus the fixture; the trigger (a client that keeps submitting, e.g. a CI wrapper) is environmental. Note this is intentional for *rolled-back* rows — the brief is explicit that unfinished native rows must be canceled through the normal API — but the predicate does not distinguish rolled-back rows from rows accepted after the gate, and only the former carry the resurrection risk the comment describes.

The machine side has the same shape (see Gap A below), which is why I treat this as a repair-starvation theme rather than a one-off.

## 4. FINDING — the `retired_domain` receipt diagnostic is suppressed exactly during a stalled retirement
**Severity: low (diagnostics). Functions: `Authority::retired_domain_receipt`, `retired_installation_receipt`, `Store::machine_connect_begin`.**

`retired_domain_receipt` returns `None` whenever `pending_domain_retirement.domain_id == domain`, and `retired_installation_receipt` funnels through it. So for a returning manager during a pending-and-stalled retirement:

```
machine_connect_begin
  → reconcile_pending_domain_retirement()   // stalls, Ok(())
  → retired_installation_receipt(...)       // None  (pending suppression)
  → participants()/machine_participant()     // anchor + SQL row still present
  → reserve_machine_connection()             // is_retired_domain() == true (no pending filter)
  → Err(io "retired domain cannot reconnect") → StoreError::Io, no code, no receipt
```

The operator loses the `retired_domain` code, the operation ID and the serialized receipt in precisely the window where a stalled retirement is being diagnosed. Secondarily, `retired_installation_receipt` uses `find(...)` with `installation_nonce == nonce || manager_store_uuid == store`; if the pending entry is encountered first in `BTreeMap` order, a *different*, fully-retired match later in the map is never consulted.

Suppression is correct for `snapshot().retired_domains` (an unfinished retirement must not be published as complete); it is wrong for the connect-time diagnostic, where a distinct code such as `retirement_pending` carrying the operation ID would be strictly better than an untyped `Io`.

## 5. FINDING — `future ≤ 4 MiB` is an induction invariant that is never verified at load
**Severity: low–medium, upgrade-only. Functions: `Authority::load`, `check_publication`, `encode_registry`.**

The reservation is enforced only at admission time. A registry produced by a build *without* the 64 KiB/hold term (the reservation caps live holds at ~59 today; `MAX_HOLDS` is still 1024) can be loaded with `future ≫ 4 MiB`. Check 1's `&& future > prior_future` escape then permits each `force_release` (it lowers `future`) even though each one raises `encoded` by up to ~6.4 KiB with a maximally escaped reason. After enough releases, `encode_registry` hits the hard `"authority registry exceeds durable byte budget; retire existing obligations"` error, and there is no implemented path to shrink — the remaining holds become unreleasable.

`load()` validates `valid_reason` for `hold.reason`/`release_reason` but nothing bounds `requester`/`released_by` `ProcessIdentity` strings, so the 64 KiB bound also rests on those being daemon-derived rather than caller-supplied. That is true on the paths I can see (`Store::hold_authority` / `force_release_authority` take the peer identity), but it is an unstated assumption, not an enforced one.

This is not introduced by the delta — it is the pre-existing hazard the delta fixes going forward — but the fix is not retroactive and is invisible until an operator is halfway through releasing.

## 6. Minor observations (non-blocking)
- `apply_domain_retirement` runs `save_grant` (which writes the just-Released grants) and *then* `UPDATE machine_candidates SET state='canceled' ... WHERE domain_id=?1`, overwriting whatever candidate state `save_grant` set for those rows. Harmless today because the domain leaves `participants`, but it makes the retired domain's candidate/grant states disagree with what `prepare_machine_reset_recovery` would rebuild (`'released'`).
- An audit file written by `prepare_domain_retirement` whose subsequent `publish` fails is never GC'd; `prune_machine_blobs` correctly leaves `domain-retirement-*.json` alone (asserted by the fixture). Recovery requires a fresh operation ID — reusing the old one yields `retirement_conflict` ("retirement operation payload conflict") rather than the more suggestive "inventory changed" message. Accepted GC/UX item per the brief.
- `storage_budget`'s `new_admission_headroom_bytes` mirrors check 2 exactly (including the strict-vs-nonstrict off-by-one), and its `bytes` proxy is length-faithful to `encode_registry` because the `"0".repeat(64)` checksum has the same width and `publish` keeps `pending_machine_blob` consistent in the in-memory `Ready` registry. No discrepancy.

---

# Evidence Gaps

**A. Is `proposed_machine_commit` gated by the reset/blocker state?** I inspected its actual predicates: retired domain, pending retirement, anchor committed + exact session, existing pending commit. There is **no `coordinator.pending_reset` check**, unlike `record_native_start`, `establish_native_coverage` and `retirement_candidates`, which all test `pending_reset.is_none()`. If the exchange path does not consult `authority_blocker()`, a paired manager can Arm new Grants while a reset gate is set; those grants then make `finish_machine_reset_recovery`'s `covered` query and `expected.machine_obligations.is_empty()` false, blocking epoch rotation for as long as the manager keeps arming. This is the machine-side twin of Finding 3. I am marking it a gap rather than a bug because `machine_exchange` was not supplied and the rights are durably charged either way — no double-allocation follows, only starvation. **Question:** does `machine_exchange` call `check_authority_release()` (or equivalent) before dispatching commands?

**B. Does `machine_queue::save_grant` re-insert `machine_ticket_identities` from `grant.tickets`?** The rebuild batch drops that table and re-inserts nothing explicitly; reconstruction can only come from `save_grant`. If it does not, a same-UUID rebuild silently drops the `invocation_id` PK / `containment_id` UNIQUE fence on issued Ticket identities for every surviving manager. `machine_queue.rs` was not supplied. This has direct decision impact on whether the repair is safe for a *surviving* peer.

**C. The same-UUID divergent repair is never exercised with a peer that holds rights.** `other_rights` is computed only when `stage.is_none() && !reset`, and `divergent` requires `stage == Some("after_retirement_journal")`, so at rebuild time the peer has no Grants and no Tickets — the fixture then retires it with `accept_risk: false`, which only succeeds because its inventory is empty. Neither the `reset` nor the `divergent` branch drives an authenticated exchange by a surviving manager *after* a rebuild. So "the rebuild preserves an unrelated manager's issued rights" is asserted only for the no-rebuild paths (`assert_eq!(preview, expected, "retirement changed another manager's issued rights")` is in the `else if !divergent` arm).

**D. `crate::machine::MAX_DOMAINS`** was not supplied; my participant-count bound (~240, byte-limited) is derived from the reservation arithmetic and is independent of it, but the interaction of the two limits is asserted rather than checked.

**E. `bootstrap::validate_proof`'s 4096-byte cgroup path bound** is brief-asserted, not visible. The 64 KiB hold reservation has ~2× margin over my worst-case estimate, so this is not tight, but it is the one input to the release bound I could not read.

---

# Recommendations

1. **Scope the same-UUID quiescence predicate to rows that can actually be resurrected** (Finding 3): require final/canceled only for Jobs that predate the reset gate, or that carry any lease/containment/root record — a never-admitted Job accepted after the gate cannot hold rolled-back native state. Alternatively reject `submit` with the active blocker while a same-UUID repair is pending. Either removes the operator-vs-client race without weakening the "no dropped native rows" property. Add one fixture variant that submits a second Job between the cancel and `machine_recover` and asserts the repair still converges.
2. **Return a typed pending-retirement diagnostic from `machine_connect_begin`** (Finding 4) rather than falling through to `reserve_machine_connection`'s untyped `Io`; also make `retired_installation_receipt` scan past a suppressed pending entry instead of short-circuiting on it.
3. **Verify the reservation invariant at load** (Finding 5): compute `future(R)` in `Authority::load` (or surface it in `AuthoritySnapshot.detail`) and report explicitly when a registry was admitted under weaker reservation rules, so the condition is visible before the first unreleasable hold rather than after.
4. **Answer Gap A explicitly.** If `machine_exchange` does not gate on `admission_blocker()`, add a `pending_reset.is_none()` predicate to `proposed_machine_commit` for symmetry with `record_native_start`.
5. **Answer Gap B, then close Gap C** with a `retirement_fixture(Some("after_retirement_journal"), false, true)` variant that enables the `other_rights` branch and asserts the peer's `machine_clearance_preview` is byte-identical across the rebuild — this is the single highest-value test delta for the reviewed change.
6. Non-blocking: reorder the candidate cancel before `save_grant` in `apply_domain_retirement`; consider GC of orphan `domain-retirement-*.json` whose operation is neither pending nor in `retired_domains`.

---

# Confidence

**High** on Findings 1 and 2: both are derived directly from the supplied predicates, and the byte arithmetic is closed-form with the escape-ratio worst cases (6× for control-char JSON escaping) taken as given. The one input I could not read (Gap E) has ~2× margin.

**Medium-high** on Finding 3: the code predicate is certain; the inference that `submit` is accepted while blocked rests on the `reset` branch's successful `.unwrap()` after `machine_recover` reports a blocker, which I read as dispositive but did not confirm against `submit`'s own body.

**Medium** on Finding 5: the mechanism is certain; whether an installed alpha.16 store realistically carries enough live holds with maximally escaped release reasons to reach the hard cap is not something I can establish from the supplied context.

**Low-confidence, deliberately filed as gaps not bugs:** A and B. Both would change my assessment materially — B in the direction of a safety finding for surviving peers, A in the direction of upgrading Finding 3 to a systemic starvation defect.

**Verdict (scoped):** For the two deltas under review — metadata reservations and stalled-retirement / same-UUID repair — I **confirm both fixes** and find **no reproducible safety defect and no demonstrated safety blocker**. One demonstrated liveness mechanism (Finding 3) qualifies the claim that covered same-UUID repair converges under continued client load; it does not weaken any fence, debit, or containment guarantee. This verdict covers the supplied `e` sources only; it does not extend to Gaps A–C, to WSL/Linux runtime containment, to live-consumer acceptance, or to the pending `f` and phase-level MSRV/release/installation gates.