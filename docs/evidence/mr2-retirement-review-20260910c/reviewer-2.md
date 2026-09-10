# MR-2 Domain Retirement — Focused Review (Opus reviewer 2/2)

Lens: durable inventory completeness, bounded history, recovery liveness, remaining MR-2 exit criteria.

No tool access; all findings are traced from the supplied excerpts. I have no line numbers, so references are `file` + symbol. Nothing below was executed; "traced" means I followed the control flow in the provided code, "requires evidence" means the deciding code was not supplied.

## Summary

The clearance mechanism itself is sound and, in the specific respects the design doc claims, I could not break it. The fence-before-debit ordering, the digest binding, the receipt/proof separation, and the load-time validator are all correct as written, and I list them explicitly under Finding 11 so the next reviewer does not re-derive them.

The receipt-space reservation added in this root is **arithmetically sufficient**, which surprised me — see Finding 4 for the closure argument — but it is untested at the multi-participant case it exists for, and it silently makes the registry byte budget the binding capacity limit roughly 17× below the 4096 figure the design doc advertises.

The one thing I'd call a possible MR-2 exit blocker is recovery *liveness*, not safety: I cannot find any caller that completes a fenced retirement without the owner re-issuing the same request, and the crash-boundary fixture cannot distinguish that case because it always replays. Everything else is operability: the states that are genuinely unrecoverable are recoverable only by restoring a file, and the daemon does not tell the operator which file.

On the headline question: **yes, and a broad force-reset is the wrong primitive** — precise argument at the end of Recommendations.

## Findings

### 1. Pending retirement may require an operator replay to converge — [High, uncertain]

`Store::reconcile_pending_domain_retirement` (`src/store/machine_clearance.rs`) is called from exactly five places in the supplied code: `retire_machine_domain`, `machine_clearance_preview`, `pair_machine_domain`, `machine_connect_begin`, `machine_connect_finish` (`src/store/machine.rs`) and `prepare_machine_reset_recovery` (`src/store/machine_reset.rs`). All six are machine-protocol or clearance RPCs. I see no call at store open, in `validate_machine_history`, or on the native admission path.

Meanwhile `Authority::admission_blocker` returns `authority_retirement_pending` whenever `pending_domain_retirement.is_some()`, ahead of every other blocker.

Failure trace: crash at `after_retirement_journal`. Registry has `retired_domains[D]` + `pending_domain_retirement{op}`; SQL has nothing. Daemon restarts. The owner's client also died and never retries. Nothing calls any of the six entry points. `authority_retirement_pending` blocks admission indefinitely; the machine has no live participant left to trigger `connect_begin`; native Job submission is refused. The design's item 5 — *"a reset reloads the pending fenced decision and completes it before admission. A lost response never requires issuing another clearance"* — is not satisfied by any code I was given.

The fixture cannot catch this. In `retirement_fixture` (`tests/support/machine_clearance.rs`), every crash stage restarts the daemon and then falls through to `client.retire_machine_domain(request.clone(), ...)`, which reconciles at its own head. The test proves replay is idempotent, not that recovery is automatic.

Minimum correction: call `reconcile_pending_domain_retirement()` unconditionally where `validate_machine_history()` is already called at store open, and treat its failure the way a failed pending-commit reconcile is treated.

Useful test: crash at `after_retirement_journal`, restart, then **submit a native Job without re-issuing `retire_machine_domain`**; assert it succeeds and `authority_status().blocker.is_none()`.

Uncertainty: if such a call exists in `src/store/mod.rs` or `src/daemon/mod.rs` (neither supplied), this collapses to "add the negative control anyway", since the current fixture cannot prove it.

### 2. A post-fence `apply_domain_retirement` failure is undiagnosable and misdirects the operator — [Medium]

In `retire_machine_domain`, the `InvalidInput → rejected("retirement_conflict")` mapping is applied only to `prepare_domain_retirement`. `apply_domain_retirement`'s two `StoreError::InvalidState` returns — `"SQL contains start rights outside the accepted retirement inventory"` and `"domain retirement SQL audit differs"` — and `finish_domain_retirement`'s io errors propagate raw to the generic `STORE_ERROR` arm in `src/daemon/rpc.rs`.

This is the one state where the distinction matters most: the fence is already durable, so the correct operator action is *replay the same operation UUID*, and minting a fresh UUID is refused ("another domain retirement requires recovery") with an equally generic code. I am **not** claiming a reproduced bug — I could not construct a reachable trace to these branches that does not first pass through same-UUID SQL rollback or corruption, which is explicitly your separately-open item. The defect I am claiming is the error surface, which is reachable in exactly that separately-open scenario.

Minimum correction: distinct code (`retirement_stalled`) carrying `pending.operation_id`, and surface `pending_domain_retirement` in the doctor output.

### 3. The blocker detail for a missing authority file does not name the file — [Medium]

`read_with_limit` (`src/authority.rs`) does `File::open(path)?` and propagates. Rust's `std::io::Error` from `File::open` does **not** carry the path. So when `domain-retirement-{op}.json` or a `machine-commit-*.json` blob is missing, `Authority::load` fails, `State::Unknown` is set, and the operator sees:

```
blocker: authority_history_unknown
detail:  The system cannot find the file specified. (os error 2)
```

Restoring that file is the *only* recovery action available for this state — `initialize()` refuses over uncertain history, by design — and the daemon does not say which file. `missing_or_changed_pending_retirement_audit_keeps_authority_closed` asserts only `blocker.is_some()`, so it locks in the silence.

Minimum correction: wrap in `read_with_limit`: `.map_err(|e| std::io::Error::other(format!("{}: {e}", path.display())))`.

Useful test: extend the existing test to assert `snapshot().detail` contains `"domain-retirement-"`.

### 4. The reservation is sufficient, but the operative capacity is invisible and far below the documented figure — [Medium]

First the positive, since it needs to be on the record. `check_publication` enforces, on any publication that increases `admission_bytes`:

```
encoded ≤ 3 MiB   AND   encoded + 16 KiB × live_participants ≤ 3.75 MiB
```

Closure argument for "already admitted obligations retain retirement space": retirements serialize (a second `prepare` is refused while pending), and each `finish_domain_retirement` removes the participant's registration. Peak registry size while retiring all N is bounded by `(3.75 MiB − 16 KiB·N) + 16 KiB·N = 3.75 MiB < 4 MiB`. The unreserved growth between admission-increasing publications is dominated by `accept_machine_session` writing a `SessionIdentity` (~250 B, not counted in `admission_bytes`); at the self-enforced ceiling of N ≈ 240 that is ~60 KiB against 256 KiB of true slack. **The arithmetic closes**, with the receipt cap (12 KiB checked in `prepare_domain_retirement`) comfortably inside the 16 KiB reserve.

The problem is what that implies and what it hides:

- `3.75 MiB / 16 KiB ≈ 240` live participants is a hard ceiling on **every** admission-increasing publication — `prepare_pairing`, `prepare_machine_commit`, `record_native_start`, `hold`, `arm_bootstrap`. Past it, ordinary native Windows Job admission fails with `"authority admission budget reached; reserved space is for cleanup and session recovery"`.
- `retired_domains` at the documented 4096 × a typical ~700 B receipt is ~2.8 MiB, but with a worst-case 12 KiB receipt the hard `MAX_REGISTRY_BYTES` binds at ~300. Either way, **the byte budget, not the 4096 count, is the operative bound.** Design §8 mentions both; the 4096 number is not reachable under the reservation.
- `record_native_start`'s `native_permissions.len() >= 65_536` check is dead — 65 536 permissions exceed 4 MiB by an order of magnitude.

This is fail-closed and I am not asking you to change the mechanism. But a bound the operator cannot observe until it refuses a build is not operationally bounded, and that is squarely an MR-2 exit criterion.

Minimum correction: expose `registry_bytes`, `reserved_retirement_bytes`, `admission_headroom_bytes` on `AuthoritySnapshot` and add a doctor check that warns below some headroom; correct design §8 to name the byte budget as binding.

Uncertainty: `crate::machine::MAX_DOMAINS` was not supplied. If `MAX_DOMAINS − 2 ≤ ~100`, the ceiling is never reached and this drops to Low.

### 5. The reservation change has no executable control at N participants — [Medium-Low]

`saturated_registry_still_journals_large_release_and_detects_missing_blob` (`src/authority.rs`) now saturates a registry with grants, then retires **one** domain with a deliberately worst-case reason (`"\u{1}".repeat(1024)`, which escapes to 6 KiB). That is a good control for "a saturated registry admits a worst-case receipt", and I'd keep it.

It does not test the property the reservation exists for: that **N** admitted participants can each be retired after the registry was driven to the admission limit. It also reaches the saturated state via `retirement_authority.publish(registry.clone())` — a direct write that bypasses `prepare_pairing` entirely, so the N-participant path never runs through the real API.

Useful test: pair participants through `prepare_pairing` until it is refused, saturate `machine_permissions` until `prepare_machine_commit` is refused, then retire every participant in sequence; assert each `prepare_domain_retirement` publishes and the final `encode_registry` is under `MAX_REGISTRY_BYTES`. This is the single highest-value missing control in the new root.

### 6. `validate_retirements` does not reject a live participant parented to a retired domain — [Low-Medium]

`prepare_domain_retirement` blocks retiring a parent while a live child names it (`participants.values().any(|p| p.registration.parent_domain == request.domain_id)`), so this is unreachable in normal operation. But `validate_retirements` is precisely the guard for registries that arrived by rollback or corruption, and its loop is otherwise thorough (it already catches a retired domain still holding a participant or a grant). Consequence if it occurs: load succeeds, then every subsequent `pair_machine_domain` fails inside `ResourceTopology::new` with an `InvalidSpec` that names neither the retirement nor the dangling parent.

Minimum correction: one clause added to the existing `!pending && (...)` condition — `|| registry.participants.values().any(|p| p.registration.parent_domain == *domain)`.

### 7. `apply_domain_retirement` unconditionally nulls `sealed_release` — [Low, uncertain]

`grant.sealed_release = None;` runs for Uncertain grants as well as Armed. If an Uncertain grant can carry a genuine partial seal, retirement discards proof the operator already has. Safety is unaffected — the grant is Released either way and `risk_clearance` marks it — but audit fidelity is: the receipt cannot distinguish "we had platform proof for these tickets and accepted risk on the rest" from "we accepted risk on all of them".

I could not determine from the supplied code whether an Uncertain grant ever carries a seal; `machine_queue::save_grant` and the coordinator's Armed→Uncertain transition were not provided. If it can, preserve the seal and rely on `risk_clearance.is_some()` as the unproven marker. If it cannot, make it an assertion rather than an assumption.

### 8. SID equality is checked only on retirement — [Low; depends on missing evidence]

In `src/daemon/rpc.rs`, `authority_admin` gates `MachineRetireDomain`, `MachineClearancePreview`, `MachineRecover`, `MachinePair`, `AuthorityInitialize`, `AuthorityHold`, `AuthorityForceRelease` on "unmanaged peer with a process identity". The new `process_user_sid_string(peer.handle) == current_user_sid_string()` check is applied to `MachineRetireDomain` only.

Reading the SID from the daemon-held peer handle rather than the JSON is right, and PID reuse cannot alias an open handle. Under the cooperative-owner model the asymmetry is acceptable **if** the named pipe's ACL already restricts connections to the owner. If it does not, `AuthorityForceRelease` — a comparable risk-acceptance operation — is a strictly weaker door than the one that just got hardened. `src/daemon/transport.rs` was not supplied, so I cannot resolve this.

Also cosmetic: a SID mismatch returns `StoreError::InvalidState` → `STORE_ERROR`, not `REJECTED`.

### 9. `pending_domain_retirement()` is silent on a non-Ready authority — [Low]

```rust
let State::Ready(registry) = &self.state else { return Ok(None); };
```

Its sibling `pending_machine_commit()` errors in the same situation. No current caller is misled — `reconcile_pending_domain_retirement` returning `Ok(())` is followed by calls that all fail closed on the Unknown authority — but "no pending retirement" is a dangerous thing to report when the truth is "unknown". Same shape in `is_retired_domain`, which returns `false` on non-Ready; it is currently only used as an early gate before a `State::Ready` check, so it is safe today, but it is `pub(crate)`.

### 10. Retired domains leave permanent SQL rows — [Low]

`apply_domain_retirement` deletes challenges, reconciles, and reservations for the domain, cancels candidates, and clears `reconciliation_required` — but retains the `machine_domains` row (required: `machine_candidates.domain_id` is an FK), the Released grant rows, `machine_ticket_identities`, and the `machine_queue` owners. This is correct for anti-revival and FK integrity, and `prepare_machine_reset_recovery`'s wipe rebuilds without them. It does mean the SQL side has no bound corresponding to the registry's; worth one line in the design doc rather than code.

Note also that `machine_domain_retirements` is created lazily inside `apply_domain_retirement` and is *not* in `prepare_machine_reset_recovery`'s DELETE batch — correct — but it is lost when the SQLite file itself is replaced, as in the `reset` fixture variant. The authoritative retirement record is the registry receipt plus the audit file; the SQL row and `machine_events` are reset-volatile. That is defensible, but say so, because design §4 promises "public participant/Grant/event surfaces expose retirement".

### 11. Properties I checked and believe are correct — [record, do not re-litigate]

- **Digest completeness.** `DomainClearanceInventory` covers `authority_epoch`, `installation`, `manager_store_uuid`, `session`, `committed`, and every `GrantSnapshot` including `tickets`, over a `BTreeMap`-ordered iteration. A reconnect (`session`), a pairing completion (`committed`, which is what makes the no-risk abandonment path safe), an epoch rotation, a new Grant, and a new Ticket all invalidate a stale clearance. `prepare_domain_retirement` recomputes the preview under the same lock and compares both the structure and `expected_inventory_sha256`.
- **Fence before debit.** The `retired_domains` tombstone plus `pending_domain_retirement` publish before any SQL mutation; `is_retired_domain` is true during pending, so `reserve_machine_connection`, `accept_machine_session`, and `proposed_machine_commit` are all fenced before the SQL commit. The receipt is *withheld* from `snapshot().retired_domains` and `retired_domain_receipt()` while pending — fence early, acknowledge late, which is the right asymmetry. The fixture's direct read of `registry.json` asserting `charged == (stage != "after_retirement_ack")` is a good control for this.
- **No fabricated proof.** `sealed_release = None` + `risk_clearance = Some(operation_id)`, asserted in both the SQL and the event surface.
- **No revival.** `check_retirement_pairing` permanently bars the domain_id, the installation_nonce, and the manager_store_uuid, and additionally bars two live participants sharing a manager store. The fixture's three-way `for reused in 0..3` loop is the right negative control.
- **`retired_allocations` is correctly *not* populated by retirement.** `AllocationKey` embeds `domain_id` and `manager_store_uuid`, both permanently barred, so the key cannot recur; and `complete_machine_reset` clearing `retired_allocations` while retaining `retired_domains` across an epoch rotation is exactly right per design §8.
- **Scope containment.** Every statement in `apply_domain_retirement` is `WHERE domain_id=?1`; only a paired participant can be previewed (`participants.get(&domain)`), so `native_domain` and `machine_scope` are unreachable. The `reset` fixture variant asserting that the *second* participant still blocks `machine_recover` until separately retired is the indispensable negative control for design §7, and it is present.
- **Load-time closure.** `validate_retirements` closes the authority on a missing or altered audit file, on a completed retirement that still owns a participant or grant, and on a pending commit that names a retired domain. Tested.
- **No secret leakage.** `PairingRegistration.secret` is absent from `DomainClearanceInventory`; only `InstallationIdentity` is exposed.

## Evidence Gaps

- **`src/store/mod.rs`, `src/daemon/mod.rs`** — whether anything calls `reconcile_pending_domain_retirement` at open or on the admission path. This single fact decides Finding 1.
- **`src/daemon/transport.rs`** — the named-pipe ACL and how `PeerProcess.handle` is acquired. Decides Finding 8.
- **`crate::machine::MAX_DOMAINS`** and the full `machine::Command` enum. `MAX_DOMAINS` decides the severity of Finding 4; the `Command` enum would let me confirm exhaustively that retirement is unreachable over the HMAC path (the `Request::MachineRetireDomain` → `authority_admin` route is confirmed to reject managed peers).
- **`machine_queue::save_grant` and the Armed→Uncertain transition** — decides Finding 7.
- **`recover_machine_authority`** in `src/daemon/rpc.rs` — referenced but not supplied; it is the RPC the reset fixture drives.
- Per the brief, I made no inferences about omitted functions; the above are marked rather than assumed.

## Recommendations

**Blocking for MR-2 acceptance:**

1. Resolve Finding 1 — either point me at the existing unconditional reconcile call, or add it and the negative control that submits a native Job after a crash at `after_retirement_journal` without replaying the clearance.
2. Add the N-participant reservation control (Finding 5). The reservation is the change that gates *all* admission for the whole daemon and it currently has a single-participant test.

**Should-fix before accepting (cheap, all operability on the recovery path):**

3. Name the file in `read_with_limit` errors (Finding 3) and assert it in the existing audit test.
4. Distinct `retirement_stalled` code carrying the pending operation ID (Finding 2).
5. One clause in `validate_retirements` for dangling parents (Finding 6).
6. Add the mid-handshake negative control: `connect-begin`, then retire, then attempt `connect-finish` with the pre-retirement challenge. I traced this as double-guarded (`apply_domain_retirement` deletes `machine_challenges` for the domain, and `accept_machine_session` rejects retired domains), so I expect it to pass — but it is the one state transition the digest cannot see, since `reserve_machine_connection` bumps `next_connection_epoch` without touching anything in the inventory. Assert it rather than reason about it.

**Not blocking:** MR-3 Linux runtime, identity-fence compaction, same-UUID rollback, native pre-coverage. I confirmed that retirement does not worsen the same-UUID case: a rollback after a completed retirement drives `validate_machine_history` to `record_reset`, and `prepare_machine_reset_recovery` then bails on `history.store_uuid == self.store_uuid` — the same terminal state as before this change.

---

### The precise answer on force-reset

**Yes. MR-2 can ship safe explicit operational recovery without a broad force-reset, and a broad force-reset would be the wrong primitive even if you wanted one.**

Every state this feature *introduces* falls into one of three classes, all of which already have a safe resolution:

- **Fenced-but-incomplete** (crash boundaries 1–4): resolved by replaying the same operation UUID under the Store mutex, which the fixture verifies at all four boundaries. The only gap is that it may currently need an operator to trigger the replay (Finding 1) — the fix is a startup hook, not a new operation.
- **Refused before the fence** (digest mismatch, missing `accept_risk`, receipt over 12 KiB, registry over budget, live descendant, pending commit): nothing durable was written; re-preview and re-issue.
- **File-loss** (missing or altered `domain-retirement-{op}.json`): resolved by restoring the file, which the code never deletes. The registry independently retains the full receipt, so this loses inventory detail, not the fence.

The genuinely unrecoverable states are all pre-existing and none is fixable by a broad reset without destroying the property MR-2 exists to provide. A force-reset over them would have to either fabricate `ProvenEmpty` for grants whose OS processes are unknown, or delete registry/anchors — forbidden by design §7. And its safety is untestable in principle: you cannot write a negative control asserting "it did not clear something it shouldn't have" when the operation's scope is "everything".

The correct shape for the three remaining gaps is **more operations of exactly the form `retire-domain` already takes** — narrow, inventory-scoped, digest-bound, `accept_risk`-gated, immutably audited, refusing to run when the named inventory changed, and fenced before any debit:

1. **`machine retire-native --spec`** — per-`InvocationId` risk clearance with a preview digest over `native_permissions`, for the "history predates external coverage" case that currently drives `record_reset` in `establish_native_coverage`. Same ordering, no `ProvenEmpty` claim.
2. **`machine adopt-store --spec`** — for same-UUID rollback: an owner-audited assertion binding the current SQL store's content hash to the pending reset gate, which permits `prepare_machine_reset_recovery` to run its existing journal-driven rebuild against the same UUID. Note this is *not* a reset — the rebuild already refuses to invent obligations and reconstructs solely from the journal; it currently declines only because it cannot distinguish rollback from continuity, and the owner can supply exactly that one bit.
3. **`machine compact-identities --spec`** — converts design §8 from open to bounded: replace a completed retirement's ~700 B–12 KiB receipt with a compact permanent forbidden-identity entry (truncated hashes of domain_id, installation_nonce, manager_store_uuid, ~40 B), keeping the full audit file on disk as the record. `check_retirement_pairing` consults the compact set instead, preserving anti-revival at roughly 1/20 the registry cost. This is the operation that makes the 4096 figure real rather than aspirational — see Finding 4.

None of the three is required to accept MR-2; all three are the right follow-on shape, and each is testable by the same four-boundary fault-injection pattern this root already established.

## Confidence

- **High** on Findings 3, 5, 6, 9, 10 and on the Finding 11 positives: these are read directly off the supplied code.
- **High** on the reservation's arithmetic closure in Finding 4; **medium** on its severity, which turns on the unsupplied `MAX_DOMAINS`.
- **Medium** on Finding 1 — the control-flow analysis is solid over what I was given, but a single call in `src/store/mod.rs` would resolve it. I flag it as the top item precisely because the existing fixture structurally cannot distinguish the two outcomes.
- **Medium** on Finding 2 (I could not construct a reachable trace outside your separately-open rollback item, so I claim the error surface, not a bug) and Finding 8 (turns on the pipe ACL).
- **Low-medium** on Finding 7 — genuinely uncertain whether the discarded seal is ever non-`None`.
- I make no claim about WSL process cleanup; nothing here proves it, and nothing here asserts it does.

Two questions that would change my view: does anything call `reconcile_pending_domain_retirement` at store open, and what is `MAX_DOMAINS`?