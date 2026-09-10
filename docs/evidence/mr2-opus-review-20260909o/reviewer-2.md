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
