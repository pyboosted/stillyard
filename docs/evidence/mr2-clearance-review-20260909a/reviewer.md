# MR‑2 audited domain retirement — conceptual sanity check

*Read‑only review. No files were modified; no tools were run. All evidence is from the four supplied source excerpts and the design document.*

## Summary

Whole‑domain retirement is the right **identity** scope, and the strongest argument for it is in the code you already have: `manager/recovery.rs::import_inventory` treats absence from a completed authenticated inventory as acknowledgement only for *already sealed* releases, and otherwise fails with `"coordinator omitted an unsealed local allocation"`. Any *partial* coordinator‑side clearance therefore permanently bricks a manager that later returns. Since retirement forbids the domain from ever re‑pairing, that failure mode is unreachable by construction. That argument should be stated in the design as the load‑bearing justification, because it is what makes the scope non‑arbitrary.

However, the design conflates two separable clearances under one `--accept-risk`, and it does not survive contact with three concrete code paths: `Authority::proposed_machine_commit` (no session/domain fence), `Store::prepare_machine_reset_recovery` (rebuilds **every** participant with `reconciliation_required=1`), and `Authority::finish_machine_commit` (hard‑errors when the participant anchor is missing). As written, retirement can (a) be raced by a writer it believes it fenced, (b) deadlock against the very reset gate it exists to unblock, and (c) wedge the single‑slot `pending_machine_commit`.

On the brief's direct question: **a broad force‑reset is not essential.** The residual cases that whole‑domain retirement cannot cover are exactly the two that `prepare_machine_reset_recovery` already early‑returns on — `history.store_uuid == self.store_uuid` (same‑UUID rollback) and `native_coverage_store != Some(history.store_uuid)` (history predating native coverage). Those are enumerable and should be named as two separate bounded remediations, not folded into a generic clearance.

---

## Findings

### F1 — The retirement fence is not enforced at the durable layer; a fenced writer can still commit. **Severity: High**

`proposed_machine_commit` inserts grants into `machine_permissions` with no check that `intent.request.session` is the anchor's currently accepted session, that the anchor is `committed`, or that the domain is live:

```rust
fn proposed_machine_commit(&self, intent: MachineCommitIntent) -> std::io::Result<Registry> {
    ...
    for grant in &intent.grants {
        if grant.state == Released { continue; }
        // retired_allocations check, duplicate-key check ...
        next.machine_permissions.insert(grant.grant_id, grant.clone());
    }
```

The only session fencing shown is at `accept_machine_session` (connection‑epoch monotonicity), which happens once per connection. Design item 3 revokes the session durably, but nothing in the authority layer re‑reads that at commit time, so the fence depends entirely on transport teardown — code not supplied.

Second, independent resurrection path: `machine_recovery.rs::reconcile_pending_machine_commit` gates only on `history.store_uuid != self.store_uuid || history.pending_reset.is_some()`. After a crash it will `save_grant` and insert `machine_operations` rows for a domain that was retired in between.

**Minimal fix:** make retirement a *durable predicate*, not just a state change: reject in `proposed_machine_commit` (and in `reconcile_pending_machine_commit`) any intent whose `request.session.domain_id` is retired, or whose `request.session != anchor.session`. This makes the fence hold regardless of transport races and is directly testable.

### F2 — Reset‑gate deadlock: retirement and coordinator reset each require the other. **Severity: High**

Two mechanisms, one deadlock.

*(a)* `prepare_machine_reset_recovery` rebuilds SQL from `journal.participants()` with a hardcoded literal:

```rust
tx.execute("INSERT INTO machine_domains(... ,reconciliation_required,retired_sequence_floor,accepted_sequence)
            VALUES (?1,?2,?3,?4,?5,?6,1,?7,?7)", ...)
```

and `finish_machine_reset_recovery` requires

```sql
NOT EXISTS(SELECT 1 FROM machine_domains WHERE reconciliation_required!=0)
```

A retired domain re‑inserted here can never reconcile (its store is gone), so `covered` is never true and `complete_machine_reset` is never reached. Design item 7's "A coordinator SQL reset can then converge" is **false against this code** unless retired anchors are filtered or inserted with `reconciliation_required=0`.

*(b)* The design is silent on whether `retire-domain` is admissible while `pending_reset` is set. It must be: the motivating incident (manager store irretrievably lost) plausibly co‑occurs with a coordinator SQL reset, and item 7's wording implies retirement always precedes reset. Note `retirement_candidates` already refuses to act when `pending_reset.is_some()`, so the "not admissible during reset" reading has precedent in the file.

**Minimal fix:** state explicitly that retirement is admissible under `authority_reconciliation_required`, and that `prepare_machine_reset_recovery` must skip retired anchors (or insert them settled). Add the convergence test as a negative control (N7 below).

### F3 — Single‑slot `pending_machine_commit` and the anchor‑required `finish_machine_commit`. **Severity: High**

```rust
let anchor = next.participants
    .get_mut(&intent.request.session.domain_id)
    .ok_or_else(|| std::io::Error::other("machine commit participant anchor missing"))?;
```

If retirement removes the participant anchor while any commit for that domain is pending or replayable, `finish_machine_commit` errors, `pending_machine_commit` never clears, and `admission_blocker` returns `authority_commit_pending` **machine‑wide, permanently** — the first branch checked, ahead of everything else. There is also exactly one slot: `proposed_machine_commit` rejects a non‑byte‑identical second intent with `"another machine commit requires reconciliation"`, so a retirement fence stored in that slot collides with ordinary commits in both directions.

**Minimal fix:** (i) retirement keeps a *retired anchor tombstone*, never deletes it; (ii) a separate `pending_domain_retirement` field, not the machine‑commit slot; (iii) an explicit rule for the interaction — retirement of domain *D* subsumes and discards a pending commit for *D* (recording its grants in the retirement inventory), and is rejected with a distinct code while a commit for another domain is pending.

### F4 — Two clearances are conflated; the risk‑accepted part can be much smaller. **Severity: High (scope decision)**

Retirement bundles: **(a)** fencing the writer identity — mechanical, loses only the dead manager's own state, needs no unproven claim; and **(b)** freeing the physical debit charged to Armed/Uncertain grants whose Invocation intents were issued — this is the *only* part that asserts something unproven about OS processes, and WSL cleanup is not implemented.

Right now (b) is forced, because `complete_machine_reset` requires `registry.machine_permissions.is_empty()`. So the design silently converts unproven residual containment into free capacity, on a system whose entire `AuthorityHold` mechanism exists to prevent exactly that (`force_release` is documented as "deliberately not a runtime empty proof").

**Minimal fix:** split them. Retirement (a) always fences the identity and moves the domain's grants out of `machine_permissions` into a `retired_residue` record that is *still charged* against physical capacity. A second, separately audited acceptance releases the residue. `complete_machine_reset`'s guard must then enumerate `retired_residue` explicitly rather than relying on `machine_permissions.is_empty()`. This makes the risk statement enumerate exactly the invocation intents whose processes are unproven, and restores reset liveness without the capacity claim. If the owner declines (b), the machine is still correct — just smaller.

### F5 — "Reuse requires a fresh domain/installation/store identity" is unenforced. **Severity: High**

`prepare_pairing` checks only two things, and only against **live** participants:

```rust
if let Some(old) = registry.participants.get(&id) { ... }
if registry.participants.values().any(|p|
    p.registration.installation.installation_nonce == registration.installation.installation_nonce) { ... }
```

There is **no `manager_store_uuid` uniqueness check anywhere in the supplied code** (`load` only rejects `is_nil()`). Store UUIDs are load‑bearing identity: `record_native_start` derives `invocation_id.store_uuid()`, `containment_id.store_uuid()`, `grant_id.store_uuid()` from it, and `machine_queue` owners are `remote:{job_id}` where the job id embeds it. A re‑pairing that reuses a retired store UUID collides with retired‑but‑audited history. Design item 3 asserts the fence; nothing implements it.

**Minimal fix:** a `retired_identities` set (domain_id, installation_nonce, manager_store_uuid) consulted by `prepare_pairing`, plus a live `manager_store_uuid` uniqueness check. Store it *outside* `participants` so it does not consume `MAX_DOMAINS - 2` (see F8).

### F6 — Preview digest does not cover the surface that can change. **Severity: Medium‑High**

Item 1 defines the preview as "every Armed/Uncertain Grant including its complete issued Invocation intent inventory"; item 2 says "A changed preview rejects before mutation". But an `Offered` candidate can be Armed by the still‑connected executor between preview and command, producing a *new* Armed grant with issued invocation intents. If the digest covers only the previously‑Armed set, that grant slips through unchanged‑digest validation and is retired without ever appearing in the risk statement the owner signed.

**Minimal fix:** either extend the digest to cover Offered candidates and reservations, or — better, and it also closes F1 — make the order **fence → digest → accept**: publish the session revocation first (no new commits accepted from *D*), then compute the digest against the now‑frozen state, then require the digest match inside the same publication that frees anything.

### F7 — Replay identity is undefined after completion. **Severity: Medium‑High**

Item 5 says "Replays use the exact operation ID/payload" and "A lost response never requires issuing another clearance", but names no durable record that answers a replay *after* the pending fence has been cleared. Machine commits are answered by `machine_operations(domain_id, sequence)`; retirement deliberately has no executor sequence (item 2). Without a completed‑operation tombstone, a replayed `retire-domain` either fails with `"participant anchor missing"` or, worse, is misread as a fresh retirement.

The pattern already exists twice in `authority.rs` and should simply be mirrored: `hold` ("A retired operation is a tombstone, never permission for another run") and `force_release` (`"authority release payload conflict"` when the replayed reason differs).

**Minimal fix:** the retired‑registration tombstone records `operation_id`, requester, digest and reason; identical `operation_id` + payload returns the recorded outcome; identical `operation_id` with a different payload is a payload conflict; a *different* `operation_id` against an already‑retired domain is rejected as already‑retired, not silently succeeded.

### F8 — Durable representation of retired records fights the existing budgets. **Severity: Medium**

If retired records live in `participants`:

- `admission_bytes` counts `p.registration` for **all** participants unconditionally, so retirement never reduces that component and accumulated retirements permanently raise the number that `check_publication` gates on.
- `prepare_pairing` (`participants.len() >= MAX_DOMAINS - 2`) and `load` (same bound) count them, so retirements consume the domain budget with epoch rotation as the only exit.
- `PairingRegistration` carries `secret: [u8; 32]`. Retaining a live pairing secret for a permanently fenced installation is free to avoid and pointless to keep, even under a cooperative‑owner trust model.

Separately, `retired_allocations` has no compaction predicate for risk‑cleared releases: `compact_machine_retirements` consumes a `proven` set whose SQL source is not supplied, and item 4 requires these rows be Released **without** a fabricated `SealedRelease`. If the proof query keys on seal presence, these hashes are uncompactable until epoch rotation. Note also that adding them is arguably redundant — if F5's identity fence is enforced, the retired domain's `AllocationKey`s (which embed `domain_id` *and* `manager_store_uuid`) can never recur.

**Minimal fix:** a separate `retired_participants` map holding a minimal fenced identity + audit reference (no secret, no budgets/aliases), excluded from `admission_bytes` and from the `MAX_DOMAINS` bound but consulted by `prepare_pairing`. Decide explicitly whether retirement writes to `retired_allocations` at all; if yes, specify the compaction predicate.

### F9 — `ProcessIdentity` does not identify the requester for an audited risk acceptance. **Severity: Medium**

Item 2: "The server records the OS-authenticated requester identity, not an identity asserted by the JSON caller." The only identity type in play is

```rust
ProcessIdentity::Windows { host_id, boot_id, pid, creation_filetime_100ns }
```

which identifies a *process*, not a principal. It cannot distinguish the administrative role from the runtime role that the stated trust model depends on. (`InstallationIdentity` has an `owner_uid`; `ProcessIdentity` has no user/SID field.) Also, in `hold`/`force_release` the `requester` is an argument supplied by the Rust caller — whether the daemon derives it from the authenticated peer token is not visible in the excerpts.

**Minimal fix:** bind the authenticated token user (SID/uid) into the retirement audit alongside `ProcessIdentity`, and state where it is derived from.

### F10 — Audit/blob filename namespace collides with `prune_machine_blobs`. **Severity: Medium‑Low**

`prune_machine_blobs` deletes **every** file matching `machine-commit-<uuid>-<64 hex>.json` that is not the single kept `pending_machine_blob`, and runs on every `open()` and every `publish()`. A retirement blob reusing that prefix, or reusing the single blob slot, is deleted the next time an ordinary commit blob is kept.

Also worth copying carefully rather than verbatim: `complete_machine_reset` keys its audit path on `reset-{reset_id}-{payload_hash(expected)}.json`. Because the digest is *in the filename*, a differing inventory writes a *second* file instead of tripping the `"reset audit inventory differs"` check. For retirement, where the digest is the entire point, key the audit path on `operation_id` alone so a payload change collides and is rejected.

### F11 — Item 6 abandonment has no third state to land in. **Severity: Medium**

`ParticipantAnchor.committed` is a `bool`, and two independent guards treat `!committed` as blocking:

```rust
State::Ready(registry) if registry.participants.values().any(|p| !p.committed) => Some(("authority_pairing_incomplete", ...))
// and, in complete_machine_reset:
|| registry.participants.values().any(|a| !a.committed)
```

An "explicitly abandoned" pending registration that stays `committed: false` blocks admission and blocks reset convergence forever. The design correctly distinguishes abandonment (never had launch authority) from retirement, but does not say where that distinction is durably stored.

**Minimal fix:** replace the bool with an explicit lifecycle (`Pending | Committed | Abandoned | Retired`), and have both guards treat `Abandoned`/`Retired` as settled. `load`'s schema validation should reject an `Abandoned` anchor that has a session or a non‑zero `accepted_sequence` — that is the executable form of "it has never had launch authority".

---

## Evidence Gaps

These changed how strongly I could state several findings; answers may move F1, F6 and F9 materially.

1. **Transport/session layer not supplied.** Whether an accepted session is re‑validated per request against the durable anchor, and what terminates an open connection on revocation, is unknown. F1 assumes it is not re‑validated at the authority layer — that part I can see; the transport part I cannot.
2. **`Request` authentication/HMAC path not supplied**, so I cannot confirm item 2's "not exposed as an executor-authorized HMAC operation" is structurally enforced rather than conventional.
3. **`ParticipantSnapshot` construction not supplied.** `manager/recovery.rs::begin` validates against it heavily; whether it is derived from `machine_domains` (and therefore whether a retired domain can even produce one) determines whether N8 below is already satisfied.
4. **The `proven` set feeding `compact_machine_retirements` is not supplied** — F8's compaction claim is conditional on that query's predicate.
5. **No admin client / `machine retire-domain` code exists in the excerpts**; the entire design is unimplemented, so all findings are about the specification and its fit with existing invariants, not about defects in shipped code.
6. **No tools, not a git repository, WSL runtime not installed.** I did not compile, run, or verify that the shown tests pass. Per the brief: none of the protocol tests discussed here demonstrate anything about WSL process cleanup, which is unimplemented — the negative controls below prove *bookkeeping and fencing* only, and the design's item 4/5 language should not be read as cleanup evidence.
7. **`MAX_DOMAINS`, `GrantState`, `machine_queue::save_grant`, `payload_hash`** semantics assumed from usage.

---

## Recommendations

**Ordering change that closes the most holes at once (F1 + F6):** make retirement three durable steps — (1) publish the domain fence (session revoked, domain marked retiring; nothing freed, nothing charged changes); (2) compute the preview digest against the now‑frozen domain; (3) accept with digest revalidated *inside* the publication that frees anything. This removes the preview TOCTOU and makes the fence testable without transport code.

**Scope answer to record in the design (F4):** whole‑domain is the smallest defensible *identity* scope, justified by `import_inventory`'s unsealed‑absence rule. It is **not** the smallest *risk* scope: separate identity retirement from capacity reclamation and require a second, separately audited acceptance for the latter.

**Bounding answer (brief's question on broader reset clearance):** broad force‑reset is not needed. Name and bound the two residuals that `prepare_machine_reset_recovery` already excludes — same‑UUID rollback and pre‑coverage native history — as distinct inventory‑level remediations. Keep item 7's "deleting registry/anchors is never a recovery action" as stated; it is consistent with `Authority::open`'s "missing history is closed" behaviour and with `missing_and_lost_history_never_implies_free_capacity`.

### Indispensable executable negative controls

Written in the style of the existing `topology_tests`; each should be a distinct default‑daemon isolated Job where fault injection is required.

- **N1 — no post‑fence writer.** A commit intent naming a retired domain's session is rejected by `proposed_machine_commit`; the same intent replayed through `reconcile_pending_machine_commit` does not create `machine_operations` rows or call `save_grant`. *(F1)*
- **N2 — no re‑pairing.** `prepare_pairing` fails for (i) the retired `domain_id`, (ii) the retired `installation_nonce`, (iii) a **fresh** domain_id + installation_nonce reusing the retired `manager_store_uuid`. (iii) fails today. *(F5)*
- **N3 — crash between fence and SQL commit.** Reopen: the pending retirement reloads, `admission_blocker()` is non‑`None` until it completes, **and the domain's grants are still present in `machine_obligations`** (assert the debit is not freed early). *(F1, item 5)*
- **N4 — crash between SQL commit and finish.** Reopen converges to retired exactly once; replay with the same `operation_id` + payload is a no‑op returning the recorded outcome; same id + different payload is a payload conflict; a different id against an already‑retired domain is rejected. *(F7)*
- **N5 — pending commit interaction.** Retirement while `pending_machine_commit` belongs to *another* domain is rejected with a distinct code; while it belongs to the retired domain, it is subsumed, and a subsequent `finish_machine_commit(op)` neither re‑charges grants nor errors with `"machine commit participant anchor missing"`. *(F3)*
- **N6 — nothing unrelated moves.** `assert_eq!` on the full post‑retirement snapshot minus the retired domain: `native_permissions`, `native_coverage_store`, `holds`, `epoch`, `domains`, and every other domain's `machine_permissions`/`session` are byte‑identical. *(item 7)*
- **N7 — reset converges.** After retirement, `prepare_machine_reset_recovery` does not insert the retired domain with `reconciliation_required=1`; with only live participants reconciled and native proof supplied, `finish_machine_reset_recovery` reaches `complete_machine_reset` and rotates the epoch. This test fails against the current code. *(F2)*
- **N8 — a returning manager cannot import.** If the retired manager's store somehow reappears, the handshake fails before `recovery::begin` can reach `import_inventory`; assert no `attached_inventory` row is ever written. *(F5, F1)*
- **N9 — abandonment is not risk acceptance.** After abandoning a pending registration, `admission_blocker()` is not `authority_pairing_incomplete`, `complete_machine_reset` is not blocked by it, and the audit record is distinguishable from a risk‑accepted retirement; a registration with any session or non‑zero `accepted_sequence` cannot be abandoned. *(F11)*
- **N10 — releases are not proofs.** Assert risk‑cleared grants have `sealed_release == None` and carry the annotation, and that no proof‑consuming path (`compact_machine_retirements`' proof source, `import_inventory`'s seal lookup, `retire_native_starts`) will accept them as evidence of emptiness. *(F4, item 4)*
- **N11 — capacity cannot block retirement.** From the saturated registry fixture already used in `saturated_registry_still_journals_large_release_and_detects_missing_blob`, a retirement publication succeeds even at the admission budget, and repeated retirements do not exhaust `MAX_DOMAINS - 2` or monotonically grow `admission_bytes`. *(F8, item 8)*

---

## Confidence

| Finding | Confidence | Basis |
|---|---|---|
| F2 (reset deadlock) | **High** | Literal `1` in the INSERT vs. the `reconciliation_required!=0` guard; both quoted. |
| F3 (anchor / single slot) | **High** | `ok_or_else` and the byte‑equality branch are explicit. |
| F5 (identity fence unenforced) | **High** | Absence of any `manager_store_uuid` uniqueness check across all four excerpts. |
| F8, F11 (budgets, `committed` bool) | **High** | Direct from `admission_bytes`, `load` bounds, and both `!committed` guards. |
| F10 (prune collision) | **High** | `prune_machine_blobs` prefix logic is explicit; the audit‑naming point follows from the path format. |
| F1 (fence at durable layer) | **Medium‑High** | The authority‑layer gap is certain; whether the transport independently closes it is unverifiable (Gap 1). |
| F4 (scope split) | **Medium‑High** | The conflation is clear from the design plus `complete_machine_reset`'s `machine_permissions.is_empty()`; the right split is a judgement call the owner may reasonably decide differently. |
| F6, F7, F9 | **Medium** | Reasoned from design text + code patterns; F9 depends on Gap 2. |

Overall: I'm confident the design is not yet implementable as written against this codebase, and confident that the *shape* of the fix is a retired‑identity tombstone + a separate pending‑retirement slot + fence‑before‑digest ordering. I'm least confident about F4's exact partition — if the owner's position is that a retired domain's capacity should simply be forfeited until epoch rotation, that is simpler and also defensible, and would replace F4's `retired_residue` with a documented capacity loss.

**Questions that would change my view:** (1) Does the transport re‑validate the session against the durable anchor per request, or only at connection accept? (2) Is `ParticipantSnapshot` derived from `machine_domains`, such that a retired domain cannot produce one? (3) What SQL predicate produces the `proven` set for `compact_machine_retirements`? (4) Is forfeiting the retired domain's capacity until epoch rotation acceptable, or is prompt reclamation a hard MR‑2 liveness requirement?
