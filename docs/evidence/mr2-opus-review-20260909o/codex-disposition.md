# Verified disposition of the three MR-2 reviews

Review target was snapshot o. This is a working disposition, not MR-2 approval.
Scheduled review Job: 01a05f1f-858c-7880-8c15-d55875da9e6b~01a087b2-1ec3-7551-a136-63a4305bba6d.
Later snapshot receipts/manifests in the implementation ledger identify fixes.

| Finding | Verification / disposition |
|---|---|
| Local outbox counts Unicode characters | Confirmed; q uses BLOB byte lengths and tests budget/ack headroom. |
| Unframeable committed replies; incomplete Inspect; 16 reconciliation pages | Confirmed; r bounds outcome before business savepoint commit, adds InspectPage cursor, checks byte/count budgets, exercises 300 large Unicode allocations and public endpoint limits. |
| Replayed commands change their serialization hash | Confirmed; r compares reconstructed command hash to the durable original. Upgrade must retain/reconcile a mismatch. |
| Missing manager path for retired lost acknowledgements and epoch rotation | Confirmed; s adds a durable restricted recovery mode, retained abandoned intents, complete inventory import as cleanup obligations, snapshot builder and sealed release. Its unit controls pass. The public combined lost-Ticket/reset/recovery/epoch-rotation fixture passed in t's test Job; remaining t gates are tracked separately. |
| Ticket freshness and cancellation barrier only documented | Confirmed; u passed a barrier retaining original local send clocks, checking readiness/freshness through commit/release, and typed never-released cleanup. Preview-a passed the real SQLite consumed-but-unused control. MR-3 runtime integration remains. |
| Retired registry growth and insufficient pending-release journal space | Confirmed; w passed segmented 32 MiB commit blobs with bounded 4 MiB registry and admission headroom; x passed exact SQL tombstone compaction/old-key rejection and generated orphan collection. Domain retirement metadata reserves and corruption controls are later follow-ups. |
| Expired candidate rows exhaust active budget | Confirmed; u passed active-count/reactivation controls including retained Armed/Uncertain obligations. Historical rows do not count as active candidates. |
| Same-job reservation/ready ties unstable | Confirmed; u passed explicit final Lease/key ordering. |
| Audited attached Grant/reset clearance and Uncertain transition | y passed ReportUncertain. Retirement-b passed explicit whole-domain retirement with OS principal, exact inventory, four crash boundaries and risk annotation, plus reset convergence. Final focused review and follow-ups remain. |
| Uncommitted registration reset recovery | Retirement-b passed abandonment of a real interrupted external pairing intent, with no claimed cleanup and permanent old-identity fencing. |
| Recovery loses original candidate aging anchor | x passed retention/reconstruction of original coordinator queue identity/time in external Grants. Whole-SQL reset still loses native Job rows, and is not described as preserving native queue history. |
| Offer notification / persistent bridge | MR-2 fixtures poll successfully. Candidate refresh cadence is not discovery cadence. Persistent notification/bridge and idle-budget acceptance remain OPEN for delivery. |
| Cross-domain filesystem alias exclusion bug | Proposed reproducer assumes shared-fence alias registration, which current topology does not implement: aliases map scalar IDs and fences always get the requesting domain scope. No verified current cross-domain fence bypass. Do not claim shared-filesystem aliases supported; WSL shared mounts still require the prescribed mapping work. |
| Configuration hash undiscoverable | Not reproduced: public daemon_status supplies config_sha256; r's InventoryPage also includes it. |
| Every ~14 unanswered outbox operations causes permanent release deadlock | Conservative future-response reservation is real, but permanent deadlock is not established: draining earlier responses frees reserved bytes; acknowledgement compacts. Improve/document backpressure and release reservation, but do not exempt arbitrary release payloads from all byte limits. |
| Consumed ticket can never be cleaned if OS release did not occur | Existing consumed=true cleanup conservatively retires an empty boundary, so absolute deadlock claim is incorrect. Positively proving unused-after-commit was missing; post-t barrier adds a typed path without resetting the consumed bit. |
| Non-durable errors indistinguishable from durable rejection | Not reproduced: outer RPC Response::Error differs from persisted machine Reply::Rejected. Never apply an outer error as a sequence acknowledgement. |
| External session before SQL handshake commit is wrong ordering | Intended fencing: old writer is invalidated before acknowledgement; new ConnectBegin reserves another epoch after a crash. Reversing publication would revive the old writer. |
| Host sample precedes challenge / quiet generation never resets | Current RPC with_release_sample samples after receipt under the provider barrier; service handles cadence/suspend generation and native/attached share quiet helpers. Manager-side freshness still needed as separately confirmed above. |
| Upgrading busy legacy store should silently reconstruct coverage | Rejected as a general fix: incomplete historical Invocation coverage must stay gated. Installer already enforces a drain/admission barrier. Documented audited remediation is still needed for unsupported busy bypasses. |
| Pairing secret in serializable registration is public exposure | Registration is an owner-controlled pairing input; ParticipantSnapshot and AuthoritySnapshot omit it and Debug redacts it. Do not put real registrations into public evidence. |
| enqueue returning an already-answered sequence is a second execution | It is the existing idempotent outcome, not re-enqueue. An outcome lookup API can improve caller ergonomics; caller must not assume a new pending row. |

Required follow-up: complete open coordinator history/bounds/clearance work, run
focused independent review of the final recovery paths, then close MR-2 only with
all required public fault traces. None of these reviews or SQLite start fixtures
constitutes installed WSL or three-round live-consumer acceptance.
