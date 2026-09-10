# Final focused review disposition

One Opus reviewer completed in 697.084 seconds under default system Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08836-a0ad-7790-a465-71c80624eb1b`.
Subscription OAuth/Max, tools disabled; actual model usage `claude-opus-5` plus
`claude-haiku-4-5-20251001` helper. Exact input manifest, curated brief, raw result
and canonical evidence are retained here. The brief used final-f file-map; the
review's closing reference to e/pending f tests is stale wording, not a source
identity. f gates and matched protected Linux check succeeded.

The reviewer confirmed the metadata-reservation arithmetic and stalled-retirement
repair, with no reproducible safety defect in those changes. Verified follow-ups:

| Finding/gap | Verified disposition |
|---|---|
| New submissions can starve covered same-UUID repair | Confirmed. g rejects new single/batch submission before creating a Received row while that gate is active. Existing idempotent receipt replay precedes the check, and cancellation remains available. The public rollback fixture now queues before the gate and checks both post-gate submit and post-cancel batch refusal. Passed in h stable and Rust 1.85 tests. |
| Returning peer during stalled retirement gets untyped error | Confirmed. g emits `retirement_pending` with the original operation ID without presenting retirement as completed; completed receipt lookup scans past unmatched/suppressed entries. Divergent public fixture checks the typed diagnostic. |
| Older registry may lack reserved cleanup space | Confirmed upgrade limitation. g exposes a clear legacy-budget warning alongside exact byte/reservation values before releases, retaining all obligations. A unit fixture loads checksummed pre-reservation history with 71 holds and checks this warning. This does not manufacture extra storage or claim automatic repair of arbitrarily saturated legacy history; the actual installed registry's budget must be checked before upgrade. |
| A: attached Arm may bypass reset blocker | Not reproduced. `machine_exchange` passes `authority.blocker.is_some()` into `apply_command`; CandidateUpsert, Offered-to-Armed and AuthorizeInvocation reject it. Replayed existing Armed rights do not mint a new ticket. These functions were omitted from the curated brief. |
| B: SQL rebuild might lose Ticket identity uniqueness | Not reproduced. `machine_queue::save_grant` inserts every issued invocation/containment/intent into `machine_ticket_identities` and checks the exact conflict key. Rebuild calls this function. g's surviving-peer fixture checks these SQL fences explicitly. |
| C: surviving peer with rights not exercised after rebuild | Confirmed evidence gap. g enables a second Armed participant with issued Ticket in the divergent-reset branch; requires byte-identical preview after rebuild and exact Ticket identity rows. It authenticates that surviving peer, rejects fresh work while gated, then accepts its full sealed snapshot and rotates epoch without retiring it. Passed in h stable and Rust 1.85 tests. |
| D/E omitted constants/proof bound | `machine::MAX_DOMAINS=256`; `bootstrap::validate_proof` bounds cgroup_path to 4096 bytes and rejects NUL. Present in code supplied in earlier reviews; no new defect. |

No broad force reset, implicit risk acceptance, automatic Job deletion or fake
Linux cleanup was added. g exposed an outdated test count (two retained Grants versus one); h corrects that assertion.
h stable and Rust 1.85 tests passed the full surviving-peer path, and all final
Windows gates succeeded. Installed alpha.17 verification succeeded in ../mr2-installed-20260910h/; MR-2 is complete.
