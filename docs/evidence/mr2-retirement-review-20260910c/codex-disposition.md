# Verified disposition: two focused retirement reviews

Both reviewers completed under default protected Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0881a-682a-7893-977d-71f151da06dc`.
Actual model usage includes `claude-opus-5` and the runner's Haiku helper; the
selected route was subscription OAuth/Max, no provider API billing and no tools.
Durations: 569.570 and 536.107 seconds. The exact supplied files/hashes and outputs
are retained beside this disposition. Findings concern that input, not all later
root changes. This is not final MR-2 acceptance.

| Finding | Verification and action |
|---|---|
| No startup retirement recovery (both, explicitly uncertain) | Not reproduced: `Store::attach_authority` in `src/store/authority.rs` calls pending retirement recovery before pending machine recovery and history validation, before native admission. That file was omitted from the curated brief. Root strengthens the public crash test to wait for native completion before replaying retirement. |
| Reserved bytes can be consumed by late metadata (reviewer 1); arithmetic closes (reviewer 2) | Reviewer 1 identified the missing hold-release growth term. Root reserves 64 KiB per live hold as well as 16 KiB per participant, bounds bootstrap proof paths, and checks every growing encoded-plus-reserved footprint against the hard cap. Cleanup spends its own reservation. A new real-pairing test reaches the byte ceiling with 40 live holds and many participants, grows session checkpoints, releases holds with maximally escaped reasons and retires every admitted participant. Passed in e and final f, including Rust 1.85. |
| Divergent SQL after a durable retirement fence can make startup/recovery fail | Confirmed for mismatching Grant/receipt projections. Root records a reset gate while retaining the fence/debit and keeping authority inspection available. Explicit machine recovery reconstructs the SQL projection from continuous external authority and the immutable audit; same-UUID repair additionally requires native quiescence. It preserves unrelated receipts and obligations. Public e control injects an extra SQL Armed row after the journal fault and exercises diagnosis, explicit cancellation, repair, peer retirement and epoch fencing. |
| Returning retired manager sees generic missing-history error | Confirmed diagnostic gap. Root returns `retired_domain` with the original serialized retirement receipt on ConnectBegin; tests require the operation ID. It does not authenticate or reopen a retired session. |
| New operation UUID on an already retired domain returns an irrelevant error | Confirmed. Root checks the retained receipt before trying to preview the removed participant and returns a conflict naming the original operation. |
| Missing-file errors omit the needed path | Confirmed. Bounded reads now include the path for open/read/size/JSON errors. The unit corruption control checks the audit filename; a subsequent root public control removes and restores that exact audit across daemon starts without fabricating history. |
| SID formatting may diverge; other admin RPCs may allow foreign owners | Not reproduced: `current_user_sid_string()` directly calls the same process-token SID formatter. The pipe ACL in `src/daemon/transport.rs` is `D:P(A;;GA;;;<owner SID>)` and rejects remote clients. Retirement records the actual connected process token SID. The cooperative same-owner trust model remains explicit. |
| Retired parent could remain referenced by a live child on load | Added the missing structural load check. Normal retirement already refuses live descendants. |
| Retirement SQL table is lazily created and schema epoch should change | Table creation moved into machine schema initialization. Schema 1 is still the explicitly uninstalled MR-2 additive extension; no installed manager/coordinator schema 1 is being migrated or reset by this edit. Incompatible future changes require an explicit epoch decision. |
| Offered rows have a misleading risk annotation | Root now expires an unused Offer without `risk_clearance`; only released Armed/Uncertain rights carry it. |
| Existing seal could be lost | Not reproduced: a full accepted seal transitions to Released; Armed/Uncertain has no accepted SealedRelease. The immutable audit preserves all prior Grant fields and issued intents. No partial seal is represented as ProvenEmpty. |
| Finite identity capacity is obscured by a larger count ceiling | Root exposes registry bytes, hard limit, reserved recovery bytes and admission headroom on AuthoritySnapshot. Byte limits can bind before count limits. Retired identities remain fenced; no ordinary epoch rotation discards them. |
| SQL/audit history remains retained | Intentional. SQL projections and events can disappear on whole-SQL reset; the authoritative receipt/fence and full audit are external. Completed audit-file loss does not erase the receipt or authorize identity reuse. The current implementation does not promise unlimited new registrations. |
| Same-UUID rollback and native precoverage require a broad force-reset | Both reviewers reject a broad reset and distinguish optional follow-up scopes. Root d independently passed a restricted covered-history repair: preserve native Jobs, require explicit normal cancellation of unfinished rows and native cleanup proof, reconcile all managers, then rotate epoch. Uncovered/missing authority remains gated; restoration of the exact durable file is the supported repair for file loss. No native risk-clearance is inferred from an empty replacement DB. |

The current root also acknowledges archived manager intents only after complete
reconciliation and release of all local allocations, in the caller's lifecycle
transaction. That d public control passed; Grant/Ticket/cleanup history is retained.

Scheduled e/f gates, final Rust 1.85 tests and release build passed; see
[final-f evidence](../mr2-final-20260910f/). Remaining before closing this slice:
focused verification of the new reservation and stalled-retirement repair deltas
and installed alpha.17 verification. These protocol controls do not establish WSL
runtime containment or live-consumer acceptance.
