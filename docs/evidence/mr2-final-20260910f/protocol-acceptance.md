# MR-2 protocol acceptance on final-f

Source file-map: `8ba4d5c8034551d15dd8ec5c843b2d9eef2da054911cc86b7ded1a7cd5ac8e74`.
Default system test Job: `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08835-84cd-7662-a3fe-47935513878e`.
Rust 1.85 repetition: `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08835-84d6-7462-9fd3-50a0b6810ff1`.
Both succeeded. Canonical logs/status and submitted specs are beside this report.

The test participant owns real SQLite journal transactions but simulates attached
execution/cleanup. Native marker/ping processes are real Windows Jobs in isolated
coordinators supervised by the default system Job. No row below proves Linux
containment, installed attached execution or W-C consumer acceptance.

| Control | Executable evidence in this snapshot | MR-2 result | MR-3 result |
|---|---|---|---|
| M-A01 one machine token | `machine_pairing_reconnect_fences_challenge_replay_and_store_reset`: Armed attached allocation blocks a real native marker until sealed release; ordered native/attached events corroborate grant order | pass | not_run |
| M-A02 two compatible allocations | Same fixture: capacity two permits simultaneous attached Armed and native running allocation, machine granted=2 | pass | not_run |
| M-A03 no head-of-line blocking | Same fixture: higher-ranked cargo reservation does not hide a side-token candidate; conversion precedes native waiter; withdrawal permits native progress | pass | not_run |
| M-A04 duplicates/durable boundaries | `machine_arm_release_crash_boundaries_recover_without_freeing_live_rights` injects before_journal/after_journal/after_sql/after_ack around Arm, primary/postcondition tickets and sealed snapshot commit. Manager outbox tests lose Arm/Ticket/release responses and retain one consumed ticket/start. Authenticated reordered/conflicting sequence and moved operation UUID reject | pass | not_run |
| M-A05 disconnect/lost ack | Manager fixture reconnects durable outbox after coordinator restart, recovers missing response and release; no manual clearance on ordinary reconnect. Armed Offer outlives original TTL | pass | not_run |
| M-A06 reset/history loss | Public allocation, authority, manager and clearance fixtures cover whole-SQL reset, partial rollback, missing/corrupt pairing/commit/audit, lost manager Ticket reply and rotated epoch. Unknown history retains debit. Exact audit restoration recovers; irretrievable manager can be explicitly retired with immutable risk audit | pass | not_run |
| M-A07 stale/duplicate peer | Public allocation fixture fences old challenge, connection epoch and changed store. Retirement fixtures reject retired domain/nonce/store reuse and return original receipt; other live participant remains unchanged | pass | not_run |
| M-A09 roles/lifetime | `machine_probe_ticket_cannot_borrow_work_or_postcondition_authority` and primary fixture enforce distinct probe allocation, ordered per-Invocation tickets and complete cleanup inventory; real native probe test checks separate probe/work allocations | pass | not_run |
| M-A11 impacts/freshness | Allocation fixture holds real native cpu_heavy work while spare scalar capacity exists; attached measurement receives no Offer. Host CPU quiet interval gates ticket and replay preserves old issuance. Manager release unit controls reject delayed reply, cancel, readiness change and clock discontinuity | pass (protocol/host evidence) | not_run |

Additional required protocol controls: candidate cancellation and Offer expiry reject
late Arm and higher-revision revival; capacity shrink retains outstanding debit and
rejects stale-configuration tickets; version/hash/identity mismatch reject; active
Uncertain rights remain charged. Manager release barriers recheck readiness before
and after the caller's durable transaction and require exact unused cleanup after
a consumed ticket that never reached OS release. These controls are found in the
public allocation fixture, `machine::manager::release` tests and machine wire tests.

The bounded registry controls reach actual publication limits, grow every admitted
participant's session, discharge all reserved live holds and retire all admitted
participants. A saturated segmented journal still completes a large release and
a full retirement; missing referenced blobs close history. These limits are
observable, finite operating limits; no unbounded identity registration is claimed.

Final independent review disposition and installed alpha.17 verification must be
recorded separately before closing MR-2. Linux runtime work remains MR-3.
