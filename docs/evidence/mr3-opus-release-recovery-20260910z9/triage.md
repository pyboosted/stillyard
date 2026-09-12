# Codex disposition — scoped release recovery review

The review is not an MR-3 exit verdict. The original two actual retained Grants
were released after z9 installation through the normal protocol (installation
evidence), without SQL repairs or forced clearance.

1. Candidate revision drift: rejected as stated. `CandidateUpsert` refresh sends
the same durable candidate revision; it does not increment the coordinator's
candidate revision. Global coordinator revision is a separate counter. The
manager's authenticated ordered channel prevents foreign/reordered mutation of
this allocation. Accepted cancellation retires the local plan. The reported
revision-1 loop is fixed by sending revision 2; actual installation recovered it.
2. Duplicate in-flight cancels: rejected. `driver::exchange` checks pending
outbox work before calling maintenance (driver.rs); maintenance's caller
contract explicitly requires a drained outbox. No new cancellation is enqueued
while its predecessor waits for a reply. Rejected requests can repeat, but the
claimed missing in-flight guard is not an actual execution path.
3. Recovered-ticket cleanup false: rejected. The query excludes tickets present
in the continuous local store. Runtime cannot obtain/consume a Ticket or record
release intent without accepting it in that store. Issued by coordinator does
not mean consumed or even received by executor. This branch requires actual
executor cleanup and is not a timer-based release.
4. Contradictory NeverReleased: rejected. `record_unused_cleanup` intentionally
requires consumed=true, binds the transient barrier proof, and populates
cleanup_json. The subsequent apply query selects only cleanup_json IS NULL, so
it cannot overwrite that proof. Requiring consumed=0 would break the contract.
5. Stop/write-lock deadlock: not reproduced; service supervisor stops the pinned
empty manager without requiring a shutdown SQL write. This ordering is the
necessary admission barrier. Accepted hardening: recheck journal/kernel after
the process has exited, before replacement; preserve the barrier throughout.
6. Retained postcondition: accepted helper usability improvement. The actual
installation controller already asserted the exact two original Grants released.
Move this check into the general explicit-recovery updater as well.
7. Path traversal: installed configuration validation already rejects ParentDir
and CurDir; kernel registry pins real cgroup identity. A malicious same-owner
anchor is outside the cooperative threat model. Still accept lexical/canonical
path equality as independent maintenance defense.
8. UUID formatting drift: failed-closed, no observed producer; canonical typed
IDs are already written by the daemon. No speculative normalization migration.
9. Regression test scope: the old implementation fails both Release-first and
revision-2 assertions. The real failed installed round and successful recovery
are additional evidence. Additional real process-death controls follow in z10;
those do not change this review into full MR-3 acceptance.

Accepted helper hardening is pending z10 validation. No verified resource-safety
regression in the installed z9 recovery remains from this review.
