# Phase 5 — Safe managed waits

Status: delivered alpha.5 bounded contract (2026-08-28)

This increment lets an authenticated managed process synchronously wait for its own descendants
without creating an ancestor-resource deadlock. It implements the bounded v0.12 rule, not a
general wait graph. Cancellation, cascade semantics, Conditions, probes, postconditions, retries,
and event subscriptions remain later increments.

## Atomic admission

- `SubmitOptions::with_wait_for_completion()` and CLI `submit --wait` declare one combined
  submit-and-wait operation. The request carries this intent into the daemon's acceptance
  transaction for a Job or an entire Batch.
- Managed acceptance inserts the proposed Jobs and dependencies transactionally, evaluates the
  resulting closure, and commits only if the wait is safe. Rejection rolls back every Job, Batch,
  and dependency row before retaining the Submission rejection decision.
- A `received` Submission durably retains the wait-intent bit. This is an acceptance qualifier,
  not a live wait edge: it prevents daemon recovery or a same-key retry from accepting work
  without the check that the original combined operation requested.
- Replaying an already accepted detached Submission with wait intent first returns and persists
  the existing receipt. The separate authenticated `wait` may then reject. It never creates a
  duplicate Job, and exit 27 cannot be mistaken for a fresh rejection that created no work.
- Rejected Submissions retain their machine-readable code and detail. Fresh submit responses,
  same-key replay, `recover`, and result files therefore agree on `blocked_by_ancestor` or
  `resource_capacity` instead of degrading to a generic rejection.

## Server-authenticated wait

- Every bounded `wait` protocol request is checked independently. The daemon derives the caller's
  parent Job, Attempt, and Invocation from the named-pipe peer's membership in a live Windows Job
  Object. Environment IDs are only an equality claim and cannot grant authority.
- A managed caller may wait only for authenticated descendants of its current Attempt. Foreign,
  unrelated, stale-parent, disabled-parent, and ambiguous-containment requests fail closed.
- A missing daemon-held handle is unknown authority, never evidence that the peer is unmanaged.
  The daemon fails closed rather than bypassing descendant and Lease checks.
- Authority candidates are limited to `live` Containments from the current daemon generation.
  Unproven cleanup commits `uncertain` before unpublishing its handle and closing the kill-on-close
  Job Object. A process in that teardown interval has no managed authority; retained uncertain
  Leases remain unavailable to the scheduler without globally blocking unrelated submissions.
- The daemon walks each target's unfinished predecessor closure. Reaching the waiter itself is an
  immediate `blocked_by_ancestor` rejection.
- For every unfinished Job in that closure, the daemon compares its complete resolved scalar and
  shared/exclusive path-fence claims with the granted or retained Leases of the waiter and every
  authenticated ancestor. A scalar is rejected only when ancestor retention leaves insufficient
  configured capacity. Unrelated running Jobs are intentionally excluded because they can finish
  while the caller waits.
- A closure claim above the host's entire configured scalar capacity is rejected as
  `resource_capacity`, even when no ancestor retains that component; such a Job cannot become
  runnable during the live parent Attempt.
- The public crate returns `Error::ManagedWaitRejected`; the CLI returns scheduler exit 27 and a
  machine-readable `blocked_by_ancestor` or `resource_capacity` protocol reason.

## Disposable operation boundary

No Wait, WaitEdge, client session, or queue position is persisted. A safe wait is repeated as
bounded authenticated reads over the local protocol. Client disconnect releases only that
operation: accepted children remain durable and continue according to their Jobs and dependencies.
Detached managed submission deliberately skips synchronous-wait admission, so a conflicting child
may queue behind its parent; a later managed attempt to wait for it is still rejected safely.

## Evidence

Deterministic tests cover scalar and fence component conflicts, total-capacity rejection,
orthogonal resources, missing-handle fail-closed behavior, exact rejection replay, the complete
ancestor chain, unfinished predecessor closure, self and foreign targets, detached submission,
and atomic mixed Batch rejection. The live consumer smoke runs an enabled parent through the
shipped daemon and CLI, proves a nested parent → middle → grandchild chain can submit and wait to
completion, proves an ancestor-conflicting combined operation exits 27 without creating a child
Job, and proves a detached conflicting child survives until its parent releases the Lease.
