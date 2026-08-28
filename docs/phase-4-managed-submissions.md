# Phase 4 — Authenticated managed submissions

Status: delivered alpha.4 bounded contract (2026-08-28)

This increment implements the loss/recovery path required by the agent-orchestration consumer. It
does not implement synchronous managed-wait admission, ancestor-resource deadlock analysis,
cascade cancellation, retries, or a general wait graph.

## Authority and parentage

- `JobSpec.allow_child_submissions` is false by default. Only an enabled, running primary may
  create managed children.
- Every primary Invocation owns a fresh unnamed Windows Job Object. The runner registers its live
  handle in a daemon-local containment registry; no public kernel name or reopenable authority is
  introduced. The same handle remains the kill-on-close process-tree boundary.
- For each named-pipe connection the daemon asks Windows for the client PID and immediately opens
  and retains that process handle before reading the request frame. Process creation time must
  predate the observed pipe connection, closing the PID-reuse interval. It checks the retained peer
  against daemon-held Job handles. Exactly one enabled, current match produces
  `ManagedParent { job_id, attempt_id, invocation_id }`; a disabled, root-exited, uncertain, stale,
  or ambiguous live-handle match rejects rather than becoming unmanaged. An unexpectedly missing
  handle for the current Invocation fails authentication closed. On teardown the runner unpublishes
  the handle under the registry mutex before closing it, so membership inspection can never use a
  closed or recycled handle value. Kill-on-close then removes any cooperative process tree.
- `STILLYARD_JOB_ID`, `STILLYARD_ATTEMPT`, and `STILLYARD_INVOCATION_ID` are only a client claim and
  provenance. When present, the daemon compares that claim to OS-derived membership; an absent
  claim makes no assertion and the OS result still wins. The variables are not bearer authority.
- Before creating a result file the client performs `submission_context`, obtaining the current
  store UUID and authenticated parent. Submit and recovery carry that expected identity and the
  daemon derives it again on their separate pipe connections.
- The `received` and acceptance transactions both recheck that Job and Attempt are current and
  unselected, the primary Invocation was started by the current daemon generation with no recorded
  root exit, its Containment is live, and child submission remains enabled. Child Jobs and receipts
  durably record all three parent IDs.

## Scoped recovery

- Idempotency scope is the current store plus `unmanaged` or the authenticated parent Job/Attempt,
  followed by the caller's key. Invocation is retained as provenance but does not fork one Attempt's
  logical operation.
- Missing unmanaged history is `unknown`.
- Missing managed history is `not_received` only while the same enabled parent primary remains the
  live current Invocation. A disabled, root-exited, settled, foreign, or replaced parent cannot
  prove absence and therefore yields `unknown` or an authentication rejection.
- Existing `received`, accepted, rejected, and conflicting records retain their ordinary
  idempotent decisions. Recovery never stages input and never creates work.

## Result-file replay

Result-file version 3 records endpoint, store UUID, key, normalized payload hash, authenticated
parent, and one typed `RecoveryResult`. Fresh creation remains atomic and create-if-absent. Updates
take a per-result-file sidecar lock, reread the current record under that lock, validate its immutable
identity, and advance the decision monotonically before atomic replacement. A repeated accepted
decision is identified by its durable Submission/Job (or Batch/member) IDs; changes to live queue,
estimate, or state fields neither replace nor invalidate the already durable receipt.

An existing result file authorizes submit only when:

1. the authenticated current caller matches its parent exactly;
2. endpoint, store UUID, key, and normalized payload/input hashes are byte-for-byte identical; and
3. its latest durable decision is exactly `not_received`.

The client then restages immutable inputs and submits the same operation. A concurrent completion
of the interrupted upload converges through the same idempotency record; a stale recovery writer
cannot replace an accepted/rejected/conflict decision with `received` or `not_received`. `unknown`, accepted,
rejected, conflict, missing/corrupt receipt, unmanaged scope, or a later Attempt cannot authorize
replay. A durable conflict response replaces a prior `not_received` receipt. Authentication,
parent-liveness, and other pre-`received` errors are non-decisions and are not mislabeled as a
durable rejection; a later recovery records rejection only when SQLite retains that decision.

## Negative controls and consumer smoke

Deterministic tests cover:

- one enabled OS-containment match, disabled/stale containment, and ambiguous membership;
- `not_received` only for the live current parent, including root-exited and prior-generation
  mutants;
- exact replay returning one child Job with committed parentage;
- changed-payload conflict and late/disabled acceptance creating no work;
- result-file reuse after `not_received`, with `unknown`, foreign parent, conflict, and stale-writer
  monotonicity mutants.

The live Windows smoke used the shipped daemon, CLI, SQLite store, named pipe, Job Object, staged
64 MiB stdin, canonical logs, and result-file code. A submission-enabled parent launched a child
CLI, the CLI was killed after publishing its initial receipt but before `received`, and a second
shim process in the same parent Attempt observed exit 27/`not_received`. It then reused the exact
key/hash/file, restaged the input, accepted one child, and waited to exit 0. A later unmanaged
recovery attempt against a managed receipt failed identity validation and left the file byte-identical.
