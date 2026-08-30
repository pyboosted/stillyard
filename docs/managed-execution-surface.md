# ManagedExecution public surface

Status: implemented for `0.1.0-alpha.11`, local protocol 16, Windows v0.1.

This amendment makes Stillyard the single owner of:

```text
Submission -> Job -> Attempt -> Invocation -> Containment -> postcondition
```

A consumer keeps only its stable task meaning and immutable domain input, a Stillyard
`SubmissionRef`/Job ID, and its domain result. It does not mirror Attempt, Containment, cleanup, or
submission-recovery state.

## Rust API

The public entry points are:

```rust
Client::ensure_job(JobSpec, &EnsureOptions, deadline, cancellation)
    -> Result<EnsureOutcome<EnsuredJob>>

Client::ensure_batch(BatchSpec, &EnsureOptions, deadline, cancellation)
    -> Result<EnsureOutcome<EnsuredBatch>>

Client::wait_outcome(JobId, deadline, cancellation) -> WaitOutcome
Client::wait_with_passthrough_outcome(...) -> Result<WaitOutcome>
```

`EnsureOutcome` is `accepted`, `pending`, `final`, typed `rejected`, typed `conflict` with the
existing/requested normalized hashes, or `unknown`. `unknown` never authorizes replay. When a
result file already exists, ensure validates its endpoint, store, key, parent, and payload under the
same per-file lock used for atomic replacement, then recovers it before deciding whether exact
managed `not_received` replay is legal. The file is a versioned receipt projection, not a lifecycle
journal. `ensure_job`/`ensure_batch` require a `Client` constructed with an explicit
`ClientBuilder::endpoint`; ambient `STILLYARD_ENDPOINT` may authenticate matching managed-parent
coordinates but never selects the instance for ensure.

`WaitOutcome` is `pending { reason }`, `final { snapshot, root_exit_code }`, `unavailable`, or
`gap_or_unknown`. Deadline and cancellation affect only this disposable wait. The legacy `wait`
methods remain compatibility adapters that translate typed pending back to their former error.

## CLI

```text
stillyard --endpoint PIPE ensure --spec JOB.json --idempotency-key UUID \
  [--result-file receipt.json] [--wait] [--passthrough]

stillyard --endpoint PIPE ensure --batch BATCH.json --idempotency-key UUID \
  [--result-file receipt.json] [--wait]
```

`ensure` and `wait` emit reports containing `exit_source` and `exit_code`. Scheduler-owned codes are
0, 20-27, 64, 69, and 70. A final primary code is returned directly only for a final
`succeeded`/`process_failed` Attempt and only outside that namespace. Consequently a terminal root
exit 25 is JSON `final` with `root_exit_code: 25`, `exit_source: scheduler`, and process exit 20.

The recorded output schema is printed by `stillyard schema managed-execution` and checked in as
`schema/stillyard-managed-execution-v1.json`. Its root is an untagged union of the actual bare
`EnsureReport`, `WaitReport`, and `PrimaryInvocationResult` JSON documents emitted or injected by
these surfaces.

## Primary cleanup and postconditions

After complete-tree termination and empty proof, the daemon records one schema-versioned
`PrimaryInvocationResult` in the Attempt before it can prepare a postcondition. Preparing the
postcondition transaction rechecks the primary Containment row is still `empty` and the Attempt
Lease is still `granted`. The value appears in `AttemptSnapshot.primary_result` and in the
postcondition's `STILLYARD_PRIMARY_RESULT` JSON environment value. No daemon database or private
path is exposed.

| Primary resolution | Postcondition starts? | Primary result |
|---|---:|---|
| exit 0 | yes | `succeeded`, `exited`, root 0 |
| nonzero exit | yes | `process_failed`, `exited`, exact root code |
| start failure | no | recorded after empty proof, no root/start time when absent |
| timeout | no | `timed_out`, `timeout`, observed root code when available |
| interrupt | no | `interrupted`, `interrupt` |
| cancel | no | `canceled`, `cancel` |
| safety failure before launch | no | no fabricated primary result |
| cleanup uncertain | no | no false empty result; Lease retained |
| daemon recovery | no new postcondition for the interrupted Attempt | existing result is preserved; prepared/started postcondition recovery follows R-RUN-5 |

If a cancel or Attempt deadline wins after the primary result but before the next postcondition
release, the remaining postcondition sequence does not start. Retry creates a new Attempt and a new
primary result. A crash after empty proof but before the primary-result transaction is a permitted
fail-closed boundary: recovery settles that Attempt `interrupted`, exposes no fabricated result,
and starts no postcondition. Once the result is durable, recovery preserves that exact value.

`started_unix_millis` and `exited_unix_millis` are optional because a pre-release start failure has
no truthful process-start or process-exit timestamp; `resolved_unix_millis` is always present.

## Acceptance evidence mapping

- `tests/isolated_daemon.rs`: concurrent exact ensure convergence, typed different-payload conflict,
  client-deadline pending, later final, and CLI root-exit-25 separation.
- `src/store/tests/attempt_lifecycle.rs`: durable primary-result requirement, result immutability,
  live-vs-empty mutation, and retained-Lease mutation.
- `src/runner/windows.rs`: primary grandchild destruction before postcondition, typed environment
  transfer, and public snapshot equality.
- `src/store/tests/store_recovery.rs`: durable result survives crash with exactly the documented
  interrupted lifecycle and Lease handling.
- Existing Batch, managed-parent scope, received recovery, result-file atomicity, unknown recovery,
  and uncertain-containment tests remain the negative-control foundation for A-02/A-03/A-10/A-18.
