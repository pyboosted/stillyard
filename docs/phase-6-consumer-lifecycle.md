# Phase 6 — Consumer lifecycle

Status: delivered alpha.6 bounded contract (2026-08-28)

This increment closes the runtime and public-crate gaps found by the first external consumer,
`moot`. It completes the existing Job → Attempt → Invocation → Containment → Lease lifecycle; it
does not add a workflow engine, durable Wait entity, or a second scheduler.

## Postconditions and retry

- `JobSpec::postconditions` declares ordered executable validators with accepted and retryable exit
  code sets; every other exit is failed. Code sets are unique and disjoint.
- A postcondition starts only after the preceding primary/postcondition Containment is proved empty.
  It gets its own born-contained Invocation and canonical provenance while the Attempt's complete
  Lease remains granted.
- `timeout_seconds` establishes one Attempt deadline shared by the primary and every postcondition;
  starting a validator never refreshes the budget.
- `RetryPolicy` accepts known Attempt verdict names. A retryable settled Attempt releases its Lease,
  waits for the finite backoff without polling, and creates a new ordered Attempt under the same
  Job. Cancel, timeout, and interruption never retry.
- The public snapshot exposes all Attempts and Invocations, postcondition exit classification,
  bounded diagnostic tails, executable hash, daemon generation, and Containment state/incident.

## Plain cancel

- `Client::cancel` and `stillyard cancel JOB...` atomically select explicit Job IDs only.
- A pending Job becomes canceled without an Attempt. A running Job receives a durable cancel
  request; its runner terminates the complete Job Object, proves it empty, settles the Attempt
  canceled, releases the Lease, and suppresses retry.
- `JobSnapshot::cancel_requested` makes acceptance of an active cancellation immediately visible
  before bounded Containment cleanup reaches the final state.
- Repeating cancel is idempotent. Plain cancel does not traverse authenticated children or
  dependency successors, so a terminal-dependent collector remains runnable.
- Cascade, drain, and force remain later baseline increments.

## Impact admission and daemon identity

- `HostConfig::impact_incompatibilities` is interpreted symmetrically. Ordinary admission reports
  `impact_busy`; a managed wait whose child conflicts with an ancestor-held impact fails as
  `blocked_by_ancestor`. An impact remains self-compatible unless explicitly configured otherwise.
- Job and Batch receipts preserve the daemon generation that accepted the Submission across daemon
  restart. `DaemonSnapshot` reports the current generation, active profile names, capacities, and
  a canonical SHA-256 of the loaded non-secret `HostConfig` representation.
- The durable result-file format is version 4 because accepted receipts now include their accepting
  daemon generation.

## Evidence

Deterministic store tests cover Lease retention through postconditions, two Attempts under one Job,
ordered public snapshots, queued/running cancel, retry suppression, terminal-successor preservation,
impact admission, ancestor impact conflict, and accepting-generation persistence. Live Windows
tests run real primary and postcondition processes through separate born-contained Job Objects,
exercise retryable then accepted validation, and terminate a running Containment through plain
cancel.
