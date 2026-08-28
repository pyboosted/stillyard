# Phase 3 — Agent-ready execution path

Status: delivered increment-2b contract (2026-08-28)

This bounded increment makes the existing scheduler usable by the motivating local agent
orchestrator. It does not add Conditions, retries, observed RAM/VRAM, quiet admission, nested
submission, cancellation, retention, or the TUI.

## Staged stdin

- `StdinSpec::File.path` is a client-side source path. It is never opened by the daemon.
- The client hashes the complete file, then uploads it in bounded protocol chunks to a random
  upload ID. The daemon writes only an upload-specific partial file.
- Commit verifies the declared length and SHA-256, flushes the file, and publishes one immutable
  content-addressed blob. A disconnect before commit leaves no Submission; abandoned partials are
  collectable.
- Job submit carries one committed blob reference. Batch submit carries references keyed by member
  name. The normalized idempotency hash covers the specification and these references.
- The daemon revalidates every referenced blob before the `received` transaction. That transaction
  stores the input references; Batch acceptance copies them to every Job in the same transaction.
- The runner opens the staged blob, verifies length/hash, rewinds it, and passes that exact handle as
  stdin. EOF jobs continue to receive a real null/EOF handle.

## Explicit clean environment

- `EnvironmentSpec` contains only explicit per-Job `set` and `unset` operations. Names are compared
  with Windows environment semantics in v0.1, and conflicting operations reject the Job.
- Acceptance persists the exact validated environment with the Job; there is no daemon-local
  preset, merge, or precedence layer.
- The runner starts with only the documented Windows base, applies the Job environment, then
  injects Stillyard coordinates. The daemon ambient PATH and unrelated variables are absent.

## Recovery and passthrough

- A fresh result file is atomically created before staging or submission and records the key and the
  normalized hash including staged-input hashes plus endpoint/store identity. A successful response
  or recovery atomically replaces it with the latest durable decision. `unknown` and a foreign store
  fail closed without overwriting the last durable receipt.
- Passthrough reads only committed canonical stdout/stderr ranges, tracks explicit offsets, and
  drains both streams through final EOF. Reconnection may restart at explicit offsets or zero and
  does not claim exactly-once writes to external handles.

## Required negative controls

- disconnect or injected failure before upload commit creates no Submission;
- altered/truncated staged bytes reject before `received` and never run;
- a payload replay with a different stdin hash conflicts;
- partial Batch input mapping creates no Batch or Job;
- ambient-variable inheritance and locked-variable override mutants fail;
- direct child-pipe passthrough and uncommitted-log-tail mutants fail;
- an existing result file rejects fresh submit, while recovery never creates work.

## Consumer smoke

The deterministic `staged_stdin_handle_reaches_the_contained_process` test exercises the same
shipped path as an agent prompt: a prompt-sized byte sequence is staged, accepted, opened by the
runner, inherited as the managed root's stdin, and observed again in the committed canonical log.
`environment_block_has_exact_path_and_no_daemon_ambient_user_environment` pairs it with the clean
environment boundary required by Codex/Claude launchers. A CLI consumer uses that path as follows:

```text
stillyard submit --spec reviewer.json --idempotency-key UUID \
  --result-file reviewer.result.json --wait --passthrough --silent
stillyard recover --result-file reviewer.result.json --wait --passthrough --silent
```

The first command creates the result file before staging. The second command only recovers the
retained decision and never creates replacement work. `--silent` is mandatory for a transparent
cargo/agent shim: without it scheduler JSON is written to stderr separately from stdout but shares
the child's stderr handle. With it, stdout and stderr contain only canonical committed child bytes.
Authenticated managed `not_received` resubmission is delivered by
[Phase 4](phase-4-managed-submissions.md); an unmanaged missing decision remains deliberately
`unknown` and cannot authorize replay.
