# Review-fleet consumer case — simplified v0.12 disposition

Status: incorporated into `requirements.md` Draft v0.12.

The consumer case remains a generic orchestration workload. `fleet` owns model/account/host selection, prompts, worktrees, validation policy, and cross-host quotas. Stillyard owns host-local durable submission, resource admission, process containment, logs, and observation.

| Consumer requirements | v0.12 requirements | Disposition |
|---|---|---|
| C-STDIN-1 | R-SUB-2, R-JOB-1..2, R-STORE-3, A-15 | Accepted as completely staged immutable stdin or immediate EOF. Partial upload creates no Submission. |
| C-ENV-1..3 | R-ENV-1..2, R-SUB-3, A-15 | Accepted with account/toolchain selection as explicit self-contained Job environment data. Named host presets and locked precedence rules are rejected as hidden per-daemon state; reusable consumer templates may generate Jobs. Reserved managed coordinates remain server-owned. |
| C-ENV-4 | R-ENV-5, R-LINUX-1..5, A-20 | Accepted with a smaller Linux base. XDG/DBUS/SSH/display variables are not inherited; the Stillyard endpoint is injected directly. A Job may add application-specific runtime variables explicitly. |
| C-SUB-1 | R-SUB-1..4, A-02..3 | Accepted as one durable Submission whose Batch/Jobs/DAG commit atomically in SQLite. |
| C-SUB-2..3 | R-JOB-1, R-SUB-8, R-OBS-3..5, R-CLI-1..2 | Accepted. Explicit IDs and retained Batch IDs are exact; label selectors snapshot bounded currently retained membership without claiming evicted history. Set wait streams settlements and a final aggregate; Any stops after the first final member. |
| C-SUB-4 | R-JOB-6, R-OBS-4, A-15 | Accepted as bounded informational artifact snapshots; a postcondition enforces required output. |
| C-SUB-5 | R-PKG-5 | Accepted as one generated public schema shared by crate, CLI, and daemon. |
| C-SUB-6 | R-SUB-1, R-SUB-5..7, R-OBS-1..2, R-CLI-1..2, A-02, A-12, A-18 | Simplified. SQLite Submission is the exactly-once decision boundary. `--result-file` is an atomic recovery receipt, not a hash-chain journal; `recover` never itself resubmits. For a current managed parent Attempt whose idempotency history is pinned, `not_received` permits the wrapper to restage the exact same payload/key; `unknown` fails closed. Passthrough resumes from explicit canonical offsets or zero and makes no external-handle exactly-once claim. |
| C-NEST-1 | R-ENV-2, R-NEST-1, R-ENV-4, A-11 | Accepted with server-derived parent Job/Invocation from OS containment; no bearer parent field. |
| C-NEST-2 | R-JOB-5, R-NEST-2, R-RUN-4..7, A-10, A-14 | Accepted. Explicit cascade returns the complete selected closure. Cascade, parent timeout, force, and uncertain cleanup cancel unfinished authenticated descendants; ordinary completion and plain cancel do not. |
| C-NEST-3 | R-NEST-3..5, A-11 | Deliberately simplified. v0.1 has no durable WaitEdge or general hypergraph. Managed waits target only authenticated descendants and reject before combined acceptance when a target dependency or next primary/probe/postcondition claim conflicts with waiter/ancestor Leases. |
| C-POST-1..3 | R-JOB-4, R-RUN-6, R-JOB-7, R-RES-7 | Accepted. Postconditions classify accepted/retryable/failed, expose diagnostics publicly, and feed Job-owned retry. |
| C-RES-1..3 | R-RES-1..8, A-04..7 | Accepted. Built-in and multiple custom scalars plus shared/exclusive path fences grant in one Lease. Missing-path identity uses ancestor identity plus canonical remainder. Sidecar and strict-quiet GPU lanes share the scheduler with CPU/cargo work. |
| C-RES-4 | R-JOB-2..5, R-DOM-4, R-RUN-4, R-NEST-2, A-10, A-18 | Accepted as a consumer DAG. Fleet cascade-resolves the prior child closure, submits a terminal-dependent fenced reset Job that preserves the slot-root identity while replacing contents, and gates reuse on reset success plus the same fence. Uncertain Containment retains the Lease/fence after Job finalization. |
| C-LINUX-1..5 | R-LINUX-1..5, R-ENV-5, A-20 | Accepted for product v0.2 with native cgroup-v2/pidfd Containment and no weaker process-group tier. Native Linux and WSL2 evidence are separate; WSL2 session survival requires external keepalive reported by doctor. Linux executable replacement follows `execve` semantics and records the actual launched target identity/hash. |
| C-OBS-1..2 | R-OBS-4, R-ENV-1, R-JOB-2, R-JOB-7, R-RUN-2 | Accepted. Provenance includes the effective non-secret environment and actual per-Invocation executable identity/hash. A same-path ordinary-file self-update while queued or between Attempts is permitted; disappearance or an unsafe Windows type/reparse transition fails before release. |

## Deliberate simplifications from the archived design

- SQLite replaces custom entity journals, AdmissionSlot generations, and most recovery protocol.
- Submission replaces client-owned submission progress and is resumed by the daemon after crash.
- Containment owns cleanup uncertainty; Quarantine is no longer a separate lifecycle entity.
- Managed waits use a conservative descendant/ancestor rule instead of a durable general wait graph.
- Crash consistency is required; certified physical-power-loss persistence is outside v0.1.
- Fleet continues to own multi-host placement and policy. SSH remains an external transport.
