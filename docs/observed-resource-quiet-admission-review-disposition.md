# Observed resource and quiet admission — review disposition

Date: 2026-08-28
Reviewed brief: `docs/observed-resource-quiet-admission.md` at commit `7f83f65`
Disposition: all findings accepted; revised brief requires closure review before implementation

## Review provenance

- Fable: `claude-fable-5`, xhigh, subscription OAuth through `claude-current`; launcher resolved to
  `C:\Users\User\.local\bin\claude2.cmd` (`.claude2` profile). Session
  `04d1c224-7236-4f68-b292-137cdbcb1e87`. Verdict artifact:
  `C:\Users\User\AppData\Local\Temp\fable-stillyard-observed-quiet-20260828.json`.
- Grok: exact `grok-4.6`, xAI OAuth subscription, no API-key billing. Verdict artifact:
  `C:\Users\User\AppData\Local\Temp\grok-stillyard-observed-quiet-20260828.json`.

The artifacts are local review evidence; the durable disposition and resulting contract live in
the repository.

## Fable findings

| Finding | Disposition in revised brief |
|---|---|
| Planned Attempt conflicts with current recovery, cancel, settlement, and new-attempt preparation | Accepted. Planned/admitting recovery is excluded from blanket interruption; pending cancel settles the attached Attempt; reservation adopts it rather than minting another. |
| Cancel can be lost while a contaminated suspended root is cleaned | Accepted. Replan checks cancel first and reservation refuses canceled work. |
| Final evidence can age while waiting for the Store mutex or between authorization and release | Accepted. Transactions revalidate typed capture operands at current time; the runner checks a bounded release token and clock continuity immediately before `ResumeThread`. |
| Quiet wait burns current Attempt runtime timeout | Accepted. Process start/deadline are nullable and begin only at release authorization. |
| Repeated primaries collide with postcondition `role_index` and the 256-Invocation bound | Accepted. Global monotonic `role_index`, explicit `postcondition_index`, and a combined acceptance bound are required. |
| Pre-release cancel/timeout can record a fake suspended-root exit | Accepted. Never-run cleanup has no root-exit classification and is separate from ordinary stopped-process settlement. |
| Lease release after replan lacks an Attempt-wide open-containment guard | Accepted. Replan proves no other open Containment before release. |
| Cleanup uncertainty is hard-coded to `interrupted` | Accepted. The uncertain settlement accepts explicit `safety_failed/pre_release_cleanup_uncertain` and retains the Lease. |
| Resource contention can be charged as `quiet_unattainable` | Accepted. Quiet budget runs only while all non-quiet gates pass and pauses during actual resource/impact contention. |

Fable's additional A-19 observation is also accepted: the sampler is demand-driven and parked at
idle rather than waking at 1 Hz forever.

## Grok findings

| # | Disposition in revised brief |
|---:|---|
| 1 | `AdmissionContext` is now a typed operand bundle with capture times, component status, observation generation, GPU identity, detector evidence, and transaction-time revalidation rules. |
| 2 | Preparation is explicitly split into create/adopt Attempt, admitting wait, and later atomic reservation. |
| 3 | Invocation ordering uses global `role_index`; postconditions use a separate spec index. |
| 4 | Runtime start/deadline begin at release authorization. |
| 5 | Recovery preserves planned/admitting Attempts and their consumed budgets. |
| 6 | `observation_generation` is explicitly distinct from daemon generation. |
| 7 | Validation lifts only quiet/observed rejects; Conditions/artifacts remain fail-closed. |
| 8 | Contamination uses a dedicated never-run cleanup/replan path, not `start_failed`. |
| 9 | Pending cancel settles the attached planned/admitting Attempt. |
| 10 | Observation arithmetic is checked; overflow/underflow is unusable evidence, never saturating capacity. |
| 11 | First CPU/disk delta in a generation is `warming_up`, never 0%. |
| 12 | All GPU claims, detectors, thresholds, VRAM debit identity, and provenance bind to the single configured GPU UUID. |
| 13 | `HostObservationConfig` is an actual `HostConfig`/schema field; unsafe old nonzero-capacity defaults reject. |
| 14 | Receipt/snapshot/doctor additions are named typed public fields, including Attempt reason and optional pre-release timestamps. |
| 15 | A-05 gains a shipped public-CLI negative control using an external NVML ABI fixture plus detached stale-check mutant. |
| 16 | Quiet wait occupies the required `admitting` Attempt state rather than overloading `planned`. |
| 17 | Admitting work owns no Lease or FIFO occupancy; only granted Leases block a sidecar, and quiet budget pauses after a race. |
| 18 | VRAM custom keys have one case-folded canonical NVML debit identity; aliases/duplicates reject. |
| 19 | Missing UUID/driver blocks even sidecar `gpu_slots`; provenance is not waived when NVML is disabled. |
| 20 | Quiet sample-gap and provider-generation cadence-gap thresholds are separately named and ordered. |

## Gate consequence

Implementation may start only after focused Fable and Grok closure reviews find no remaining
high-severity contract hole. All local build/test/format/Clippy evidence from this point is produced
by the checked-in Jobs on the system default Stillyard daemon, per `AGENTS.md`.

## Closure-round disposition

The first closure round used the same Fable session/model and a new exact `grok-4.6` pass. Fable
confirmed every original finding closed, then found two High ambiguities. Grok found one Critical,
three High, and two Medium; the overlapping release-timestamp finding was the same issue from both
reviewers. All are accepted:

- release ordering is restored to durable `started/running` before `ResumeThread`, under an
  observation-service barrier that takes the synchronous sample, revalidates exact live
  generation/clock/expiry, and is held through resume;
- release arithmetic excludes the evaluating Attempt's own Lease but no other granted debit;
- no replan occurs after release authorization, so authorization timestamps cannot leak into a
  later planned cycle;
- cleanup uncertainty is final and non-retryable even when ordinary `safety_failed` is retryable;
- bare `gpu_slots` is bound to exact configured/live NVML `gpu_slot_uuid`, not any GPU;
- quiet sample gaps use the named host threshold everywhere;
- deferral Invocations count only for Jobs that declare quiet;
- a separate non-pausing admitting wall-clock limit prevents resource traffic from parking a
  quiet Attempt forever without mislabeling it `quiet_unattainable`.

Fable's lower-severity closure notes are also incorporated: restart discards the open volatile
eligible interval; pending admission exhaustion has a named settlement path; the deferral config
bound is reduced; non-quiet observed Jobs have no second release check; and release expiry is the
minimum applicable evidence deadline.

Final closure verdicts:

- Fable `claude-fable-5`, same session/profile: **implementation-ready**, zero Critical/High.
- Grok exact `grok-4.6`: **pass**, empty findings array.

Fable's final non-blocking notes were incorporated without changing the reviewed safety shape:
barrier-before-Store lock order is explicit, the barrier is released before bounded cleanup,
cancel wording distinguishes pre/post authorization, and the `admission_starved` paused-clock
mutant is required. The brief is frozen for implementation.
