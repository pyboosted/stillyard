# R-COND-1..5 external readiness and probes

Status: normative for Stillyard `0.1.0-alpha.14`, JobSpec schema 4, local protocol 19, and the
`stillyard-conditions-r1-2026-09-01` greenfield store epoch.

## Condition set and public schema

`JobSpec.conditions` is an ordered AND-set of at most 32 immutable Conditions. Omission means an
empty, already-ready set. Every Condition has a stable `ConditionId`, its original zero-based
index, one predicate, an explicit deadline, and a deadline outcome. It participates in the
normalized payload and idempotency hash. A Batch member owns its own independent set.

Schema 4 supports these predicates:

- `path_exists { path }` and `path_absent { path }` are level predicates over a bounded absolute
  path;
- `path_transition { path, from, to }` requires distinct `absent`/`present` states and is armed by
  an authoritative acceptance-or-later observation of `from`;
- `not_before { unix_millis }` becomes true at that absolute wall-clock instant;
- `probe { probe }` runs a non-interactive executable with explicit arguments, working directory,
  environment, resource claims, timeout, retry interval, and accepted exit codes.

A transition cannot be satisfied merely because the target state already existed at acceptance.
After the source has been observed and a later authoritative rescan sees the target, the transition
is durably satisfied and remains latched. A caller that also requires the target level to hold at
launch includes a separate `path_exists` or `path_absent` Condition in the AND-set.

Deadlines are exactly one of `none`, `relative { seconds }`, or `absolute { unix_millis }`.
Relative deadlines are in `1..=31_536_000` seconds and are resolved to an absolute timestamp in the
same acceptance transaction that creates the Job. They are never recomputed by retry or restart.
The configured outcome is `failed` by default or `canceled` explicitly.

## Observations and authoritative re-evaluation

Every authoritative evaluation appends an immutable Observation containing its value, wall and
monotonic timestamps, boot identity when available, daemon generation, freshness deadline, and
source. Public Receipt, Status, and List projections expose each Condition and its latest
Observation. Observation sources are `filesystem_rescan`, `clock`, `probe`, and `invalidation`.

Filesystem notifications, when available, are wake-up hints only and never evidence. The Windows
alpha.14 authority is the rescan itself; correctness does not depend on retaining a watcher or on
receiving every notification. A missing/lost/overflowed watcher therefore falls back to the same
bounded freshness path. The daemon rescans after generation change or freshness expiry and always
rescans synchronously at both Lease preparation and the final release barrier. The host setting
`observation.condition_rescan_interval_millis` bounds normal path/clock evidence age to
`100..=60_000` milliseconds and defaults to 1,000. Restart necessarily changes daemon generation;
detected long cadence gaps/resume advance the scheduler and expired wall evidence is rescanned.

Filesystem inspection errors are explicit invalidation observations and leave the Condition
waiting. They are never converted into the requested level. Clock and arithmetic conversions are
saturating and bounded.

## Scheduling and final release barrier

Dependencies and Conditions are checked before a work Lease. A blocked Condition releases any
scalar reservation owned by the Job and does not cause global head-of-line blocking. A pending Job
with only `not_before` blockers reports that time as an ETA lower bound. Path and probe completion
are not predictable and report unknown rather than a fabricated ETA.

After all Conditions and ordinary admission checks pass, the scheduler grants the complete work
Lease and creates the primary process born-contained and suspended. Immediately before
`ResumeThread`, one transaction force-rescans every non-probe Condition, verifies latched probe
results, dependencies, claims, fences, impacts, and cancellation, then records the primary start.
The primary runtime timeout starts at that authorized release, not at process creation.

If readiness changes before release, Stillyard proves the suspended Containment empty, releases the
work Lease, and returns the same Attempt from `starting` to `planned` after
`pre_release_backoff_millis`. The durable admission row retains the deferral count. The configured
`pre_release_max_deferrals` (1..=64) and admission wall deadline are finite; exhaustion settles the
Attempt as `safety_failed/readiness_unstable`. Ordinary retry policy may then create a later
Attempt, but it does not change Job acceptance time or Condition deadlines.

Once the primary is released, that Attempt no longer depends on its pre-start Conditions. Later
path or clock changes do not revoke its non-preemptive Lease.

## Probe execution and leases

A probe is a first-class `InvocationRole::Probe` in the owning planned Attempt. It uses the same
Windows born-contained process path and `CREATE_NO_WINDOW` behavior as other non-interactive
Invocations. At most one unresolved probe Invocation exists for one Condition.

Probe timeout is in `1..=3_600` seconds, retry interval in `1..=86_400` seconds, and accepted exit
codes are a non-empty unique bounded set. A probe receives one atomic Lease for its own complete
claim vector. Its RAM/GPU claims also require fresh host evidence. The Lease names the probe
Invocation, is released only after empty-Containment proof, and is gone before the primary requests
its work Lease. It never borrows or retains the primary claim vector.

Accepted exit status satisfies the Condition. Rejected exit, start failure, or timeout records a
public diagnostic Observation and schedules the next probe after the declared interval while the
Job remains pending. Stdout/stderr tails, executable hash, exit classification, Invocation ID, and
Containment are exposed through the ordinary Attempt snapshot. Cleanup uncertainty retains only
the probe Lease, blocks that Condition, and is resolved through the ordinary containment incident
surface before another probe can run.

Managed-child authorization applies the complete effective ancestor envelope to probe claims,
impacts, and fences as well as primary claims. A violation is the same durable machine-readable
managed-policy rejection used for other child capabilities and creates no Job; Batch rejection
remains atomic.

## Deadlines, cancellation, retry, and restart

While no primary has been released, the earliest expired Condition deadline wins deterministically
by absolute deadline then Condition index. Stillyard removes scalar reservation protection,
settles any planned/admitting Attempt, makes the Job terminal with reason
`condition_deadline_expired`, and selects the declared failed or canceled outcome regardless of
remaining retry budget. An unresolved probe is canceled/cleaned under the same Containment rules.

Explicit cancellation, terminal dependency outcome, or terminal Job transition prevents new
probes and releases every safe probe or work Lease. Retry resets level/probe readiness for the new
Attempt and schedules probes no earlier than retry backoff; acceptance-anchored transition history
and absolute deadlines remain durable. Daemon restart never extends a Condition deadline, resets a
deferral count, or turns uncertain cleanup into an empty proof.
