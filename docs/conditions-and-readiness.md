# R-COND-1..5 external readiness and probes

Status: normative for Stillyard `0.1.0-alpha.14`, JobSpec schema 4, local protocol 19, and the
`stillyard-conditions-r3-2026-09-02` greenfield store epoch.

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
Within one daemon generation, freshness is decided by the monotonic observation deadline; reaching
that deadline makes the evidence stale. An already expired monotonic deadline is not published as a
zero-delay scheduler wake. If evidence expires during a long scheduler pass, the next rescan is
staggered by a deterministic 25-millisecond yield instead of spinning immediately.

Filesystem inspection errors are explicit invalidation observations and leave the Condition
waiting. They are never converted into the requested level. Clock and arithmetic conversions are
saturating and bounded. Filesystem provider work runs before SQLite write transactions through an
eight-worker bounded inspector. One Job or Batch evaluation has a two-second aggregate provider
deadline; timeout, queue saturation, or worker failure is an explicit invalidation rather than an
unbounded scheduler/store lock hold.

When Condition provider work runs beside host observation admission, the admission clock is
refreshed after that work before the sample is evaluated. In production this uses the live wall and
monotonic clocks; deterministic tests account for the measured provider duration. A HostSample that
became stale while paths were inspected therefore cannot authorize either a probe Lease or primary
release. The Condition-only release path performs the same complete host-policy recheck when the Job
also declares observed-resource thresholds, and its release evidence is persisted atomically.

## Scheduling and final release barrier

Dependencies and Conditions are checked before a work Lease. A blocked Condition releases any
scalar reservation owned by the Job and does not cause global head-of-line blocking. A pending Job
with only `not_before` blockers reports that time as an ETA lower bound. Path and probe completion
are not predictable and report unknown rather than a fabricated ETA.

After all Conditions and ordinary admission checks pass, the scheduler grants the complete work
Lease and creates the primary process born-contained and suspended. Immediately before
`ResumeThread`, one transaction force-rescans every non-probe Condition, verifies latched probe
results, dependencies, claims, fences, impacts, and cancellation, then records the primary start.
The primary runtime timeout starts at that authorized release, not at process creation. If the
freshness window expires after that transaction but before the suspended primary is actually
resumed, Stillyard follows the same never-run replan path below; it does not classify the Attempt as
a process start failure. The daemon serializes a final cancellation/deadline check and
`ResumeThread` under its store boundary, so a terminal intent committed first prevents user code;
the successful resume is the ordering point when release wins. A never-resumed Invocation leaves no
public Job, Attempt, or Invocation start timestamp.

If readiness changes before release, Stillyard proves the suspended Containment empty, releases the
work Lease, and returns the same Attempt from `starting` to `planned` after
`pre_release_backoff_millis`. The durable admission row retains the deferral count. The configured
`pre_release_max_deferrals` (1..=64) and admission wall deadline are finite; exhaustion settles the
Attempt as `safety_failed/readiness_unstable`. Ordinary retry policy may then create a later
Attempt, but it does not change Job acceptance time or Condition deadlines.

Once the primary is released, that Attempt no longer depends on its pre-start Conditions. Later
path or clock changes do not revoke its non-preemptive Lease. Conditions constrain only the primary
pre-start boundary; postcondition JobSpecs do not inherit them and are governed by their existing
lease, cancellation/timeout, and containment barriers.

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

Probe resolution is one crash-atomic transaction covering the resolved Invocation, empty
Containment proof, diagnostic Observation, Condition transition, Lease release, and any pending
deadline/cancellation settlement. Terminal settlement waits for every live probe owned by the Job,
not merely the Condition's current probe pointer. Startup repairs the legacy resolved+empty
split-commit shape idempotently before scheduling new work; because that legacy shape did not
durably retain the exit classification, repair fails closed and schedules a fresh probe.

Observation history retains the latest eight immutable records per Condition. Resolved probe
Invocation history retains at least the latest 64 runs and may retain additional rows while their
events remain in the bounded event ring; after those references age out, older empty/released probe
rows and their stdout/stderr logs are pruned by global scheduler/recovery maintenance. The latest
Observation and every live or uncertain boundary are never pruned. A log that Windows temporarily
refuses to delete retains a durable GC tombstone and is retried on later maintenance passes; missing
files count as completed cleanup. Retry batches are bounded and rotate failed tombstones so one
permanently locked file cannot starve later cleanup.

Managed-child authorization applies the complete effective ancestor envelope to probe claims,
impacts, and fences as well as primary claims. A violation is the same durable machine-readable
managed-policy rejection used for other child capabilities and creates no Job; Batch rejection
remains atomic.

## Deadlines, cancellation, retry, and restart

While no primary has been released, the earliest expired Condition deadline wins deterministically
by absolute deadline then Condition index. Stillyard removes scalar reservation protection and
selects the declared failed or canceled outcome regardless of remaining retry budget. With no live
probe it settles the planned/admitting Attempt and publishes Final immediately with reason
`condition_deadline_expired`. An unresolved probe first receives a durable stop intent; the Job
remains Pending until every probe boundary has empty proof or an honest uncertain transfer, then the
Attempt and Job become terminal in that same cleanup transaction. Startup performs the same terminal
sweep after containment recovery, so conversion of a live probe to uncertain cannot strand a
durable cancellation or deadline intent.

Explicit cancellation is durable while any boundary remains live, prevents both sibling and
reconciled probes, and releases every safe probe or work Lease. Terminal dependency outcome or
terminal Job transition applies the same no-new-probe rule. Retry resets level/probe readiness for
the new Attempt and schedules probes no earlier than retry backoff; acceptance-anchored transition
history and absolute deadlines remain durable. Daemon restart never extends a Condition deadline,
resets a deferral count, or turns uncertain cleanup into an empty proof. Once expiry has latched a
Condition as failed, wall-clock rollback cannot remove its stop or terminal intent.

Cancellation and the selected deadline outcome are durably latched before a suspended primary is
resumed. Startup applies that latch, and also deadlines that became due during downtime, before the
generic interrupted/start-failed recovery sweep. If cleanup of a never-resumed suspended primary
cannot prove the boundary empty, Stillyard preserves the winning cancellation or configured
deadline outcome, publishes no false start timestamp, marks containment uncertain, and retains the
Lease. A Condition-only primary that has crossed its durable release transaction is publicly
`released` while running and after settlement; absence of host-observation fields is not reported as
absence of release evidence. A cancellation or canceled deadline settled before that transaction
retains `reserved` as the last attained admission stage and never fabricates `released` or a final
sample.
