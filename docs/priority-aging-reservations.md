# R-RES-6 priority, aging, and scalar reservations

Status: normative for Stillyard `0.1.0-alpha.13`, JobSpec schema 3, local protocol 18, and the
`stillyard-priority-reservations-r1-2026-09-01` greenfield store epoch.

## Priority and scheduler order

`JobSpec.priority` is an immutable signed integer in the inclusive range `-3..=3`. Larger means
more urgent and omission means neutral `0`. Each Batch member owns its own value. Priority is part
of the normalized JobSpec and therefore its idempotency hash.

For a Pending Job, at wall-clock instant `now_unix_ms`, Stillyard computes:

```text
wait_ms = max(0, now_unix_ms - accepted_unix_ms)
aging_quanta = floor(wait_ms / 60_000)
effective_priority = min(1_000_000, priority + aging_quanta)
```

Subtraction, conversion, addition, and the final bound are saturating. The sort key is descending
`effective_priority`, ascending original `accepted_unix_ms`, then ascending durable Job row order.
The last component makes simultaneous Batch acceptance deterministic. Retry changes neither
acceptance time nor row order. Both values are durable, so restart does not reset aging.

A low `-3` Job ties a fresh high `+3` after six complete quanta and wins that tie by earlier
acceptance; after seven it has strictly greater effective priority. Thus a stream of newly accepted
high-priority work cannot starve an older low-priority Job. Reading status/list recomputes effective
priority and rank without a durable write or periodic event.

Priority selects only the next admission. It never revokes, shrinks, transfers, or partially lends
an existing Lease. The scheduler scans the entire ordered Pending set on every pass. Dependency,
Condition, retry/backoff, fence, impact, quiet/evidence, capacity, or any other blocker skips only
that Job; it does not stop the pass.

## Managed children

`ChildSubmissionPolicy.min_priority..=max_priority` is an inclusive range, itself bounded to
`-3..=3`. Its default is `0..=0`, the neutral-only policy used by Moot. Children inherit no parent
priority; omission remains `0`.

An out-of-range managed request is durably rejected as `child_priority_not_permitted`. Detail names
the requested value, allowed inclusive range, and authenticated policy ancestor. It is not a
`resource_capacity` blocker and creates no Job. Exact recovery returns the same code and detail.
Batch policy evaluation occurs before member insertion; one forbidden member rejects the complete
Batch atomically. Delegated policies may narrow but never widen the effective ancestor range.

## Reservation eligibility and identity

A scalar reservation is considered only after dependencies and Conditions allow admission, retry
and reservation backoffs have elapsed, fences and impacts are compatible, and required observed or
quiet evidence is currently sufficient. Every positive scalar claim must be at most configured
capacity. The Job must be blocked exclusively by already granted scalars or other active scalar
reservations.

The reservation contains the complete scalar claim vector—CPU, RAM, Cargo, GPU, and every custom
scalar—or no reservation is inserted. It contains no fence, impact, dependency, Condition, or quiet
state. It has a durable `ReservationId`, immutable creation time, and an absolute hold deadline
exactly 60,000 ms later. Candidates are considered in scheduler order. For every scalar:

```text
sum(active_reservation_claims[r]) <= configured_capacity[r]
```

Reservations may overlap granted Leases, so `granted + reserved <= capacity` is deliberately not an
invariant. A claim above full configured capacity remains visibly `resource_capacity`, never owns a
reservation, and does not block later candidates.

## Admission and atomic conversion

An unreserved Job with a positive claim may use only:

```text
unreserved_headroom[r] = max(0, capacity[r] - granted[r] - active_reservations[r])
claim[r] <= unreserved_headroom[r]
```

Scalars whose claim is zero are not tested, so a GPU reservation does not block CPU-only work. The
whole vector must pass; no partial Reservation or Lease is possible.

For a reservation owner, conversion order is current effective priority then original acceptance
order. A higher-ranked overlapping reservation is protected first, but a reservation that still
does not fit cannot block conversion on independent scalars. Conversion uses one SQLite immediate
transaction: recheck every non-scalar gate, verify the complete vector against current grants,
delete the reservation, insert the complete Lease, and perform the normal admission transition.
No reader can observe a deletion-to-grant gap. Accepted managed policy and its admission evidence
are immutable; there is no mutable policy state that can widen between acceptance and conversion.

If a dependency/Condition check, fence, impact, or quiet/observation gate no longer passes, the same
scheduler transaction deletes the reservation and leaves the Job Pending on that blocker.
Cancellation, an impossible dependency, or any terminal transition also deletes it immediately.
An uncertain Containment remains an ordinary granted Lease and is never preempted by reservations.

## Finite hold and deterministic yield

At the absolute hold deadline, Stillyard atomically deletes the reservation and writes
`reservation_not_before = expiry_processing_time + 5,000 ms`. During this durable backoff the owner
may neither obtain a Lease nor create another reservation. It therefore yields a deterministic
opportunity to compatible competitors even if it still has the highest effective priority. The
deadline and backoff are absolute durable instants; restart cannot extend them. The daemon schedules
a wake for retry deadlines, reservation deadlines, and reservation backoffs.

## Public observation and ETA

Receipt, Status, and List expose separate values for `priority`, `effective_priority`, `queue_rank`,
`accepted_unix_millis`, and nullable `reservation { reservation_id, claims,
created_unix_millis, hold_deadline_unix_millis }`. Pending Jobs have effective priority/rank;
non-Pending Jobs retain explicit priority and acceptance time but publish no current queue values.
An exact idempotent replay returns the current receipt projection, including any reservation.

`daemon-status.resources[*].reserved` is the checked sum of those active public reservation claims.
Insert, release, expiry, cancellation cleanup, and Lease conversion emit `job_changed` through the
existing durable lifecycle event surface. Aging alone emits nothing.

ETA uses running work and the current effective-priority order. An overlapping active reservation
makes a precise start time unsafe because it may convert or expire while aging changes the order;
the result is explicitly `unknown`. Other estimates state that priority order is a current snapshot
and that aging/new reservations may reorder it.

## Compatibility

The JobSpec wire shape is schema version 3, the strict local request/response protocol is 18, the
ManagedExecution schema fixture is version 2, and the package is `0.1.0-alpha.13`. The SQLite epoch
is intentionally replaced rather than migrated under Stillyard's pre-stable whole-store reset rule.
Public Job/Submission/Lease meanings and containment safety guarantees are unchanged.
