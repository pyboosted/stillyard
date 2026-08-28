# Phase 7 — Live observation

Status: planned alpha.7 bounded contract (2026-08-28)

This increment makes the already durable scheduler observable as a live system. It adds one
event-driven public observation path and the first useful terminal monitor without changing
alpha.6 admission, retry, cancellation, dependency, or process-containment semantics. The public
crate remains the only interface used by both CLI and TUI; neither may open SQLite or canonical log
files directly.

## Outcome

After this increment an owner can start `stillyard watch`, see queued, blocked, running, retrying,
and recently final Jobs move without polling, open one Job, and follow its committed stdout/stderr.
An application can obtain the same information through runtime-neutral blocking Rust APIs, detach
without affecting work, and reconnect from a durable event cursor. The first external consumer,
`moot`, can replace status polling with a wait stream.

## Public observation model

- `EventCursor` contains the store UUID and a monotonically increasing sequence. A cursor from a
  replaced store is never accepted as a position in the new store.
- `SchedulerEvent` is a bounded invalidation/provenance record, not a duplicate `JobSnapshot`. It
  identifies the affected Job and optional Batch, records the public change kind and commit time,
  and carries the cursor assigned in the same SQLite transaction as the authoritative transition.
- `JobSummary` is the bounded row returned by list and resynchronization: identity and parent/Batch,
  state/outcome, accepted/started/finished times, queue rank, estimate, claims, first blocker, current
  Attempt/Invocation, and committed stdout/stderr extents. Full specs and lifecycle history remain in
  `JobSnapshot` and are fetched only for the selected detail view.
- Public change kinds cover submission acceptance, lifecycle/Attempt/Invocation/Containment
  transition, cancellation request, scheduling eligibility change, and committed log extent.
  Consumers must treat unknown future kinds as a reason to refresh, not as an error.
- One event may invalidate queue rank, blockers, or estimates for Jobs other than its subject.
  A watcher therefore refreshes its bounded selected view after an event batch; the event stream is
  not a materialized copy of every derived snapshot field.
- `ObservationFrame` is either `Events`, or `Gap` followed by a bounded current snapshot page and a
  new cursor. A caller never infers that no transition occurred across a store reset or expired
  event range.
- `JobSelector` supports all retained Jobs, an explicit nonempty set of Job IDs, one retained Batch,
  or an AND-set of exact labels. Explicit identities are exact. Label/all selection is a dynamic
  view over retained Jobs and makes no claim about already removed history.

The public crate adds bounded blocking operations for:

- listing a selector with an opaque pagination cursor and caller-selected limit;
- reading an event page after a cursor;
- waiting for the next observation frame with deadline and optional `CancellationToken`;
- streaming settlements for a bounded selected set through `WaitStream`; and
- following canonical log chunks from explicit stdout/stderr byte offsets.

`WaitStream` snapshots its selected membership once with a 1,024-Job alpha.7 bound, emits each member
at most once when it is observed final, and then emits the existing worst-outcome aggregate ordered
`succeeded < skipped < canceled < interrupted < timed_out < failed`. An `any` option stops after the
first settlement without changing the other Jobs. A reconnect may replay source events, but
stream-level settlement deduplication is by Job identity; it is not a durable Wait entity and
promises no exactly-once side effect outside the caller. List and watch remain paged and do not use
the WaitStream membership bound as a retention limit.

Dropping any iterator or blocked call only disconnects that client operation. It never cancels or
otherwise changes a Job. Public stream objects own no async runtime and do not spawn a helper
process. They may perform repeated deadline-bounded local protocol calls internally.

## Durable events and wakeup discipline

- The current greenfield schema gains an `events` table and a durable next-sequence value. There is
  no schema migration: an older schema epoch is replaced by the existing whole-store reset rule.
- An externally visible authoritative transition and its event commit atomically. A crash may leave
  the prior transition and no event, or the new transition and its event; it may not expose the new
  transition without the corresponding cursor advance.
- Event history has a fixed alpha.7 row bound of 16,384. Pruning advances the durable oldest
  available sequence. General configurable Job/log/input retention remains later work.
- A request before the oldest available sequence returns `Gap`; a future sequence, wrong store UUID,
  or malformed cursor rejects explicitly. The current head cursor is returned with every page and
  snapshot resynchronization.
- Daemon-local condition variables remain wakeup hints only. Every wake, timeout, reconnect, and
  daemon restart rechecks durable sequence/state. Lost or spurious notifications cannot lose a
  transition or fabricate one.
- Event reads, long-polls, and TUI refreshes have bounded page sizes and memory. Slow consumers are
  allowed to fall behind and receive `Gap`; writers never wait for a viewer.
- Log-extent events are coalescible invalidations. Canonical committed files and explicit offsets
  remain the source of log bytes; an event row never contains arbitrary child output.

## CLI and first TUI

Alpha.7 adds the read-only commands:

```text
stillyard list [--label KEY=VALUE...] [--limit N] [--cursor CURSOR] [--json]
stillyard events [--since CURSOR] [--label KEY=VALUE...] --json
stillyard logs JOB [--stdout | --stderr] [--follow] [--since OFFSET]
stillyard watch [--job JOB | --batch ID | --label KEY=VALUE...]
```

`watch` is a disposable `ratatui`/`crossterm` client of the public crate. Its bounded first view has:

- a queue table with state/outcome, queue rank, honest estimate, elapsed time, declared claims, and
  the first blocker;
- a Job detail view with parent/Batch identity and ordered Attempts, Invocations, exits, containment
  state, and incidents already present in the public snapshot;
- independent stdout and stderr panes following committed offsets, with arbitrary bytes rendered
  lossily for the terminal but never rewritten in canonical storage;
- visible reconnecting, stale, and `Gap` states; and
- keyboard navigation and quit/detach that never mutate scheduled work.

The TUI keeps only the selected bounded snapshot page, bounded diagnostic history, and bounded log
windows. It performs no fixed-rate status polling while idle. Terminal redraw timers may update
display-only elapsed durations but may not issue daemon requests.

## Explicit non-goals

This increment does not add Conditions or probes, quiet/load/RAM/VRAM providers, priority/aging,
general configurable retention, secrets, artifacts, cascade cancellation, drain/force, distributed
placement, Linux containment, or a durable Wait entity/edge. It does not add `doctor --json`; the
existing daemon status already exposes active profile names, scalar capacities, generation, and
configuration hash, while the broader capability/containment doctor belongs with alpha.8.

Existing single-Job `wait`, status, submit, recover, cancel, and scheduling meanings remain
byte/behavior compatible except for the new event records they produce.

## Acceptance and adversarial evidence

Alpha.7 is complete only when all of the following hold through the shipped daemon, public crate,
CLI, and TUI path:

1. A queued Job progresses through start, output, retry/postcondition, and final settlement while a
   client observes monotonic events and snapshots; no status polling is used.
2. Disconnecting and reconnecting from the last cursor neither loses a committed transition nor
   duplicates a settlement in `WaitStream`. At-least-once event delivery across the external handle
   remains explicit.
3. More than 16,384 committed events force a visible `Gap`, deliver a bounded current snapshot, and
   resume from the returned cursor. Mutants that silently continue after pruning fail.
4. Store replacement rejects the old store cursor and resynchronizes visibly. A mutant that reuses
   a sequence under the new store UUID fails.
5. Crash injection around a lifecycle/event transaction exposes only the valid prior pair or new
   pair. A transition-without-event mutant fails.
6. A waiter starting at the commit/wait boundary cannot sleep past the event. Lost-notify,
   notify-before-wait, spurious-wake, daemon-restart, and client-deadline mutants pass without hot
   polling.
7. A slow or abandoned viewer cannot block submission, log draining, lifecycle settlement, or event
   pruning. Memory stays bounded under continuous output and event churn.
8. `logs --follow` and the TUI resume stdout/stderr from committed byte offsets across reconnect;
   binary/non-UTF-8 output remains byte-identical through the public log API.
9. CLI and TUI contain no SQLite/private-store access. A private-read mutant fails the dependency
   and source audit.
10. A five-minute idle daemon plus attached `watch` uses event waits, starts no helper process, and
    meets the existing A-19 wake/CPU/private-memory bounds, with the measurement method and host
    provenance recorded.
11. The `moot` adapter replaces its status-backoff workaround with the public wait stream and its
    workspace gate plus one live review Job complete without degraded behavior.

Focused external review is required for cursor/Gap semantics, transaction boundaries, bounded
memory, terminal detachment, and the consumer adapter before alpha.7 is declared delivered.
