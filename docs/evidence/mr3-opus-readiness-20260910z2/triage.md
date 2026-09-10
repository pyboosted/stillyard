# z2 readiness review triage (not MR-3 exit acceptance)

Both reviewers ran as installed default WSL Jobs using subscription OAuth,
actual Opus model reported in their provenance/result, no mutation tools.
Ticket review: `01a089b1-9a6a-7711-a600-39e2b74e495d~01a089f1-8d6c-7032-aea1-9f5e273f25b2`.
Operations review: `01a089b1-9a6a-7711-a600-39e2b74e495d~01a089f1-90a5-72b2-b575-e723160e9110`.
Both Jobs succeeded; their typed verdict is findings, not acceptance pass.

## Verified disposition

- Ticket findings 0/3 (old request re-clocked after reconnect / authorization
  sent before reconciliation): proposed trace is contradicted by
  `src/machine/manager/recovery.rs::begin`: every unanswered outbox operation is
  archived in attached_abandoned and removed from outbox, session is updated.
  `recovery::allow` refuses new AuthorizeInvocation during inventory recovery.
  Driver::connect always calls begin before step. Existing manager recovery tests
  exercise lost Ticket reply, higher connection epoch, refusal of consumption and
  authorization while recovering. A same-Invocation retry after an explicit
  rejection still needs actual disconnect fault acceptance; no acceptance inferred.
- Ticket finding 1 (late Ticket loses cleanup) is contradicted by
  `src/store/attached/cleanup.rs::apply` and `attached.rs::accept`: durable executor
  seals are applied in the same transaction accepting any late Ticket, including
  missing Ticket obligations imported from inventory. No outbox deletion workaround
  is adopted because it would break continuous protocol sequence history.
- Ticket finding 2 is confirmed liveness risk: initial Opus starts expired the
  250-ms barrier. The two later starts passed. Bound remains unchanged; ordinary
  pre-release retry/deferral under load remains open.
- Ticket finding 4 requires producer-code coverage; not assumed fixed by guessed
  strings. Finding 5 is allocation lifecycle: probe grants belong to one Invocation,
  so a new probe needs a new Lease; this is not permission to reuse its old grant.
- Ticket finding 6 is conditional on missing installation validation. Store-level
  check_authority_release/installation validation fences a damaged pairing before
  execution; durable configuration predicate is additional defense to consider.
- Ticket findings 7/11 are coupling/invariant observations: actual acceptance uses
  UPDATE and one active Invocation per Work Lease. No observed failing trace yet.
- Ticket finding 8: lock order remains Store then barrier, matching release.
  Dropping Store before cancel would weaken ordering with concurrent starts;
  misleading disconnect comment should be corrected in the next Rust slice.
- Ticket finding 9 concerns diagnostics only: persisted machine snapshot is not
  coordinator admission authority. Actual launch still needs a current Ticket.
- Ticket finding 10 and operations journal retention finding are valid bounded
  capacity/retention concerns; cleanup failure must fence the in-memory release
  permission even when durable cleanup fails. Journal archival is still open.
- Operations interop validation finding is addressed in z3 source: canonical
  /run/WSL root-owned Unix socket required before bridge spawn. Live fault test
  remains pending.
- Operations OOM default claim does not match this installed delegated unit:
  `systemctl --user show stillyard.service -p OOMPolicy` reports `continue`.
  Explicit unit setting is still preferable; no OOM fault acceptance claimed.
- Operations child-cgroup creation concern is covered by the omitted launch
  namespace code: Invocation cgroup filesystem is read-only. Do not recursively
  remove unexpected unproven external boundaries merely to make cleanup pass.
- Operations systemd restart/bridge orphan, coldboot/keepalive, interrupted install,
  upgrade failure handling and boot-change cleanup are outstanding implementation
  or live acceptance work. Idle restart evidence is not active-restart evidence.
- SQL write barrier across upgrade stop is intentional: Linux daemon currently
  terminates by default SIGTERM (no graceful SQL shutdown handler). Releasing the
  barrier before stop, as suggested, would admit new work in the gap. Improve
  failure recovery and independent journal/cgroup checks while retaining fencing.
