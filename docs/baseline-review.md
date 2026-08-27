# Stillyard v0.12 baseline review

Status: frozen for implementation on 2026-08-27.

The simplified v0.12 requirements were independently reviewed through two substantive Claude Opus lenses, Claude Fable xhigh, and Grok 4.6 high. All reviewers classified the remaining work as local fixes rather than an architecture stop. A narrow amended-snapshot confirmation returned `PASS` from Fable and Grok with no high-severity regression.

The review closed these implementation-blocking seams:

- complete dependency-closure and Lease-component checking for managed waits;
- parent liveness revalidation when accepting a managed child;
- closed Attempt, Invocation, Containment, and Condition transitions;
- explicit Windows and Linux creating/no-root containment proofs;
- distinct plain-cancel and cascade semantics;
- total Attempt-to-Job outcome and set aggregate mappings;
- acceptance-anchored Condition deadlines;
- bounded drain progress for already-accepted managed-wait work;
- same-Attempt `not_received` replay with pinned idempotency history;
- stable slot-root identity across fenced reset content replacement;
- same-path agent executable updates with actual-image provenance.

The review deliberately retained these boundaries:

- nine durable domain entities and one SQLite lifecycle store;
- no durable general wait graph;
- no automatic replay after `unknown` or evicted history;
- uncertain Containment retains its Lease rather than globally delaying every dependency;
- host choice, SSH transport, worktrees, prompts, and verdict synthesis remain external orchestration concerns;
- Windows is v0.1; Linux is v0.2.

Future requirement changes require an explicit amendment with an acceptance scenario or mutant. Implementation discoveries should first be resolved inside the frozen public contract; architecture changes reopen the baseline deliberately rather than through incidental code drift.
