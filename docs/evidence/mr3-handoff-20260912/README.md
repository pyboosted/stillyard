# Non-disruptive daily-use handoff, 2026-09-12

This evidence supports integrating the installed Windows/attached-WSL alpha.20
slice into main. It does not close full MR-3 lifecycle acceptance or certify
standalone Linux. No WSL terminate/shutdown or host restart was performed.

source-continuity.json verifies all 127 tracked runtime/build inputs (src, tests,
Cargo/build inputs and the embedded bootstrap supervisor) against accepted z11f.
Both installed executable hashes match that build. Documentation, observer and
new example changes are outside that binary identity. No new Cargo invocation
or build cache was required for this handoff.

## Scheduled observations

jobs.json contains full Job IDs, outcomes and later canonical Grant states.
Every recorded Grant is released in those later snapshots; immediate ensure
results can still show armed during asynchronous release.

- Final documented Python hello and same-key replay:
  L`01a0960a-9c3f-7593-8f8a-113103d3ba56`, succeeded, same Job and Attempt,
  one Attempt, contained and released. Exact prepared helper is retained here.
- Native installed smoke:
  W`01a09609-aa84-74c3-a982-9c8ed06a3682`, succeeded and released.
- Coreutils control:
  L`01a09607-b0fc-7672-b1d6-d30ad8d2dd17`, succeeded in demonstrating ordinary
  path execution succeeds and fd execution with the same argv[0] fails.
- The initial direct printf hello failed as
  L`01a09605-46e9-7fe2-bd79-03e10601ecf6`; retained alongside the passing Python
  cases. No runtime fallback or transparent coreutils support is claimed.

W uses Store `01a05f1f-858c-7880-8c15-d55875da9e6b~`, L uses
`01a089b1-9a6a-7711-a600-39e2b74e495d~`. These smoke Jobs supplement historical
build/consumer gates; they do not replace them or claim new concurrency coverage.

## Independent review and attribution

Two Opus review Jobs (review-evidence and review-operations) failed at the
subscription login precondition before a model was invoked. Their exact briefs,
runner, receipts and canonical errors are retained. A separate read-only auth
query confirmed loggedIn=false, authMethod=none, apiProvider=firstParty. No API
credential or alternate billed provider was used and no Opus verdict exists.

The applied opus-review skill allows a Codex fallback and Codex subagents. One
independent Codex reviewer (`/root/handoff_review`, separate from the root
implementer) read the handoff docs, helper, actual runner/cleanup/client recovery
code and fresh evidence. This was a platform review agent, not a local CLI Job;
no synthetic Stillyard Job ID is assigned to it. The root Codex pass verified
source/images, ran the scheduled controls and resolved findings.

Initial findings:

1. P2: the example fsynced file contents but not directory entries. Applied
   fsync to the completed directory and then its parent before reporting ready.
2. P2: a draft incorrectly described successful subscription CLI exit reviews.
   Corrected attribution to failed Opus authentication and actual Codex review.

The reviewer re-read both corrections and returned:

> Approve merging the bounded Windows/attached-WSL daily-use slice. No remaining
> blocker identified in this review scope. Full MR-3 lifecycle acceptance
> remains open; this verdict does not certify standalone Linux or deferred
> recovery cases.

The coreutils compatibility limitation and contained interpreter workaround were
reviewed as accurately documented. Missing exact boot/cgroup cleanup proofs
continue to retain Grants; the documentation does not promise unattended recovery
after unsealed VM/distro interruption. This review is a scoped handoff review,
not a claim to have independently re-audited the entire repository.

## Completed cache cleanup

cache-cleanup.json records removal of the two completed z11f target directories
after fully paging both Job histories, checking empty queues and inspecting
process references. Reclaimed allocated size: 38,257,094,656 bytes (35.63 GiB).
The installed images and their rollback copies, source and canonical evidence
remain. Future lifecycle/build work must prepare its own current validation
cache; the prohibited historical interruption specs do not reserve these caches.
