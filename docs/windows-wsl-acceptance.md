# Windows/WSL alpha.20 acceptance and daily-use handoff

Prepared 2026-09-12 for merging the `wsl` branch into `main`. The user requested
finishing the non-disruptive work and using the result while retaining the ban
on stopping Ubuntu-SSD or the shared WSL VM. This handoff covers the installed
Windows/attached-WSL daily-use slice. It does **not** close the full original
MR-3 lifecycle matrix. Independent exit review and fresh smoke results are
recorded below.

## Delivered scope

One installed Windows coordinator admits native and attached WSL Jobs against
the same machine resources. The Linux manager executes real Linux processes
under cgroup v2 containment, persists its Jobs and executor journal, and obtains
Windows Grants and per-Invocation Tickets. Consumers use ordinary executables,
explicit environments, durable receipts and the managed child API.

This is the reference workstation's paired WSL2 installation, not a universal
Linux installer or standalone Linux support. Native Linux and container runtime
acceptance remain MR-4; macOS remains MR-5. No published binary release or registry
package is implied by merging source into main.

## Source, installed images and gates

Accepted source file-map:
`9dd388044392f8b7bd991321f5dd80857e449667dc4abd12aabb0d56b3466af0`.
Both installed binaries report alpha.20 / IPC 25. The handoff compares current
runtime/build inputs with this map; subsequent documentation, idle observer and
evidence changes do not claim a new binary build.

| Side | Installed executable | SHA-256 |
|---|---|---|
| Windows | `C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe` | `8c5a6ec722a9f89942587f9c51f02487f7cc7683ead67bfe535e4de5a65ec791` |
| WSL | `/home/pythonic/.local/share/stillyard/bin/stillyard` | `95dc85a28c57c084f2e15210d08162d95642117da5c95b300e4b2cbb0593527b` |

All 11 matched gates passed as default Stillyard Jobs: Windows and Linux check,
Clippy, full tests, Rust 1.85 tests and release builds, plus native formatting.
[Exact gate Jobs](evidence/mr3-membership-20260910z11f/jobs.json),
[source manifest](evidence/mr3-membership-20260910z11f/source.json), and
[installed pair upgrade](evidence/mr3-installed-20260910z11f/).
Release Jobs are W`01a08b5f-48a9-7630-86f4-6e641ab724af` and
L`01a08b60-39c4-7043-b3e6-6e8cb8221d80`.

The W prefix abbreviates Store `01a05f1f-858c-7880-8c15-d55875da9e6b~`;
L abbreviates `01a089b1-9a6a-7711-a600-39e2b74e495d~`. Receipts retain full IDs.

## Live consumers and fault coverage

Three complete W-C1..4 rounds passed on installed z3 with machine Cargo capacity
2, 2 and 1. They include native/WSL build overlap and serialization, a real CLI
review with lost-client recovery, an agent requesting managed Cargo work and
recovering the same child, and an actual measurement with cross-OS quiet/impact
exclusion. A daemon restart separates rounds.
[Three rounds](evidence/mr3-consumer-rounds-20260910z3/rounds.json).

The final installed z11f source passed a further W-C1..4 confirmation: six Jobs,
including both full test suites, real review and measurement, and the managed
parent/child. The two agent adapter calls sequentially recovered one child,
Attempt, stable key and receipt; the authenticated postcondition passed.
[Final-source confirmation Jobs](evidence/mr3-consumer-confirmation-20260910z11f/jobs.json).
These are distinct evidence generations; the three earlier rounds are not
relabeled as three rounds of z11f.

| Matrix | Recorded outcome |
|---|---|
| M-A01..03 | Pass: shared single token, actual compatible overlap, priority/aging and no domain head-of-line blocking |
| M-A04 | Pass: composed real kernel/SQL and native coordinator crash/ack controls |
| M-A05 | Pass: live bridge loss retains the Grant, then sealed release reconciles |
| M-A06 | Pass: isolated cross-OS Store/journal reset does not free live work |
| M-A07 | Pass: stale writer fenced; new incarnation reconciliation and duplicate Ticket rejection |
| M-A08..10 | Pass: descendants/cancel/cleanup failure, postconditions/probes, managed child replay |
| M-A11 | Partial: cross-OS quiet/impacts pass; actual suspend/resume deferred |
| M-A12 | Partial: daemon restart/crash pass; distro terminate, whole-VM shutdown, logout and host reboot unaccepted |

The [ledger matrix](machine-resource-implementation-status.md#negative-control-and-consumer-matrix)
retains each row's exact historical source and the corresponding detailed
evidence. Component models, kernel controls and live-consumer runs are separate.

Installed five-minute idle acceptance passed all 21 conditions, including bridge
and helper coverage: 0.0550% of one logical CPU, 67.1406 MiB endpoint memory and
3.2000 timer expirations/min. Limits are 1.1%, 96 MiB and 6/min respectively.
Memory is endpoint private bytes/RssAnon, not interval peak usage.
[Raw interval, validator and source-audit dependencies](evidence/mr3-idle-20260910z11f3/).

## Operational limits retained at handoff

- Keep the installed Windows coordinator and its external WSL keepalive running.
  Linger alone is insufficient. Automatic cold start and logout persistence are
  not accepted; the S4U task was not installed.
- Bridge loss blocks new attached starts and retains Grants for uncertain work.
  A new boot or an absent cgroup is not implemented as a cleanup seal. A distro
  or VM stop during an unsealed Invocation can retain its Grant after restart;
  unattended recovery from that case is not promised. Preserve Store, anchor
  and journal; do not force-release resources or re-pair to bypass the incident.
- WSL GPU observation is unsupported. CPU/memory/disk/process observation and
  quiet/impact admission are supported in the accepted paired configuration.
  Admission reservations and cgroup/VM hard limits are separate quantities.
- Linux durable Stores and client receipts require local ext4. Use an ext4
  evidence/receipt directory even when input files live on `/mnt/c`.
- The fresh hello check found that this Ubuntu 26.04 multicall coreutils printf
  fails when executed by fd. A real Job reproduced normal path success and fd
  execution failure with the same argv[0]. The hello example now uses Python;
  the operator guide provides an explicit contained wrapper for affected tools.
  Transparent multicall execution is a known compatibility follow-up, not a
  tested capability. No unsafe path-based fallback was added to the runner.
- The disposable Stillyard-MR3-Test import and its targeted cleanup timed out on
  2026-09-10. WSL management list calls also stalled; this was not a successful
  lifecycle test. Its partial registered VHD is retained pending reconciliation
  through WSL, not manually deleted. Existing Ubuntu-22.04 was left untouched.
  [Failed fixture and working-pair continuity](evidence/mr3-test-distro-20260910a/).
  Do not infer the current state of that service-side operation from an old
  client timeout. This merge does not require resuming it.

## Review and merge record

Two scheduled Opus review attempts failed their subscription-authentication
precondition before model launch (`loggedIn=false`). No Opus verdict is claimed.
One independent Codex reviewer then inspected evidence, actual cleanup/launch and
client recovery code, the operator guide and the new example. The root Codex
pass checked source/image identity and ran fresh scheduled smoke/control Jobs.
The independent reviewer recommended merging this limited slice after adding
directory fsync to the example and correcting review attribution; both changes
were applied. Review scope does not claim a new whole-repository code audit.
[Review disposition and current evidence](evidence/mr3-handoff-20260912/).

Final helper smoke/replay L`01a0960a-9c3f-7593-8f8a-113103d3ba56` and native
smoke W`01a09609-aa84-74c3-a982-9c8ed06a3682` passed. The later canonical
snapshots confirm released Grants; replay retained one Job and Attempt.
The fd/path coreutils control L`01a09607-b0fc-7672-b1d6-d30ad8d2dd17` passed
in reproducing the documented incompatibility. The failed initial printf Job
and failed Opus preconditions remain alongside these successes.

No unrun lifecycle case becomes PASS through this review. The full MR-3 phase
remains open while this bounded slice is handed off for daily use.

Follow [Windows/WSL operation](windows-wsl-operation.md) for submitting a Linux
Job, scheduled development gates, managed consumers and incident diagnosis.
