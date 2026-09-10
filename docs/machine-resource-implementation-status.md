# Machine resource scheduling — status/evidence ledger

Updated: 2026-09-10. Target delivery: installed Windows/WSL pair and complete MR-3
live-consumer acceptance. **That delivery has not been made.**

Recovery after the user-reported unexpected restart: working files and native
evidence survived. The first CLI reconnect returned auto-start OS error 5;
subsequent discovery found the installed alpha.14 daemon already running as PID
49088, generation `01a086be-d3c9-79f1-909a-22d6f2757351`, with the original store
UUID and no queued/running Jobs. No queue reset, process termination or binary
replacement was used. [Recovery observations](evidence/mr0-reboot-20260909/)
retain daemon/doctor and Linux boot identity. Windows' reported boot GUID is
unchanged; the restart report is not adopted as a blanket empty-process proof.
The observation is not controlled M-A12 acceptance.

Pre-interruption Clippy Job `01a085eb-fe51-7ca2-95ae-1e8509baaef4` and MSRV-check
Job `01a085eb-fe4a-7c21-b6b3-b9d279d51c68` both succeeded; canonical results were
retrieved again after reconnect. Both use store prefix
`01a05f1f-858c-7880-8c15-d55875da9e6b~` and validate the native admission interlock,
before the subsequent trusted-bootstrap changes.

## Phase status

| Phase | Status | Delivered / remaining |
|---|---|---|
| MR-0 | complete | Normative contract/amendment and failure traces; installed safe bootstrap; matched native/WSL check/build baseline; concrete consumer commands and validator controls. Linux compiler failures are the permitted portability baseline. |
| MR-1 | complete | Shared admission/lifecycle core installed in Windows alpha.16 / IPC 21; native gates and installed test Job passed, scoped accounting observed. |
| MR-2 | complete | Durable coordinator/manager protocol and public fault controls passed; final review disposition closed; Windows alpha.17 / IPC 22 installed and ordinary default Job passed. |
| MR-3 | in_progress | Windows/WSL z11f alpha.20 installed and paired; three W-C1..4 rounds passed on z3 with shared capacity 2/2/1. Cross-OS Store-reset, live capacity and incarnation controls passed on z8. M-A04/M-A07 passed. Final consumer confirmation, complete lifecycle coverage, aggregate idle timers and exit review remain pending. |
| MR-4 | not_started | Later native Linux/container delivery. |
| MR-5 | not_started | Later macOS delivery. |

## Current slice and source identity

Current slice: **MR-3 installed Windows/WSL acceptance**. Base commit
`5c440f6a2437510e56d3d728843562bae411303b`. Both installed binaries are
**alpha.20 / IPC 25**. Both installed sides now use z11f, retaining the release repair and adding
the membership/cleanup synchronization fix, exact unsealed negative controls
and the empty-provider model-clock correction, plus separate all-role transport
authentication with primary-only submission authority and finite native busy-pipe waits
(file-map `9dd388044392f8b7bd991321f5dd80857e449667dc4abd12aabb0d56b3466af0`).
Shared machine cargo_slots is restored to 1; both original retained Grants released.
The three accepted consumer rounds used z3 file-map
`005ed18d96eb8f5e14cf5b178308b9d7cbb8ec1eeeaef1f16d58b0ca5fffa10a`.
Historical x2 file-map
`4dbfc18c91bbede4df1f6714e442448797df7b064a0ee58a58159c49afba4c99`.
Windows: PID 60892, generation `01a08b65-9a4c-7111-ad59-a0825b917b7b`, unchanged
store `01a05f1f-858c-7880-8c15-d55875da9e6b`, SHA-256
`8c5a6ec722a9f89942587f9c51f02487f7cc7683ead67bfe535e4de5a65ec791`.
WSL: installed `/home/pythonic/.local/share/stillyard/bin/stillyard`,
store `01a089b1-9a6a-7711-a600-39e2b74e495d`, SHA-256
`95dc85a28c57c084f2e15210d08162d95642117da5c95b300e4b2cbb0593527b`.
Linux daemon PID 1929683, generation `01a08b65-a2dd-74b3-bbb1-4f45ebe170e4`. [Current installation evidence](evidence/mr3-installed-20260910z11f/).
Historical daemon identity after crash is recorded in
[daemon-crash evidence](evidence/mr3-daemon-crash-20260910z3/);
[upgrade barrier and retained identity](evidence/mr3-installed-20260910z3/).
The user service runs the installed binary in its delegated `manager` cgroup;
`executors` has memory.max=16 GiB and cpu/memory/pids controllers. Linger is enabled.
The Windows S4U keepalive task is **not registered**: ordinary registration was
access-denied (0x80070005), and the elevated UAC attempt was canceled by the user.
Current interop connection is proven, logout/cold-start lifetime is not.
Maintenance-helper corrections are now included in the accepted z8 source.
Native/WSL z8 gates and the explicit pair upgrade
passed, preserving both Stores and the executor journal.
[Historical z8 installation](evidence/mr3-installed-20260910z8/); historical
[first installed Job](evidence/mr3-installed-20260910x2/).

Accepted MR-2 h file-map:
`4d119e5cab94f2a9c675a3bc388df56e8e1f5e52952910b8393b24ac55b9b898`.
[Windows gates and protocol matrix](evidence/mr2-accepted-20260910h/),
[matched protected Linux check](evidence/mr2-linux-20260910h/),
[final review disposition](evidence/mr2-final-review-20260910f/codex-disposition.md),
[installation and installed Job](evidence/mr2-installed-20260910h/).

MR-2 includes restricted same-UUID SQL repair, retained native history, manager
reset/epoch recovery, audited retirement and bounded metadata reserves. New
submissions cannot starve a same-UUID repair; existing receipt recovery remains.
The full second-manager-with-Ticket repair/reconnect/sealed-release scenario
passed stable and Rust 1.85 tests. Legacy over-budget history is diagnosed; the
actual upgrade target was within budget. No unresolved safety review finding
blocks this installed MR-2 slice.

Installed postconditions/probes, three complete live consumer rounds, priority/
aging, bridge loss, active daemon crash and forced cleanup failure passed.
Cross-OS isolated Store-reset, live capacity and explicit incarnation controls
passed on z8. Remaining: complete VM/distro/logout/
reboot lifecycle evidence, aggregate timer wakes, final-source consumer
confirmation and independent exit review. z8 native/Linux Clippy, full tests,
MSRV tests and release builds passed and were installed. The actual attached
wait-path negative control passed; helper timer coverage remains open. The five-minute idle observation passed CPU/memory; aggregate timer coverage remains open.
Historical “current” notes below are superseded by this section.

Historical MR-0 baseline source identity follows.
The incoming Linux checkout had no tracked changes; the supplied plan was untracked
(SHA-256 `c11d86436ea5504449436a46af67426a386373107c755d3d8a4e17d649a83d5e`).

The Windows checkout at `C:\Development\stillyard` has the same commit and unrelated
changes to `AGENTS.md`, `scripts/run-stillyard-job.ps1`, `src/instance.rs`, plus
untracked `.agents/` and `reviews/`. None was overwritten. A separate snapshot was
created at `C:\Development\stillyard-mr0-baseline-20260909b` by
`scripts/prepare-source-snapshot.py`.

The [baseline source manifest](evidence/mr0-20260909/source-manifest.json) includes
every selected tracked/untracked file hash, tracked diff hash and commit. Its
canonical file-map SHA-256 is
`427cba2fa1a2437c6fbe470f3cef2de65271006d366e7f4ffb5c5f80b2f97c72`.
Rust sources and Cargo.lock are unchanged from the initial commit. The snapshot
includes the first launcher changes, before later documentation and probe changes;
its Jobs are baseline evidence, not validation of a future MR implementation.
All manifested file bytes were rechecked after native build/check: unchanged.

The first snapshot without suffix `b` was not built: Windows PowerShell rejected
`$PSScriptRoot` in a parameter default. The launcher now resolves the default inside
its body. No Job was accepted on that failed preflight. Both snapshot directories
are separate from the user's Windows checkout and installed binary.

## Initial machine survey (historical discovery, not current installation)

| Item | Observed on this run | Evidence / limit |
|---|---|---|
| Default Windows daemon | Running alpha.14, PID 56788, installed `C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe` | [daemon snapshot](evidence/mr0-20260909/windows-daemon-before.json); process path independently queried through Windows PowerShell |
| Store / generation | `01a05f1f-858c-7880-8c15-d55875da9e6b` / `01a082ce-ae5d-72d3-84e5-5579ad17b6da` | Snapshot; daemon was not replaced or restarted |
| Machine resources | cargo_slots=1, ram_mb=32768, GPU slot=1; cpu_units=0 | Configured budgets, not physical hardware measurements |
| WSL selection | `Ubuntu-SSD`, WSL2; another registered `Ubuntu-22.04` was stopped | Windows `wsl --list --verbose`, corroborated by WSL_DISTRO_NAME; registration GUID still needs adapter discovery |
| Linux | Ubuntu 26.04 x86_64, kernel `6.6.87.2-microsoft-standard-WSL2`, ext4 checkout | uname, os-release, df; no native Linux acceptance claim |
| Linux toolchains | active stable Rust 1.98.1; also 1.92.0/1.94.1/1.95.0/1.96.0/1.97.1; Linux 1.85 not installed | rustup/rustc discovery only; no Linux Cargo invoked |
| Windows toolchain | active stable Rust 1.96.0, MSVC 14.44.35207, SDK 10.0.26100.0; MSRV 1.85 available | Job environments generated with vswhere/VsDevCmd; native baseline Jobs below |
| Linux user manager | `/user.slice/user-1000.slice/user@1000.service`, `Linger=no` | systemctl/loginctl discovery; no session-survival result |
| Delegation | `systemd-run --user -p Delegate=yes` entered its own service cgroup and had writable cgroup.procs | Short completed discovery unit `stillyard-mr0-delegation-probe`; not counted as a Stillyard acceptance Job or born-contained/cleanup proof |
| Interop | `/proc/sys/fs/binfmt_misc/WSLInterop` enabled, interpreter `/init` | Actual bounded descendant escape reproduced by system Job below |
| VM keepalive | `.wslconfig` has NAT and autoProxy=false; no explicit keepalive setting observed | Capability remains unknown; neither current session nor systemd proves lifetime |
| Consumer executables | grok, claude2, claude-current, codex found in Linux PATH | Authentication, actual model and successful verdict remain untested |
| Other work | Active unrelated Rust/browser/test workloads observed in WSL | No process was killed; distro/VM stop would affect others and needs a maintenance window |

The 2026-09-07 access-denied startup observation did not reproduce: current installed
daemon access succeeds. GPU configuration names UUID
`GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a`; this does not establish fresh NVML,
physical model or WSL GPU coverage.

## Contract and implementation changes

[Requirements section 19](requirements.md#19-machine-resource-amendment-mr) records
the explicit phased exceptions to R-PKG, R-SCOPE, R-DOM, R-STORE, R-RES, R-RUN,
R-NEST, R-OBS and R-LINUX, and A-MR acceptance scenarios. Existing unamended
guarantees remain. [Protocol design](machine-resource-protocol.md) specifies roles,
keys, ownership, message/replay rules, start/cancel ordering, queue/fairness,
accounting, reset gating, reconciliation, platform proof and installation budgets.
It is a design draft, not an independently accepted or implemented state machine.

Decisions fixed in the draft: persistent owner-only pipe/interop bridge; no IP
listener; one machine allocation core; durable pairing anchors outside resettable
SQLite; no TTL release after Arm; sealed outbox releases; continuous snapshot
watermarks; per-Invocation one-shot tickets; fresh host/local quiet evidence;
explicit scope/alias mapping; domain-local parent/dependency graphs; ext4 WSL store.
The full attached runtime adapter and Grant implementation remain pending.
Transitional bootstrap proof, scoped interop prevention and safe native installation
have since been implemented and tested as recorded below.
The initial survey/launcher slice did not change schema/protocol/store epochs.
The following native authority implementation advances IPC protocol 19 to 20;
JobSpec 4, HostConfig 2 and the SQLite epoch remain unchanged.

### MR-0 continuation: durable native admission interlock

Status: implemented and installed as part of alpha.15. `src/authority.rs` persists an owner-initialized
authority epoch and holds under `<store>/authority`, outside resettable SQLite.
The registry has a bounded checksum envelope, atomic flush/publication and retained
operation tombstones. Missing history, including loss of both files, closes admission.
An interrupted publication closes the in-memory authority as well. Corruption does
not authorize reinitialization. An explicit owner assertion is needed for a new domain.

Public `authority status/initialize/hold/force-release` operations expose blockers and
audit identities. A maintenance hold requires no granted Leases, checked under the
same Store mutex as admission. All three Windows ResumeThread sites check the gate.
Managed processes cannot administer the authority. `doctor` exposes its state.
Force release is explicit operator risk acceptance; it is not an automatic Linux
cleanup proof. This interlock conservatively closes all admission and is only the
bootstrap/maintenance foundation, not the final vector allocator or MR-2 Grant API.

Windows validation uses `C:\Development\stillyard-mr0-authority-20260909a`, separate
from the installed executable and the user's Windows checkout. The
[tested file manifest](evidence/mr0-authority-20260909/tested-source-manifest.json)
records the actual formatted input bytes and base commit. Formatting itself ran in
system Jobs, with formatted Rust copied back to the Linux source tree.

| Native slice / control | System Job ID | Result |
|---|---|---|
| check (before additional admin fixture) | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085e6-bfa0-7b41-b228-175f92695825` | passed |
| full test, corrected fixtures | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085eb-119d-7012-8eb9-e39de6103f80` | passed: 264 lib, 35 CLI, 12 isolated, 2 public; 7 ignored remain unaccepted |
| ordinary WSL PE barrier | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085ea-e48b-7570-a76f-6d859f5ff640` | passed scoped control: outside cmd.exe succeeds; inside exec returns EACCES with WSL_INTEROP retained |

Canonical snapshots, stdout/stderr chunks, exact JobSpecs and receipts are retained
under [native authority evidence](evidence/mr0-authority-20260909/). Initial failed
fmt Job `01a085e5-cf36-7c61-a8ce-f866795220ea` found a Rust expression syntax error;
initial test Job `01a085e8-3cf3-7212-a7f1-eac5915e446a` and interop Job
`01a085e9-b3a9-7902-9dbd-183849e27f50` found quoted cmd control paths did not create
markers. Those commands were corrected before the positive controls passed;
failures are preserved, not counted as accepted runs.

New isolated-daemon controls demonstrate: a new runtime starts closed; explicit
initialization permits a canary; hold prevents another canary before and after
daemon restart and SQLite deletion; explicit fixture clearance resumes it; loss
of registry refuses initialization and admission; loss of both files still blocks;
active work rejects maintenance hold; a managed executable cannot administer holds.
These are native interlock tests and do not close M-A06 for the distributed protocol.

Interop capability discovery found user/mount/PID namespaces and bubblewrap usable
without sudo. The scoped negative control masks `/init`, isolates PID/mount namespaces
and `/run`, and leaves global distro interop unchanged. This proves ordinary PE
execution is blocked in that profile; it does not yet prove cgroup cleanup, bridge
authentication, durable bootstrap release, or hostile host-owner isolation.

The subsequent bootstrap/install slice below supersedes this early next-step note.
These interlock-only Jobs preceded bootstrap and do not establish its safety alone.

Windows launchers now expand all nine checked-in `*.json.in` templates from the
selected source snapshot and discovered clean MSVC environment. MSRV templates
retain placeholders and `+1.85.0`. `New-StillyardMsrvJobSpec.ps1` remains a compatible
wrapper. The root launcher rejects managed use, accepts `-RepositoryRoot`, and
optionally retains exact specs and recovery receipts under `-EvidenceDirectory`.
No daemon environment profiles were added. Snapshot creation refuses an existing
destination and checks source bytes for changes during copy.

Final launcher verification uses a separate
[launcher source manifest](evidence/mr0-20260909/launcher-source-manifest.json),
file-map hash `1ca161a4ffd58dac09250821a8b9b67f0ac9367c6376e66b0ee9eef303d49a7d`,
at `C:\Development\stillyard-mr0-handoff-20260909c`. This snapshot includes the
later generator fixes. A scheduled clean-environment probe found that Windows
PowerShell needs PATHEXT at process startup to capture native tools' output/exit;
setting it after startup did not repair command classification. The generated
Job environments now explicitly include PATHEXT and COMSPEC, and the generator
resolves cmd.exe from SystemRoot and fails clearly if its own PowerShell process
was started without .EXE in PATHEXT. All nine templates and both MSRV wrapper calls
were then successfully expanded in a system Job. These shell checks do not invoke
Cargo and are separate from the six native Cargo baseline gates.

## Job evidence

All IDs below belong to the installed **system default Windows daemon**. No Cargo
command ran directly from an agent shell. Baseline Windows Jobs used the same
manifest above and target directory outside the installed daemon.

| Job / acceptance | Actual Job ID | Outcome | Canonical evidence |
|---|---|---|---|
| baseline check | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085b7-e9e2-7e62-b2e2-0b06fb41d379` | succeeded, exit 0, empty Windows containment | [snapshot](evidence/mr0-20260909/windows-check.status.json), [stderr](evidence/mr0-20260909/windows-check.stderr.json) |
| baseline build-release | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085b8-a321-77f3-b389-d8984bb6079d` | succeeded, exit 0, empty Windows containment; **not installed** | [snapshot](evidence/mr0-20260909/windows-build-release.status.json), [stderr](evidence/mr0-20260909/windows-build-release.stderr.json) |
| baseline msrv-check | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085c1-de8d-75a2-8ccd-3af1c7c6c499` | succeeded, exit 0 | [snapshot](evidence/mr0-20260909/msrv-check-5768741cacf04d61905c175b870aac1a.status.json) |
| baseline test | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085c1-deb9-7272-a141-71aa212c6811` | succeeded, exit 0 | [snapshot](evidence/mr0-20260909/test-f1051e49ecbd4c109cb189f7f9c1391e.status.json) |
| baseline clippy | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085c1-debf-7601-91df-f2ee2c8efba1` | succeeded, exit 0 | [snapshot](evidence/mr0-20260909/clippy-409a42880bce4d54b957358b62686bec.status.json) |
| baseline fmt | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085c1-decb-7b93-bbed-1dda65fd51ef` | succeeded, exit 0 | [snapshot](evidence/mr0-20260909/fmt-31347971756d4359bc790fe330b4a7e3.status.json) |
| final launcher syntax/template generation | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085cb-ea7e-7810-9290-a8fa4ea8aa91` | succeeded, exit 0; nine templates, including MSRV check/test | [snapshot](evidence/mr0-20260909/mr0-launchers-final.status.json), [stdout](evidence/mr0-20260909/mr0-launchers-final.stdout.json) |
| A-MR-0 intermediary-timeout control | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a085c0-8228-7563-a0aa-5b55f1c39650` | **bootstrap acceptance failed**: Windows empty while matching Linux descendant remains alive | [machine-readable report](evidence/mr0-20260909/bootstrap-71ae0d65efca43159749031d083b3966/report.json), [public snapshot](evidence/mr0-20260909/bootstrap-71ae0d65efca43159749031d083b3966/status.json), exact spec and receipt beside them |
| WSL baseline check/build | see alpha.15 results below | compiler failures, protected execution and cleanup succeeded | [matched baseline](evidence/mr0-wsl-baseline-20260909/) |

The bootstrap control itself successfully detected the unsafe assumption. Its
system Job timed out after two seconds and reported `windows_job_object: empty`.
Immediately after the terminal snapshot, Linux PID 920733 still matched start tick
19870629 and was sleeping in `/init.scope`. It naturally completed its bounded
eight-second wait; final observation confirms no remaining live test descendant.
The harness exit 2 means **unsafe bootstrap detected**, not a failed script that
should be retried into a green result. Two preliminary bounded probes are also
retained: a direct Python root died on timeout; a detached descendant finished.
Neither is positive WSL cleanup evidence.

The baseline test output reports 261 library, 35 CLI, 10 isolated-daemon and 2
public-API tests passed; seven tests are ignored across those suites. Their ignored
coverage is not a passed acceptance row. Subprocess re-executions in the same output
are not additional independent suites. `msrv-test`, `fmt-write` and `schema-update`
were not executed; generating those templates is not equivalent to running them.

Launcher failure history is retained beside the final result: Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a085c8-34f6-7420-bdc6-faa63e096cda`
failed on missing LASTEXITCODE; diagnostic Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a085c9-a8c9-7c13-b724-24c9e252e6e3`
located the failure after rustup; Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a085ca-8728-7dc2-b2ee-38e742a09c87`
confirmed that setting PATHEXT inside the running PowerShell is too late;
[the PATHEXT diagnostic receipt](evidence/mr0-20260909/mr0-launchers-pathext.receipt.json)
identifies the subsequent missing-ComSpec failure. The final Job above passes with
the corrected explicit environment and generator. No failed run was relabeled pass.

Reproduce from WSL (creates a new, uniquely named evidence directory and one system
Stillyard Job, no Cargo, no daemon/distro stop):

```bash
python3 scripts/probe-wsl-bootstrap.py \
  --cli /mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe \
  --distro Ubuntu-SSD --user pythonic \
  --windows-cwd 'C:\Development\stillyard-mr0-baseline-20260909b' \
  --evidence-directory /mnt/c/Development/stillyard-mr0-evidence-20260909
```

## Capability map against alpha.14

| Surface | Code evidence | Acceptance boundary |
|---|---|---|
| Windows local transport/identity/runner | `src/daemon/mod.rs`, `transport.rs`, `src/identity.rs`, `src/runner/windows.rs` | Baseline native check/build pass; inherited full acceptance not assumed |
| Linux transport/daemon | non-Windows `run` returns UnsupportedPlatform; client also unsupported | No working installed Linux runtime |
| Linux runner | non-Windows `src/runner.rs::run` discards arguments | A no-op branch is not execution support |
| Observation | common service imports Windows-gated provider implementations | Confirmed by protected Linux check/build compiler failures on alpha.15 |
| Resources | store schedule/reservation and resources implement local vector, aging and finite reservations | No domains/Grant/authority bookkeeping yet |
| Recovery | `src/store/recovery.rs`, `src/daemon/reconciliation.rs` use Windows kill-on-close/process identities | Cannot apply that proof to WSL descendants |
| Store reset | `src/store/mod.rs::open_with_config` resets incompatible/corrupt SQLite | No external pairing/obligation restart gate |
| Stop administration | no drain/force-stop request in `src/protocol.rs` or CLI | A-14 historical requirement is not current support; safe upgrade must add maintenance gating |
| Parent/Lease/probe/postcondition | existing local lifecycle/authentication code | Must retain behavior through future refactor; no cross-domain evidence |

## Negative-control and consumer matrix

MR-2 pass rows reference [accepted-h public protocol evidence](evidence/mr2-accepted-20260910h/protocol-acceptance.md).
MR-3 rows distinguish installed evidence from protocol simulation and bootstrap.
Partial cases do not close the complete fault matrix or three consumer rounds.

| ID | MR-1 | MR-2 protocol | MR-3 live WSL |
|---|---|---|---|
| M-A01 | — | pass (h) | pass (installed z3): one-slot Grant ordering, round 4 |
| M-A02 | — | pass (h) | pass (installed z3): actual two-slot overlap, rounds 2/3 |
| M-A03 | — | pass (h) | pass (installed z3): priority aging and compatible Windows/WSL work while blocked |
| M-A04 | — | pass (h) | pass (z10e composed kernel/SQL and native coordinator crash/ack coverage) |
| M-A05 | — | pass (h) | pass (installed z3): bridge loss during Cargo and after sealed cleanup; retained debit then automatic release |
| M-A06 | — | pass (h) | pass (z7: actual isolated cross-OS Store/journal/kernel reset) |
| M-A07 | — | pass (h) | pass (z11d: actual live Linux, stale writer fenced, new incarnation reconciled then duplicate Ticket rejected) |
| M-A08 | — | — | pass (installed y2/z3): root-exit/cancel/timeout and forced cleanup failure; retained Grant then automatic seal/release |
| M-A09 | — | pass (h) | pass (installed y2): separate probe Grant; primary + two postconditions share Work Grant |
| M-A10 | — | — | pass (installed z3): actual agent managed Cargo and two-call replay; Windows token wait observed |
| M-A11 | — | pass (h) | in_progress: real cross-OS quiet/impact rounds pass; suspend/resume pending |
| M-A12 | — | — | in_progress: idle restart retained history; active daemon crash passed with retained exact boundary; VM/logout pending |
| M-A13 | not_run | — | — (MR-4 also not_run) |
| M-A14 | — | — | — (MR-4 not_run) |
| W-C1 | — | — | pass (installed z3): rounds 2/3/4, Cargo capacity 2/2/1 with event ordering |
| W-C2 | — | — | pass (installed z3): actual CLI, typed results/postconditions; client loss/recovery in round 2 |
| W-C3 | — | — | pass (installed z3): three real agent/child/replay rounds, one-slot Windows wait |
| W-C4 | — | — | pass (installed z3): three real measurements, quiet retries and cross-OS impact exclusion |

## Installed bootstrap and matched baseline — current evidence

The default Windows daemon is installed alpha.15, local IPC 20, PID 53160,
generation `01a086e8-b524-70b1-b563-4b2eb76ce274`, with the original store UUID
`01a05f1f-858c-7880-8c15-d55875da9e6b`. Installed binary SHA-256:
`868d089a7ae94445457b32b43e91a5d3c54e845535ec639fc9a8e4065c89c8e4`.
[Current public snapshot](evidence/mr0-install-20260909/current-daemon-status.json)
and [authority](evidence/mr0-install-20260909/current-authority.json) show no blocker.
No user queue was reset. Native SQLite epoch, JobSpec 4 and HostConfig 2 are unchanged.

[Installation evidence](evidence/mr0-install-20260909/) retains candidate/source
identification, exact old process identity, admission transaction barrier, backup,
new installed process and authority checks. Installation takes a Windows SQLite
write transaction, requires no granted Lease or blocking containment and holds that
barrier until the exact old daemon is stopped and the candidate is published.
The first apply safely refused before stopping because three historical cleared
containments were counted as live. The corrected installer accepts only audited
terminal resolutions; an isolated test covers this fixture. Production rows were
not edited. WMI starts the default daemon independently of the installing shell.

Current native and Linux baseline inputs share the
[manifest](evidence/mr0-bootstrap-20260909/candidate-alpha15-cleared-source.json),
file-map SHA-256 `657c21539fcabdb43285a6729a3357e3c4e916da04ac607fed9491fd187c8861`.
Windows snapshot: `C:\Development\stillyard-mr0-bootstrap-20260909a`.
Linux snapshot: `/home/pythonic/Development/stillyard-mr0-baseline-alpha15-20260909`.
Both are separate immutable validation inputs; later implementation uses new snapshots.

All Job IDs in the next table use prefix
`01a05f1f-858c-7880-8c15-d55875da9e6b~`. Native exact specs, receipts and canonical
logs/status are in [bootstrap evidence](evidence/mr0-bootstrap-20260909/).

| Job | ID suffix | Result / scope |
|---|---|---|
| Native full test | `01a086db-9c9c-7090-9116-0bed4e7f83c9` | passed on pre-installer-fix manifest e870c344…; nine ignored tests are not passes |
| Native fmt | `01a086db-9cc8-7e41-8131-0253ebe4388a` | passed on e870c344…; later Rust fixture formatted by scheduled fmt-write |
| Native MSRV test | `01a086db-9cc1-7172-8ede-ae74d186456c` | passed on e870c344… |
| Real WSL bootstrap + installer controls | `01a086e6-2526-7a53-ab77-c44780d94a0e` | both explicit ignored tests passed on final 657c2153… inputs |
| Native check | `01a086e7-18c9-7b73-b0b3-0eb5b5190f54` | passed on final inputs |
| Native Clippy | `01a086e7-18f5-7441-bb2a-beaab412e63c` | passed on final inputs |
| Native MSRV check | `01a086e7-18d9-71d1-828f-aa08a548d55f` | passed on final inputs |
| Native build-release, installed | `01a086e7-18fd-7981-9420-1bd2a46c3319` | passed; binary hash above |
| WSL check | `01a086e9-45e4-7150-9396-beb9bf24cc89` | compiler exit 101; exact source unchanged; sealed cleanup and released authority hold |
| WSL build-release | `01a086ea-76a6-7e41-9397-750083d54543` | compiler exit 101; exact source unchanged; sealed cleanup and released authority hold |

The Linux failures are E0432/E0425 in host observation: common service imports
Windows-only providers and common code calls a Windows-gated observation clock.
[Canonical Linux logs and results](evidence/mr0-wsl-baseline-20260909/) retain these
baseline failures. They establish compiler portability work for MR-1; the phase
plan explicitly permits a failing initial Linux compiler baseline. They are not
bootstrap cleanup failures and have not been relabeled successful builds.

The bootstrap matrix exercises explicit distro/UID/env/cwd, stdout/stderr, exit 25,
timeout with detached descendant, native cancellation/bridge loss, coordinator
restart and coordinator SQLite deletion while Linux work exists. A native canary
remains blocked by the durable authority hold until authenticated reconcile accepts
the sealed Linux proof. [Five retained Linux seals](evidence/mr0-bootstrap-20260909/linux-seals-cleared-matrix/report.json)
bind actual operations to that system Job and final source hash. All recorded work
cgroups were removed after verified `populated=0`. Scoped PE execution is denied.
This global bootstrap interlock does not implement per-resource Grants, concurrent
attached admission or the full MR-2/MR-3 fault matrix.

[WSL registration discovery](evidence/mr0-install-20260909/wsl-registrations.json)
identifies Ubuntu-SSD as `{4a62a312-631a-4038-b191-6fd3a0f57860}`. Its configured
default UID is 0; all bootstrap operations explicitly select `pythonic` UID 1000.
A registered name/default UID is not used as proof of process cleanup.

## Blockers and next operation

**B-01 closed for transitional bootstrap:** alpha.14's demonstrated unsafe
`wsl.exe`-only path has been replaced with an installed durable native obligation
and delegated Linux supervisor. Failure records remain evidence. No direct Cargo
was used. No external permission blocker prevents continued MR-0/MR-1 development.

MR-0 contract audit: sections 1–2 cover ownership/identity/storage; 3 wire/trust;
4 start/cancel/postconditions; 5 release/history; 6 vector/queue/accounting; 7 crash
traces; 8 platform/bootstrap/lifetime; 9 public compatibility/installation/bounds;
10 selected commands/profiles/manifests. These design decisions are closed; their
full distributed implementation and live acceptance remain MR-1..3 work.

Consumer artifact control Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a086f4-33e5-7711-bfee-fa9c16c827eb`
passed three contract tests: final/error/empty/wrong-model verdict controls, real
256-round hash measurement with changed-source rejection, and installer downgrade
rejection. [Exact inputs and canonical evidence](evidence/mr0-consumer-contract-20260909/)
are retained. These tests use synthetic review fixtures and do not count as W-C2.
The selected real Claude profile/auth metadata is retained separately in
[discovery](evidence/mr0-consumer-discovery-20260909/).

Next: implement MR-1 shared vector/ordering core and explicit platform providers,
then run Windows regression/MSRV gates before installing the refactored Windows build.
Do not mistake the installed Windows bootstrap for the requested WSL delivery.

Remaining MR-3 prerequisites include Linux MSRV 1.85; persistent installed user
service/delegation and explicit VM keepalive; full authenticated domain adapter;
shared queue and real consumers; controlled VM/logout acceptance. Unrelated active
WSL work must not be terminated to make disruptive tests convenient.


## MR-1 first core slice

`in_progress`; installed default remains the previously validated bootstrap alpha.15.
The pure `admission` module now owns full-vector scalar/fence/impact checks and aging.
Native admission delegates to that core, including hierarchy-aware vector evaluation.
The scoped core validates a bounded domain tree, expands physical claims once per
ancestor constraint, resolves explicit GPU/token aliases, and keeps fences domain-local.
It has no independent mutable counters, database or network operations.

[First core snapshot](evidence/mr1-core-20260909/core-source.json) has file-map hash
`bb8ad054d2d4346e5565e16f20554c863f572c92c6fd966f67611536b63e7a4f`.
Native path `C:\Development\stillyard-mr1-core-20260909a`; identical Linux path
`/home/pythonic/Development/stillyard-mr1-core-20260909a`.
IDs below use the original system store prefix `01a05f1f-858c-7880-8c15-d55875da9e6b~`.

| Gate | Job suffix | Result |
|---|---|---|
| native fmt-write | `01a086fb-2012-7013-88e1-57fb2c4ee50e` | passed |
| native check | `01a086fb-a6be-7540-aa81-4557ca8856d6` | passed |
| protected Linux check | `01a086fb-f956-70b2-957a-69a1a90724cb` | passed; source unchanged; two platform-gating warnings recorded |
| native test | `01a086fc-c3c8-7093-96db-903bd7c3b885` | passed: 267 library, 35 CLI, 12 isolated, 2 public; nine ignored remain separate |
| native Clippy | `01a086fc-c411-7002-8e13-7e0aec182065` | passed |
| native MSRV check | `01a086fc-c41e-7363-ad3f-afff6e6b0317` | passed |

[Native evidence](evidence/mr1-core-20260909/) and
[Linux evidence](evidence/mr1-core-linux-20260909/) retain exact source/spec/receipts.
Linux observation now selects an explicit platform facade and a real boot clock;
unimplemented providers return unsupported/unavailable, never fabricated evidence.
A successful check does not mean Linux daemon/IPC/containment/quiet support is installed.

Subsequent unvalidated local edits extract shared runner settlement and the reservation
conversion decision, parameterize containment records/doctor by backend, and replace
non-Windows's unconditional filesystem success with a Linux ext4 check. These edits
are not covered by the first-slice Jobs above and require their own snapshot/gates.
MR-1 remains open for remaining platform/public integration and installation.


### MR-1 shared lifecycle/platform slice

Snapshot `C:\Development\stillyard-mr1-platform-20260909b`, file-map SHA-256
`421b0d6e2e1a4df91981f5961ec2964678994b1d43d9d223702c87f35182b236`, passed native
check, test, Clippy and MSRV-check after scheduled formatting.
[Evidence](evidence/mr1-platform-20260909/) retains the exact source and Jobs:

- check: `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08704-12d4-7f22-8810-75957ad212f6`.
- clippy: `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08704-12e2-78c1-b3b0-56ad2378e1f5`.
- fmt: `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08702-a9cb-7153-a47e-c53730afb002`.
- msrv: `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08704-131d-7500-b51d-23363fdf2f97`.
- test: `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08704-131a-7f72-9ff9-a0b28160938f`.

This validates the common primary/probe/postcondition settlement callback and pure
reservation/conversion decision, with backend-specific containment records/doctor.
Windows test counts remain 267 library, 35 CLI, 12 isolated and 2 public passes;
nine ignored tests remain separate. Linux ext4 checking is implemented in this
snapshot but not yet runtime-accepted.

Candidate alpha.16 now adds persistent machine/native domain identities to continuous
authority history, a scoped public resource snapshot and machine-scheduling schema.
Local IPC advances to 21. The installed default remains alpha.15 pending candidate
validation and the empty-instance installation barrier. Subsequent identity/transport
codec extraction and these public additions require the candidate gates; the prior
Jobs are not their validation.


### MR-1 alpha.16 candidate and recovery of validation evidence

MR-1 remains `in_progress`; the installed Windows default is still alpha.15.
Candidate alpha.16 / IPC 21 adds persistent machine/native domain IDs, scoped
accounting, explicit observation time/config identity, and the published
`machine-scheduling` schema. The authority migration preserves existing holds;
missing/corrupt history still prevents admission. Linux unsupported providers
remain explicit and do not constitute WSL runtime support.

Candidate d source file-map SHA-256:
`1c4b9ccef08347ecf903c23ccac0fe9857877791e56c1a98d833690633bc28fb`.
[Native evidence](evidence/mr1-final-20260909/) and
[matched Linux check evidence](evidence/mr1-final-linux-20260909/) retain this
snapshot. Job suffixes use the original system-store prefix above:

| Gate | Job suffix | Result |
|---|---|---|
| fmt | `01a08715-6d78-7510-a29c-09ff63d3515f` | passed |
| check | `01a08715-6d42-7cd2-aee6-ac2ee7ff2570` | passed; receipt recovered by original idempotency key after client OS error 5 |
| Clippy | `01a08715-6d8f-7851-8e12-d7652fb11934` | passed |
| MSRV check | `01a08715-6d9f-75a1-97cd-226536387317` | passed |
| test | `01a08715-6d1f-7533-998c-be02c11aecdf` | failed: second independent doctor capture compared its timestamp for equality |
| MSRV test | `01a08715-6d97-7392-a3e7-1d653048758c` | same assertion failed; 269 library tests passed |
| build-release | `01a08715-6d80-7341-9458-415d12eb5db9` | passed; not installed |
| targeted bootstrap | `01a08715-6da4-77e0-b1f3-8b8d07ff7648` | installer control passed; bootstrap wait failed after 30 seconds; no matching Linux operation intent observed |
| protected Linux check | `01a08719-2475-7b21-a112-c62f0c5c798a` | passed; source unchanged; cleanup sealed and authority released |

The repaired candidate e normalizes only observation capture time in the remaining
idle-state equality check and adds Job/status/authority/stderr diagnostics to the
bootstrap wait assertion. It does not lengthen the deadline or weaken cleanup.
Its source file-map is
`4ccb4ef25bfdbb7b823f69a6c8f34f9be273f75addedac350d39577423772702`;
validation Jobs are running under the installed default. Failed candidate evidence
above remains failed, regardless of later results.

Linux Rust 1.85.0, rustfmt and Clippy were installed through protected system Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08713-59e0-72a1-82c6-5b7bff47b937`.
[Provisioning evidence](evidence/mr1-msrv-provision-20260909/) records successful
cleanup. This closes toolchain availability, not Linux MSRV acceptance.

Next: diagnose/recheck candidate e, install only after the native gates and
bootstrap controls pass, verify installed scoped accounting, then begin MR-2.


### MR-1 delivery — installed native shared core

MR-1 is `complete`. Candidate e passed every native gate on its unchanged manifest
`4ccb4ef25bfdbb7b823f69a6c8f34f9be273f75addedac350d39577423772702`.
[Final candidate evidence](evidence/mr1-repair-20260909/), original store prefix:

| Gate | Job suffix | Result |
|---|---|---|
| fmt | `01a08726-adef-7892-bb39-3a290aab60a3` | passed |
| check | `01a08726-adfc-7602-8494-2ce6956cb610` | passed |
| test | `01a08723-366a-7c03-8309-cc441c91606e` | passed: 269 library, 35 CLI, 12 isolated, 2 public |
| Clippy | `01a08723-3659-7343-86e3-b69ce70d8b0c` | passed |
| MSRV check | `01a08726-adf4-7991-958c-f24590468379` | passed |
| MSRV test | `01a08723-3651-7863-9cdd-923966b1ae94` | passed; same test counts |
| build-release | `01a08723-366f-7d20-a2ab-ecdda1d372c4` | passed |
| targeted bootstrap/installer | `01a08723-3676-7070-ac67-e82d2eb27984` | both passed, 24.22 seconds |

Nine ordinary ignored tests remain separately reported; the two bootstrap controls
were explicitly exercised. Candidate d's bootstrap wait failure did not reproduce
with expanded diagnostics; its underlying cause is not established and its evidence
remains failed. Public behavior regressions only required adjusting comparisons of
independent observation timestamps; identity, state and resource comparisons remain.
M-A13 is covered at the pure-kernel level by ancestor budgets, single physical debit,
alias collision, complete-vector rejection and overflow/reservation controls. This
does not mark the later live VM/container acceptance complete.

The safe installer applied candidate SHA-256
`38246f2e6c04b3983d98b30e9da4434f26ed27afd63f25e9a504f7b2c5f0d732`
at the installed Windows path, using the exact process identity and SQLite empty
admission barrier. [Installation and active accounting evidence](evidence/mr1-install-20260909/)
shows unchanged queue history/store UUID, authority holds and epoch. The new daemon
is alpha.16 / IPC 21, PID 56044, generation
`01a08727-6971-79a1-a1b3-fd73badf9036`.

Persistent IDs: machine `01a08727-697d-7002-b4e5-c9ac495818f3`, machine scope
`01a08727-697d-7002-b4e5-c9bb9d5af41f`, native domain
`01a08727-697d-7002-b4e5-c9c5feed5486`. A real test Job scheduled by the newly
installed daemon showed machine cargo_slots capacity=1/granted=1, then completed:
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08727-e78b-74e0-a9a0-98680a7a9dbb`.
[Installed Job evidence](evidence/mr1-installed-jobs-20260909/) records all ordinary
tests passing. The mode is explicitly standalone: attached Grant traffic and Linux
execution are still MR-2/MR-3 work.

Current slice: MR-2 durable protocol, beginning with typed allocation/session keys,
strict bounded framing and reset continuity, then the shared candidate/Offer/Arm
path with fake participants and fault tests. No MR-2 acceptance is claimed yet.


## MR-2 protocol foundation

MR-2 remains `in_progress`; installed default remains MR-1 alpha.16 / IPC 21.
The uninstalled alpha.17 / IPC 22 wire slice adds typed allocation/session/cleanup
messages, strict 1 MiB framing, version/hash checks, and HMAC-SHA256 pairing
challenge primitives. Its source file-map is
`c2d08e77553e9448cde4c576d14b07bb6926b7fc0435bd876e644cab4fa50765`.
[Wire slice evidence](evidence/mr2-wire-20260909/), original system-store prefix:

| Gate | Job suffix | Result |
|---|---|---|
| fmt-write | `01a0872c-9b09-7213-8900-30671fa88848` | passed |
| check | `01a0872d-4352-7632-846e-8b64a8043139` | passed |
| test | `01a0872d-4345-70c3-9b54-b53c7aa967c2` | passed: 272 library, 35 CLI, 12 isolated, 2 public |
| Clippy | `01a0872d-434e-7e33-8b90-6798becaef60` | passed |
| MSRV check | `01a0872d-434b-77d0-9b51-e68b8e4e6a12` | passed |

These are framing/authentication primitive controls, not M-A04/07 distributed
acceptance. Subsequent pairing/reset edits are outside this snapshot. Pairing now
has an external intent before its SQLite commit and a public bridge handshake with
one-use random challenges and persistent connection epochs. Before SQLite reset,
the authority preserves a durable reconciliation gate and predecessor identity.
The new gate is separate from individual bootstrap holds. Complete native/guest
inventory reconciliation, Grant operations and queue integration remain unfinished;
this candidate must not replace the installed default yet.

Pairing validation snapshot b, hash
`2ec68efd227a9384a011079e7d05d503a8c5e49a8abb9f7d39093955ce48d863`,
is running through system Jobs. Public tests exercise pairing replay/conflict,
wrong tag, old challenge, reconnect, restart and store reset. The targeted bootstrap
reset recovery will need the full MR-2 reconciliation path before it can be accepted
with the new coordinator gate; the existing passing alpha.16 installation remains
available during implementation.


Pairing slice b results are retained in
[pairing evidence](evidence/mr2-pairing-20260909/). System Job suffixes:
check `01a08738-573f-7c83-8b7c-9c97d2e1fe11`, test
`01a08738-573a-7b53-a09c-09092b398cc2`, MSRV-check
`01a08738-56d3-74e1-a1cc-34b40821f69d` passed. Test counts: 274 library,
35 CLI, 13 isolated and 2 public. The new isolated public test covers pairing and
challenge replay/restart/reset; it does not yet issue a Grant. Clippy Job
`01a08738-572a-75e2-9aa4-0837e60878f8` failed only on the enlarged internal
`authority::State` enum. The subsequent boxed representation is pending validation.

Coordinator SQL additions use a separately versioned `machine_*` extension without
resetting baseline Job tables. The extended external authority registry is rejected
by older executables, preserving their closed-admission downgrade behavior. Pairing
secrets are excluded from Debug/public snapshots. Further local edits add Grant
DTOs and coordinator tables; these are not covered by slice b evidence.


### MR-2 first common Offer queue

Snapshot c, file-map SHA-256
`9115f7a803fe970e935432627db67db54da981756326dd67d7c57b60d542239e`,
passed native check, ordinary tests and Clippy. [Evidence](evidence/mr2-offer-20260909/)
uses the original system-store prefix: check `01a0874b-c9b4-7fd0-89a4-fa93f307f3c4`,
test `01a0874b-c9ba-79a0-b9f4-fa71013ca96b`, Clippy
`01a0874b-c9c0-7d60-9b0c-d9406218f57a`, formatting
`01a0874a-5333-7a73-ab0b-017cca4baae8`. MSRV-check's initial result-file
publication failed with native OS error 5; the original operation was recovered
using its persisted key and completed successfully. Its recovered receipt/status
are retained beside the original unknown-acceptance receipt.

The public fake-participant test now authenticates, reconciles an empty initial
inventory, advertises a candidate and observes an Offer consuming machine
cargo_slots=1. A real native canary Job with the same claim remains pending until
withdrawal, then completes. Exact advertisement replay returns the same outcome.
The queue scans remote candidates in the common priority/aging order around native
candidates; native admission counts remote Offered/Armed/Uncertain debits, and the
scoped snapshot distinguishes offered from granted usage. An unarmed Offer has a
finite expiry and yield. This is partial M-A01/pre-Arm evidence, not the full Grant
fault matrix. Arm/tickets/release, remote scalar reservations, resource events,
complete inventory recovery and live consumers remain pending.

The launcher now retains a durable receipt even without an evidence-directory
argument and retries unknown acceptance using the same idempotency key. That helper
change followed snapshot c and will be exercised by the next scheduled gates.


### MR-2 durable Arm and sealed no-start release

Snapshot d, file-map SHA-256
`457450f2bd062a36fdf236e7b551632119f6fe41f389c1970b3a7c5672d01063`,
passed all scheduled gates. [Canonical evidence](evidence/mr2-arm-20260909/)
uses store prefix `01a05f1f-858c-7880-8c15-d55875da9e6b~`:

| Gate | Job suffix | Result |
|---|---|---|
| fmt-write | `01a0875a-3357-7bf3-8369-06d147889e5e` | passed |
| check | `01a0875b-348a-7aa3-b3f3-2f9633a7bdbe` | passed |
| test | `01a0875b-347b-7751-83be-ab0fc38b49de` | passed: 274 library, 35 CLI, 13 isolated, 2 public |
| Clippy | `01a0875b-3483-7333-8b29-0fe577649a3a` | passed |
| MSRV check | `01a0875b-3487-7b12-92c6-fc7bb89f91a5` | passed |

The public fake participant Arms an offered cargo token, retains it beyond the
Offer TTL and after withdrawal, and unblocks a real native canary only after a
sealed no-start release. Exact Arm/release replay is idempotent; replaying an old
Arm after release does not resurrect authority obligations. The external authority
journal prepares potentially used rights before the SQLite operation commits and
acknowledges release only after both durable writes. Automatic pending-commit
recovery exists; fault injection was not yet exercised by snapshot d.

This closes additional partial M-A01/04/05 controls, not full MR-2 acceptance.
Invocation tickets, remote reservations, complete reset reconciliation, history
compaction and MR-3 remain unfinished. Installed default stays alpha.16 / IPC 21.
Next slice adds authenticated operation messages and crash-boundary negative
controls. Later observation-policy preparation is outside snapshot d evidence.

Recovered snapshot-c MSRV Job was
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0874b-c926-76b3-b91b-6f2891d9d25d`.


### MR-2 authenticated operations and crash-boundary recovery

Snapshot e file-map `6383e623727eea14c321ded60c60eaecb4777b9ba66ba84cb0b8512a1a86d0dd`
passed [all scheduled gates](evidence/mr2-fault-20260909/). System-store prefix above:

| Gate | Job suffix | Result |
|---|---|---|
| fmt-write | `01a08766-8eef-7093-9200-4c9afcc48d91` | passed |
| check | `01a08766-f05d-73c2-840a-5094d92c6a39` | passed |
| test | `01a08766-f06d-7312-ac22-fd596eef64fc` | passed: 275 library, 35 CLI, 14 isolated, 2 public |
| Clippy | `01a08766-f042-78e1-a639-7ee092d75999` | passed |
| MSRV check | `01a08766-f056-7d41-8c0e-cb876bade9e5` | passed |

The isolated public fixture abruptly exits its coordinator at four boundaries,
for both Arm and release: before external prepare, after external prepare, after
SQLite commit, and after external acknowledgement before reply. All eight injected
exits were observed as code 86 with per-operation fired markers. Ordinary restart
and exact operation replay completed automatically; the native canary remained
blocked through Armed TTL expiry and withdrawal, and completed after sealed release.
No default daemon or foreign queue was stopped. Fault hooks are absent in release
builds and require an explicitly selected isolated root and adjacent pinned binary.

Each machine operation now authenticates its full session/sequence/UUID/payload
hash with the pairing HMAC. Unsigned operations do not advance durable sequence;
mutated identity, sequence, payload with recomputed hash, and wrong secret fail.
These are partial M-A04/05/07 controls. No user-code ticket was issued by snapshot e;
subsequent Invocation/quiet-policy work is outside its evidence. MR-2 remains
in_progress and the installed default remains alpha.16.


### MR-2 per-Invocation tickets

Snapshot f file-map `37650c5693a239007d7fd87c77b25661373ac34c9aef6a86d75d559db6596954`
[canonical evidence](evidence/mr2-ticket-20260909/), original system-store prefix:
check `01a0876c-7daf-7473-90ee-bcfb6e2404f1`, test
`01a0876c-7da2-7403-9247-846ebb6d51e7`, MSRV check
`01a0876c-7da8-7122-a339-e515601068dc`, fmt-write
`01a0876c-2c91-79e0-9f73-733d3e1860e2` passed. Tests: 275 library,
35 CLI, 15 isolated, 2 public. Clippy `01a0876c-7dab-7142-8688-32300acf4abf`
failed on the now-unreachable catch-all Unsupported command arm; the next snapshot
removes that arm. Its failed result is retained.

The coordinator issues a single-use Invocation ticket only under a fresh host
sample barrier using the shared native readiness evaluator and quiet progression.
The external journal retains ticket identities before response; exact replay returns
the original issued timestamp. Every next ticket requires predecessor cleanup,
valid role/index/sequence and a fresh Invocation/Containment/challenge. Probe authority
is tied to its own Invocation. A work Grant spans primary and postconditions.
Sealed release covers every ticket and cannot contradict prior cleanup attestations.

The public fake-participant harness now covers primary and postcondition ticket
crashes at all four durable boundaries as well as Arm/release (16 observed exits).
It rejects wrong role, duplicate new operation, missing predecessor cleanup and
incomplete release, while preserving the native canary exclusion. Separate probe
controls reject borrowing postcondition authority. These are partial M-A04/09
controls, not actual Linux process-release evidence. Host quiet failure/freshness
negative controls and manager-side outbox/release consumption remain to be exercised.

Next slice connects attached scalar reservations to the native common ordering and
adds public busy-token conversion, compatible bypass and two-token concurrency
controls. Full reset reconstruction, native Grant links/events, compaction and the
installed MR-3 WSL runtime remain outstanding; no phase completion is claimed.


### MR-2 common reservations — first validation

Snapshot g file-map `6cd266173a636531859b5cb8c2fbdcaf4126365eae42a8366cc4c1ae5960f35a`,
[evidence](evidence/mr2-reservation-20260909/), system-store prefix above:
check `01a08773-6d6c-70e2-a3ea-889b4c674f6e`, MSRV check
`01a08773-6cdd-7133-9ce7-46604ce1755f`, formatting
`01a08773-283e-7973-9377-b0f30e4c086a` passed. Test
`01a08773-6d73-7282-b299-f6cbae828fc6` passed the existing library/CLI and
14 isolated cases, but the new queue fixture advertised unsupported priority 10
instead of the public maximum 3 and was correctly rejected before its reservation
control. M-A02/03 conversion/concurrency are therefore not yet passed. Clippy
`01a08773-6d79-77e3-8441-5dcb75610560` failed on a redundant assignment in
successful reservation conversion. Both issues are fixed in the next snapshot.

Attached scalar reservations now use the same pure full-vector decision and common
accepted order as native reservations, with finite deadlines, conversion, backoff,
capacity normalization and scoped public accounting. Native grants/probes count
attached reservation debits; reconnect/readiness expiry drops unused reservations.
Subsequent root edits add sealed-release application through complete reconciliation
snapshots, journal recovery for that commit, and missing-page/replay/crash controls.
These edits are outside snapshot g. MR-2 remains in_progress; MR-3 has not started.


### MR-2 reservation/concurrency and sealed-snapshot recovery

Snapshot h file-map `5a7a2e5f2e66cba7aea3e326e2d80ad21d0d184f18984167728ce7cd43223417`
[passed all gates](evidence/mr2-reconcile-20260909/), original system-store prefix:
check `01a08776-3d65-7073-993f-9d39b176b3c4`, test
`01a08776-3d62-7441-8ff8-89344eedfa9d`, MSRV check
`01a08776-3d4a-7d11-818e-19a7939e4485`, Clippy
`01a08776-3d76-7821-92f0-6f6928d3ed55`, fmt-write
`01a08775-ee41-7b51-ba03-c81b5fce4938`. Test counts: 275 library,
35 CLI, 15 isolated, 2 public.

Public controls now observe a remote cargo reservation while a real native holder
runs; a lower-ranked compatible side-token candidate receives an Offer before that
holder finishes. The reservation converts before a waiting native cargo Job, which
runs after withdrawal. With capacity two, an Armed fake-participant cargo allocation
and a real native holder simultaneously show machine granted=2. This is MR-2
M-A01/02/03 evidence; live WSL concurrency remains MR-3 work. Reservation deadline/
normalization/backoff implementation is present but its full fault matrix remains.

Sealed releases in complete reconciliation snapshots use the same release validator
and external journal as ordinary release. Missing pages retain all rights; page
replay is exact. Four crash boundaries around the sealed-snapshot commit recover
all release records and the participant reconciliation watermark automatically.
The 16-exit harness still covers Arm, primary/postcondition tickets and now snapshot
release; ordinary release crash evidence remains in snapshots e/f. None of these
fake-participant controls proves Linux cleanup or guest durable outbox behavior.

Next: public native Lease/Grant links, machine events, quiet/freshness negative
controls, manager outbox and full history-loss/upgrade recovery. Installed default
continues serving alpha.16; MR-2 in_progress, MR-3 not_started.


### MR-2 native allocation links and host quiet

Snapshot i file-map `e1a043127679027b23a9812ee91c9043538ab2db949cbca414abc18968c795dc`
[passed all gates](evidence/mr2-observation-20260909/), original system-store prefix:
check `01a0877b-edb2-7220-a766-f2ad2784f9f1`, test
`01a0877b-eda7-72c3-bab5-d270531fbb71`, MSRV check
`01a0877b-edb5-7902-a725-2738452adbae`, Clippy
`01a0877b-edae-7ae2-abbb-c02a71939ff3`, fmt-write
`01a0877a-f056-70e2-a723-60372cd264c6`, schema-update
`01a0877b-2e9e-7021-8459-6bfdb20dc026`. Tests: 275 library,
35 CLI, 15 isolated, 2 public.

Job status exposes native allocations derived from the authoritative Lease, with
stable Grant/Lease identity, domain/allocation key, owner, state and complete claims.
The two-slot public test observes a native Armed allocation beside its distinct
attached Grant, then the same native ID Released after completion. The real native
probe test verifies separate probe/work allocations. This adds no extra resource
counter. Native durable start intents still need external reset-journal coverage.

The CPU/impact public control retains a Windows cpu_heavy Job while spare scalar
capacity exists; a measurement candidate gets no Offer until impact cleanup. A real
host CPU provider with max=100 then requires the full one-second quiet interval
before issuing a ticket. Replaying a ticket after 300ms preserves the original
issuance, and an old rejected request never becomes a fresh authorization. This
is partial M-A11 host evidence, not guest freshness/suspend acceptance. The protocol
schema is now published separately as stillyard-machine-protocol-v1.json; schema
changes were generated by the named system Job.

Next root edits add bounded common native/attached allocation events with explicit
cursor gaps and same-store identity checks. They are outside snapshot i evidence.
MR-2 remains in_progress; manager outbox, full reset reconstruction and remaining
fault controls precede MR-3 installation and live consumers.


### MR-2 common allocation events

Snapshot j file-map `44d0b6ecfcd1a75f5ba30c906f7bc7af1994a6ecb0ef30d2ad2bce75cae684a4`
[passed all gates](evidence/mr2-events-20260909/), original system-store prefix:
check `01a08784-5857-7803-b0e9-56030aa20525`, test
`01a08784-5865-76b1-8111-9122f1482de8`, MSRV check
`01a08784-5852-7a63-bf91-ae0d90ff2de9`, Clippy
`01a08784-57db-7fc2-b2f5-e8e76c96d10f`, fmt-write
`01a08780-4b5b-79e2-a471-3009b7a59ee2`, schema-update
`01a08780-ca5e-7ed2-9072-583c7c3fd529`. Tests: 276 library,
35 CLI, 15 isolated, 2 public.

Native Lease transitions and attached Offer/Arm/ticket/release transitions now
share an ordered machine event stream committed in their authoritative SQLite
transactions. Public Client.machine_events and explicit-endpoint machine events
CLI return bounded pages, store-scoped cursors and explicit retention gaps. The
stream retains at most 4096 records/16 MiB, independently of durable Grant history.
Native ticket counts are zero in this event projection; native process-release
intents are not being presented as attached wire tickets.

Public fault fixtures page through the stream and verify exactly one committed
transition per allocation/ticket despite lost acknowledgements and crash recovery,
with native grant after attached release. Store reset rejects the old cursor.
Retention unit controls evict events while leaving a granted Lease intact.
Latest root changes add reset-independent native root permissions before possible
user-code release; these are not covered by snapshot j and await validation.


### MR-2 native history negative control — snapshot k failed

Snapshot k file-map `00110b9a6839916f40b7423d7215e62c723319fac86e8f74947b8446ddde115c`
[retained Jobs and manifest](evidence/mr2-native-history-20260909/), original system
store prefix: check `01a0878c-a42a-7903-a17a-ef9fc485a061`, Clippy
`01a0878c-a41a-7281-9bae-ba6d108bc0f3`, MSRV check
`01a0878c-a42f-7300-a8cb-a0a20c9b1c73`, schema update
`01a0878b-8764-7a00-8add-2802ce2ffd8f`, fmt-write
`01a08789-d1e1-7942-939e-29bbf3ca7f88` passed. Formatting receipt publication
failed with access denied; recovery of the original key
`01a08789-d1da-7df2-a8be-2627e8d3f947` into a fresh receipt found that same Job.
The original unknown receipt is retained beside its recovered receipt.

Test Job `01a0878c-a417-7d31-8630-a89058337f56` failed: 276 library and 35 CLI
passed; isolated tests 12 passed / 3 failed, public tests not reached. Three new
assertions observed a live native allocation and root marker but no external
native permission. Inspection confirmed that only the conditions/quiet path used
record_suspended_root; ordinary primary/probe/postcondition launches used
mark_started_with_identity without external journaling. The root fix shares
permission construction and records it on both paths before user-code release.
Assertions are retained. Participant session/sequence checkpoint edits are also
outside snapshot k and remain unvalidated. No candidate installation occurred.


### MR-2 native history coverage and participant checkpoints — snapshot l passed

Snapshot l file-map `1c31a0c5ff9a2086d2df07e4e708deba7106f77c1036d8d9385eea88783a11b2`
[passed gates](evidence/mr2-native-history-20260909l/), original system store prefix:
check `01a08796-f139-7b43-9c68-40e4e3a5df1c`, test
`01a08796-f13c-7fc0-837b-1fdbb396317a`, Clippy
`01a08796-f14b-7a81-8988-cdf2a404bfd6`, MSRV check
`01a08796-f150-7010-a48b-53e5118a0235`, fmt-write
`01a08796-6535-7922-bd14-ab5a1829aa7d`. All 276 library, 35 CLI,
15 isolated and 2 public tests passed. The three strict native-journal assertions
that failed in k now pass, including active root identity and retention after
SQLite deletion. Shared permission construction covers ordinary and guarded
Windows launches before user-code release; proven cleanup retires external rights.

Connection epochs, active session and accepted sequence now have durable external
checkpoints. An abandoned challenge reserves its epoch permanently; replacement
handshake uses epoch 3, old challenge fails, restart preserves the new session.
Startup compares SQL participant and Grant inventories against external history,
reconciling pending journal commits first. Partial rollback remains gated.

Current root adds explicit safe machine recovery: reconstruct old allocations
from external history into a replacement coordinator, require complete manager
reconciliation, verify exact native creator/root death using Windows identity,
and advance authority epoch only after all obligations are covered. The next
public test retains native and fake executor rights across reset, rejects an
empty manager snapshot, and checks eventual admission after sealed cleanup.
This is outside snapshot l; full manager outbox and MR-3 remain unfinished.


### MR-2 reset reconstruction — snapshot m compilation failure

Snapshot m file-map `6b0f8a674e42c6255f9bf3ce502660afb838a2ca5d6e40a752ac0bce86f0dc28`
[retained evidence](evidence/mr2-reset-20260909m/). Fmt-write
`01a0879c-fe19-71d1-9265-c0619963ebc0` passed. Check
`01a0879d-9fd6-7032-a569-0cd05de236eb`, test
`01a0879d-9fe8-7ff3-a897-8852a4d788da`, Clippy
`01a0879d-9fe2-7d60-8bee-817e30f4f828` and MSRV check
`01a0879d-9fbe-7bc1-a41f-875e2e8957b1` use the original system store prefix.
They fail compiling the new test's unqualified GrantState enum. No recovery runtime
pass is inferred. The root uses the qualified enum and adds bilateral operation
acknowledgement with a journaled retired floor, crash controls while a Grant remains
live, and a partial SQL accepted-sequence rollback control. These await the next
snapshot's scheduled gates and intentional machine schema regeneration.


### MR-2 reset, compaction and post-recovery restart — snapshot n

Snapshot n file-map `c59ff688a90b04f09352577ab68710d514be81dc50d6a38488976748ce778a29`
[retained evidence](evidence/mr2-reset-20260909n/), original system store prefix:
check `01a087a2-7878-7591-b478-777a96bae1d8`, Clippy
`01a087a2-78aa-7691-bc62-e78dbab6148b`, MSRV check
`01a087a2-78bf-77f2-98a1-acf4eef5d0e9`, schema update
`01a087a1-1d9d-7d82-adb1-8eabb1618186` passed. Fmt-write
`01a087a0-14f9-7870-b361-334f4019ba66` passed after exact-key recovery into a new
receipt; its original key is `01a087a0-14f2-7c71-91b4-78c0376585a9`. Both receipts
are retained. The root launcher now automatically chooses a fresh receipt for
such recovery while preserving the original intent; it never resubmits a new Job.

Test `01a087a2-789d-7560-9945-2d4126cd8c91` failed: 276 library and 35 CLI passed;
12 isolated passed / 3 failed, public tests not reached. Each failing fixture
successfully reconstructed native and fake-executor inventories, rejected an empty
participant snapshot, applied sealed cleanup, advanced authority epoch and ran a
waiting native canary. The subsequent restart reported authority_history_unknown
instead of the expected rollback reconciliation gate. Inspection found that epoch
rotation changed registry.epoch while its immutable installation anchor still
identified the initial epoch. Root fix records anchor_epoch separately in the
registry, preserving the anchor file and atomic single-registry epoch publication.
The strict restart assertion remains. The crash-loop fixture only reaches later
stages after this correction, so the new full compaction matrix is not yet passed.

Root also adds manager protocol SQLite transaction primitives and a durable local
fault control: outbox and Lease share the caller transaction, responses apply once,
ticket consumption survives reopen, missing/uncertain cleanup blocks Release, and
acknowledgement compacts only confirmed response history. This component is not yet
integrated with the Linux runtime or public fake-participant harness. MR-2 remains
in_progress and alpha.17 remains uninstalled.


### MR-2 reset recovery and manager transaction primitives — snapshot o passed

Snapshot o file-map `b0d7b6cd15f22f7fe9b8f840946364588b516e68688c542399f5895b1784f46d`
[passed all gates](evidence/mr2-manager-20260909o/), original system store prefix:
check `01a087aa-cc67-7d11-bd34-69cfeb000fd5`, test
`01a087aa-cc79-7341-aba2-70d2e7df8993`, Clippy
`01a087aa-cc71-7182-8fdd-18043384aa34`, MSRV check
`01a087aa-cc5e-7ee2-b198-fec0e1996c19`, fmt-write
`01a087a9-3ea2-7ea2-b041-18ca15299e94`. Counts: 277 library, 35 CLI,
15 isolated, 2 public. The reset fixtures now survive epoch rotation and restart;
partial SQL sequence rollback is detected and remains gated. Full four-boundary
coordinator acknowledgement compaction runs while preserving the Armed Grant and
its tickets. Reconstruction/manager sealing/native identity checks converge to
safe admission, without risk clearance or restoration of lost native Jobs.

The manager transaction primitive test persists actual SQLite files, rolls back
Lease/outbox and response/start-intent transactions, closes/reopens the manager,
checks exactly one durable start intent, rejects release with unanswered ticket or
missing cleanup, retains lost release acknowledgement and compacts confirmed reply
history. It is a local protocol control, not Linux runtime acceptance. Current root
adds a separate public installed-CLI fake-manager fixture using these same journal
functions against real native contenders, plus capacity-reduction/config fencing.
MR-2 remains in_progress; MR-3 remains not_started and alpha.17 is uninstalled.


### MR-2 in-flight review and public manager acceptance

Snapshot p file-map `352379f6e85e49d7e4daa4a8b562b584e4014849604a9e39404319461483e91e`
adds the public durable-manager fixture and capacity/config reduction control.
System Jobs check `01a087b1-2737-71c3-aa4a-f0a3ba5e2e4f`, test
`01a087b1-2743-7c10-b631-1480aa7b46c0`, MSRV check
`01a087b1-2740-7013-b925-62b8d6387b9b`, fmt-write
`01a087af-3686-7853-8b96-569ac420464a` succeeded. Clippy
`01a087b1-274e-79e3-b2aa-65f30f0dd5ed` is queued behind the protected review Job;
full p gate completion is not yet recorded. All use the original system store prefix.

Independent Opus review uses the opus-review skill, three distinct lenses on the
validated immutable snapshot o: history/reset fencing, manager/ticket/release, and
scheduling/accounting/bounds. Inputs are curated full source (including untracked
modules), no Claude tools, subscription OAuth through claude-current; API/provider
environment is absent. The first Job `01a087b1-239c-7aa1-ae02-15f320e94795` named
an unconfigured custom token claude2 and never started; it was canceled, its intent
retained. Corrected Job `01a087b2-1ec3-7551-a136-63a4305bba6d` requests the actual
claude2_slots=3 and cargo_slots=1 and runs through the proven WSL bootstrap under
the default daemon. External evidence directory:
`C:\Development\stillyard-mr2-opus-review-20260909o`. Findings are not yet available.
This scheduled review is not W-C2 installed-WSL consumer acceptance.

Current root also adds new-manager-store rejection while an old Grant is active,
actual UTF-8 byte accounting in the bounded manager outbox with reserved ack room,
and explicit manager journal schema version 1. Those changes are outside snapshot p.

## MR-2 review and post-review controls, snapshots p/q

All fmt-write/check/test/Clippy/MSRV-check Jobs in
[evidence p](evidence/mr2-manager-20260909p/) and
[evidence q](evidence/mr2-fencing-20260909q/) succeeded.
Snapshot p: `352379f6e85e49d7e4daa4a8b562b584e4014849604a9e39404319461483e91e`.
Snapshot q: `6ab7c68423df5ddb882a967af144704d4477f8b5e8f7af4fe9cb645a76ee4a64`.
The durable manager public fixture passes lost Arm/Release acknowledgement,
restart, exact ticket consumption and real native waiter progression. The local
start count is a SQLite fixture, not Linux user-code execution acceptance.

Three independent subscription Opus reviews completed in protected bootstrap
Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a087b2-1ec3-7551-a136-63a4305bba6d`.
[Reports and scheduled evidence](evidence/mr2-opus-review-20260909o/).
The initially queued review requested nonexistent `claude2` tokens; its own
never-started Job was canceled and corrected to `claude2_slots`. No scheduler
bypass was used. Review is not W-C2 acceptance and does not close MR-2.

## MR-2 bounded inventory, snapshot r

Snapshot `7057fc0919456376011fcff2d73e9be7906e43178a661288f510fcec7f8165c5`
passed fmt-write, intentional schema-update, check, test, Clippy and MSRV-check.
[Canonical Jobs, logs, specs and manifest](evidence/mr2-inventory-20260909r/).
Inspection now has an authenticated allocation-key cursor and byte/count bounded
pages; 300 large Unicode records require several frames and remain complete.
Oversized command outcomes roll back their business changes before a durable
`limit_exceeded` reply is committed. Reconciliation accepts up to 4096 pages with
4096 total records / 16 MiB, instead of assuming all records fit 16 pages.
Manager replay checks the saved command hash against reconstructed wire bytes.
New manager recovery changes are newer than r and remain under validation.

## MR-2 manager recovery, snapshots s/t

Snapshot s `79af76772704f8d20432fef5666467d201184b2a419b0c9bb054147eaf9c9e8b`
passed fmt-write/check/test/Clippy/MSRV-check;
[canonical evidence](evidence/mr2-recovery-20260909s/). Lost Ticket response,
coordinator retirement floor, durable inventory import without ticket synthesis,
cleanup/sealed reconciliation, then authority rotation and new local work all
pass the manager SQLite fixture. Snapshot t adds the combined public CLI scenario;
its test Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a087d9-482b-7563-8ac6-90ba2a781fcd`
passed (17 isolated tests). Other t gates are being collected.

[Verified independent-review disposition](evidence/mr2-opus-review-20260909o/codex-disposition.md)
separates reproduced gaps, fixes and unsupported reviewer assumptions. MR-2 is
still in progress; registry release headroom, clearance, remaining recovery/fault
coverage and focused review remain open. No MR-3 acceptance is inferred.

## MR-2 release barrier and expired candidate budget, snapshot u

Snapshot t `5f4927807b25648e25bd6b67301cc800fd5eb9fdc9c08accace50af1c5d12f94`
completed all five gates successfully; [full evidence](evidence/mr2-recovery-20260909t/).
Snapshot u `e0a8675e61f75578f170dfb662f82bb7536bde86d75565ea25b6cca3fb21a32c`
also passed fmt-write/check/test/Clippy/MSRV-check;
[full evidence](evidence/mr2-release-20260909u/).

A manager ReleaseBarrier now binds the original request/send clocks, session,
configuration and local provider generation. Retry cannot refresh its age. Tests
reject delayed, canceled and discontinuous-clock tickets, and readiness/freshness
loss after local commit returns a typed NeverReleased proof without calling OS
release. The consumed bit remains durable; whole-boundary cleanup is still needed.
This is a tested protocol primitive, not yet installed Linux runtime integration.
Expired unused candidates no longer occupy the active candidate budget; an Armed
grant still counts after withdrawal/refresh expiry. Same-job scheduling ties have
an explicit final Lease/key order. All changes remain uninstalled alpha.17.

Post-u root edits split the machine commit blob from the registry, preflight its
capacity before the SQL business commit and reserve room for cleanup. External
retirement duplicates are compacted only against Released tombstones in matching
continuous SQL; SQL reset remains globally gated until authority epoch rotation.
These journal changes are not covered by u and require the next scheduled gates.

## MR-2 bounded external journal, snapshot w

Snapshot w `3b45f2c13e4aa94da93bb608b43ea808c804a6a819db7844581f0891f371d2d4`
passed fmt-write/check/test/Clippy/MSRV-check;
[canonical Jobs, specs and source](evidence/mr2-journal-20260909w/).
The registry remains bounded at 4 MiB. A checksummed immutable machine commit
blob has a separate 32 MiB limit (complete <=16 MiB reconciliation inventory plus
request, outcome and retained Grant projections). Its pointer is atomically
published before SQL commit. New obligation growth stops with 256 KiB registry
headroom; the command savepoint is rolled back into durable `limit_exceeded`.
The saturated-registry test rejects another Grant but prepares and completes a
release blob larger than the former whole-file limit. Missing referenced blob
fails closed. Existing public crash boundaries pass with the separated journal.

Retirement hashes are compacted only when exact Released rows still exist in the
bound continuous coordinator SQL. They are not dropped merely because an operation
was acknowledged. SQL loss still gates both native and attached work and rotates
the authority epoch only after complete recovery. Post-w controls additionally
reject Lease-key reuse after compaction and remove unreferenced commit blobs.
Post-w root also retains original queue acceptance/sequence in the Grant inventory;
these follow-up changes are awaiting their own validation.

Snapshot preparation v was rejected because the ledger changed during copying.
Its separately scheduled fmt-write result is retained as
[discarded evidence](evidence/mr2-discarded-snapshot-20260909v/), not acceptance.
No validation build/test used v. Snapshot w was prepared afresh successfully.

## MR-2 queue history and orphan collection, snapshot x

Snapshot x `14878ed72aefe1770a427aa13c5f5141691f2c689365a8e276834dff213de2b0`
passed fmt-write, intentional schema-update, check, test, Clippy and MSRV-check;
[evidence](evidence/mr2-history-20260909x/). The schema launcher recovered an
unknown initial receipt with the same original operation; both original intent
and recovered successful receipt are retained. No duplicate schema Job was needed.
Grant snapshots retain original queue acceptance/sequence. Public reset recovery
checks both fields; restored allocations of one Job share its original owner/age.
Completed retirement hashes cannot permit a retired Lease key to be re-advertised.
Valid authority loading and durable pointer replacement collect only generated
unreferenced commit blobs; unknown history never triggers that cleanup.

Snapshot y adds ReportUncertain and a local no-start fence. It is in validation.
The focused one-reviewer Opus conceptual check of domain retirement runs as
protected system Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a087f5-7d95-7702-9784-daa219b27d66`.
Subscription OAuth and no-tool invocation were checked; its proposed design is
[explicitly unimplemented](machine-resource-clearance-design.md). Current root
adds an exact owner-visible clearance preview and stronger unused-after-commit
SQLite controls; it does not yet implement an operator retirement mutation.

## MR-2 continuation on 2026-09-10: Uncertain, preview and retirement

The preceding paragraph is historical. Snapshot y passed all six scheduled gates;
[canonical evidence](evidence/mr2-uncertain-20260909y/) identifies source
`39ddeb4192e2b9a9eec9d7e33fc66adcb438eb689b5011176fe612aede0b4c72`.
ReportUncertain retains the allocation debit and prevents local Ticket consumption;
an already in-flight Ticket response remains a cleanup obligation. The public
Windows waiter stays blocked until complete sealed release.

Preview-a passed fmt-write/schema-update/check/test/Clippy/MSRV-check with source
`b8d273f233c8b75036fc17cd3adf2c6ab848bd2511907c113c7352e17fad1f30`.
Its test Job is
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08811-8f52-7961-bb55-aa44377ee7b0`;
[all receipts, logs and specs](evidence/mr2-clearance-preview-20260910a/).
It adds an exact owner-visible retirement preview and tests a durable consumed
Ticket whose OS release was prevented after commit. Its empty-boundary cleanup
does not reset the consumed bit or authorize another start.

Retirement snapshot b `61db2df5115b30337c13e4c8da5fe023dd8e67dbe2e6c3b93c80680040b63260`
passed fmt-write/schema-update/check/test/Clippy/MSRV-check;
[canonical evidence](evidence/mr2-retirement-20260910b/). Test Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08817-1fc5-73f0-bcc4-1abf766973ef`
passed 287 library, 35 CLI, 19 isolated and 2 public tests. It contains public
domain retirement, four crash boundaries, interrupted-pairing
abandonment, old-writer fencing and coordinator-reset controls. The mutation is
uninstalled; passing this slice does not close MR-2. It records the actual Windows peer SID and an
immutable full-inventory audit; SQL risk clearance never fabricates cleanup proof.
The retired domain/installation/store identities remain fenced across epochs.
Other participants and native obligations must retain their own recovery path.

The focused conceptual Opus Job completed successfully; its
[verdict](evidence/mr2-clearance-review-20260909a/reviewer.md) informed the separate
pending retirement journal and whole-registration fence. Final implementation
review, bounded history maintenance, same-store rollback remediation and remaining
MR-2 fault controls remain open. MR-3 runtime and live consumers are not run.

Two focused Opus reviewers completed under protected default Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0881a-682a-7893-977d-71f151da06dc`.
Their exact curated input is in
`C:\Development\stillyard-mr2-retirement-review-20260910c`; subscription Max and
absence of provider-billing environment were checked. Root subsequently adds a
restricted same-UUID rollback repair: native Jobs must all be final, native SQL
boundaries resolved, all manager inventories reconciled or explicitly retired,
and every predecessor/native external permission covered by OS proof. Native
Jobs are preserved; normal explicit cancellation is required for unfinished
history. This path and the live unrelated-native retirement control passed in d.

Snapshot d passed fmt-write/check/test/Clippy/MSRV-check with source
`9885ea6886a44217ad9a12d91a66c622d25a0d01071da01a123aff26328b5983`;
test Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08824-cce5-7c81-81bb-20b0f4a97d50`
passed 288 library, 35 CLI, 19 isolated and 2 public tests. The separate c snapshot
only ran its scheduled formatter; d superseded it before build/test submission.

The two-reviewer [verified disposition](evidence/mr2-retirement-review-20260910c/codex-disposition.md)
separates findings from omitted-context hypotheses. Startup retirement recovery
and owner-only pipe ACLs were already present. Confirmed corrections in e include
reserving late hold-release metadata, observable storage headroom, keeping
divergent SQL retirement inspectable behind a reset gate, exact original receipt
diagnostics, initialization-time retirement schema, and a dangling-parent check.
New controls exercise many paired participants and maximum escaped reasons,
another manager with issued rights, automatic startup recovery before replay,
and explicit repair of a post-fence divergent SQL projection. A later root-only
control additionally restores an exact missing audit through daemon restarts.
These follow-ups are under validation, not installed.

Snapshot e passed fmt-write/schema-update/check/test/Clippy/MSRV-check;
source `b20d85ef7ede5f988adcddccae3eef8cc908ea9d31702c9e3d12789ab550cfd2` and
[canonical evidence](evidence/mr2-reviewed-20260910e/). Test Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08830-aaf6-7190-b178-93f33f8ca842`
passed 289 library, 35 CLI, 19 isolated and 2 public tests. Many-participant
metadata reservation, post-fence divergent-SQL repair, original receipt error
diagnostics, automatic recovery before clearance replay, pending-pairing plus
whole-SQL reset and another manager's unchanged issued rights all passed.
The public missing-audit restoration control is the only subsequent Rust change;
it is queued for the final MR-2 validation snapshot alongside phase-level
MSRV tests and release build. No default daemon was replaced in these slices.


### MR-2 final Windows gates (2026-09-10, f)

All Jobs below completed `succeeded` on the installed default alpha.16 daemon.
The complete Job ID uses store prefix `01a05f1f-858c-7880-8c15-d55875da9e6b~`.
[Canonical receipts/status/logs and exact submitted specs](evidence/mr2-final-20260910f/)
cover final file-map `8ba4d5c8034551d15dd8ec5c843b2d9eef2da054911cc86b7ded1a7cd5ac8e74`.

| Job | Entity UUID | Outcome |
|---|---|---|
| fmt-write | 01a08834-bf82-76a3-a7ea-30b242b21bfc | succeeded |
| check | 01a08835-84b8-7532-a209-095dab3f1679 | succeeded |
| test | 01a08835-84cd-7662-a3fe-47935513878e | succeeded |
| clippy | 01a08835-84ab-7f91-8d9d-4fc3a57c4ad6 | succeeded |
| msrv-check | 01a08835-84bc-7510-ad3a-49e6920df3e3 | succeeded |
| msrv-test | 01a08835-84d6-7462-9fd3-50a0b6810ff1 | succeeded |
| build-release | 01a08835-8493-78f1-945f-645fe84fd739 | succeeded |

Stable and Rust 1.85 each passed 289 library, 35 CLI, 19 isolated-daemon and
2 public API tests. Ignored helpers/platform-specific bootstrap tests are not
counted as passed. The new public retirement variant removes the exact durable
audit file after the external journal boundary: restart stays inspectable/Unknown,
recovery cannot free the queued native token, and restoring that exact file plus
restart completes the journal automatically before any retirement replay.

Release alpha.17 candidate SHA-256:
`d2ddd1d39d910dc26ad43893b17a777312e30b9962c1ff283dded7fbf7719688`.
Installer preparation completed only; installed daemon has not changed.
Final focused Opus Job `01a08836-a0ad-7790-a465-71c80624eb1b` and protected Linux
check Job `01a08837-dfad-7fb3-bae8-748f76f56305` were accepted with the same store
prefix. Their results are pending and are not acceptance claims.


### MR-2 final review and follow-up g

Final-f protected Linux check
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08837-dfad-7fb3-bae8-748f76f56305`
succeeded with no changed source files; [canonical evidence](evidence/mr2-linux-20260910f/).
This is portable shared code, not a Linux runtime acceptance claim.

Final focused Opus review Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08836-a0ad-7790-a465-71c80624eb1b`
succeeded in 697.084 seconds. It confirmed both reservation arithmetic and stalled
retirement repair. [Verified disposition](evidence/mr2-final-review-20260910f/codex-disposition.md)
distinguishes verified liveness/diagnostic issues from omitted-context questions.
The current installed alpha.16 registry has 24,646 encoded bytes, zero live holds
and zero participants: the legacy reservation-overflow issue is not present on
this upgrade target. This read-only observation does not release any obligation.

Follow-up g file-map `dae98b9d40ea8370bb4957b240cd037374ed56546a7d553a9f52b263eebd2281`
adds a new-submission gate during same-UUID repair (existing receipt replay remains),
typed pending-retirement diagnostics, a visible legacy reservation warning and
public reconstruction/reconnect/sealed-release evidence for a surviving manager
with issued rights. Its scheduled gates are running; final-f is no longer the
current source candidate for installation. Windows remains installed alpha.16.


g MSRV-test Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0884c-ba44-7160-a5e5-b00ef4db9d6c`
failed in the extended retirement fixture at its old one-Grant assertion: observed
2 because the newly enabled surviving peer also owns a Grant. All 290 library
tests and the separate same-UUID submission-gate control passed. This is an
incomplete test run, not acceptance of the surviving-peer reconstruction path.
The h snapshot corrects the expected debit to include that peer; production
source is unchanged from g. Its formatting/test follow-up is scheduled.


h file-map `4d119e5cab94f2a9c675a3bc388df56e8e1f5e52952910b8393b24ac55b9b898`: stable
test Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08853-d30a-7e20-a947-136441d38e99`
succeeded (290 library, 35 CLI, 19 isolated, 2 public). The surviving-peer
reconstruction path now passed through completion: exact preview and Ticket
identity fences survive, authenticated reconnect rejects fresh work under the
reset gate, then its own complete sealed snapshot releases rights without peer
risk retirement. Final h MSRV, release and installation results remain pending.


### MR-2 phase exit: installed alpha.17 (2026-09-10)

All seven Windows gates for accepted-h succeeded, including stable and Rust 1.85
tests (290 library, 35 CLI, 19 isolated, 2 public). The seven complete Job IDs
and immutable specs are in [accepted-h evidence](evidence/mr2-accepted-20260910h/).
Matched protected Linux check Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08854-5b8b-7800-8fde-bbf7884e6661`
succeeded with unchanged source bytes.

The native installer applied release Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08853-d31b-7d73-9c54-5dcac8879b52`,
SHA-256 `823f9db65db24fa18dbd3e0d75fb2bf631048371e174d6a37ce28ffa6a297af3`,
under its SQLite admission barrier: 849 historical Jobs, zero granted Leases,
zero blocking containments. Existing store UUID and authority were preserved.
The daemon now runs from the canonical installed path, PID 53324, generation
`01a0885a-e10e-7c33-90ec-7278525c6702`; authority has no blocker. No Cargo target
executable is serving the daemon.

Installed ordinary Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0885b-39b4-78c3-816c-a3b3e4e68c76`
succeeded, observed alpha.17 and its actual managed parent, and ended with its
allocation Released. [Upgrade/barrier and canonical Job evidence](evidence/mr2-installed-20260910h/).

**MR-2 complete; MR-3 in_progress.** WSL runtime installation, persistent bridge,
shared live queue, observation and W-C1..4 acceptance remain required.


### MR-3 first runtime slice: Linux identity and client IPC

Working package advances to uninstalled alpha.18 / IPC 23; the installed Windows
daemon remains accepted alpha.17. New typed Linux identity carries installation
host, boot, PID, kernel start ticks, PID namespace inode and UID. Unix client
authentication uses SO_PEERCRED plus SO_PEERPIDFD (no PID-only fallback), checks
the live process executable by device/inode, and bounds connection/frame I/O by
the caller deadline. Linux receipt publication flushes its parent directory.
Socket mode/image/deadline and kernel identity controls are added.

The protected bootstrap launcher now supports Linux test, Clippy and MSRV gates
with optional bounded test-name filters. All compilation remains a default native
Stillyard Job until the installed attached WSL manager passes acceptance. Linux
daemon/startup and Invocation execution are still UnsupportedPlatform; these new
primitives do not advertise runtime containment. First scheduled validation is
pending.


MR-3 identity/client snapshot a file-map
`31270ebae21fd10d2e6b451549f7494c4c77d65a663bce31839b0e9485a61056` passed
Windows fmt/schema/check/test/Clippy/MSRV-check;
[canonical native evidence](evidence/mr3-linux-ipc-20260910a/).
The following protected Linux default Jobs also succeeded with unchanged source:

| Gate | Complete Job ID | Outcome |
|---|---|---|
| check | 01a05f1f-858c-7880-8c15-d55875da9e6b~01a08868-e2dd-7642-9829-6473f30629ee | succeeded |
| test filter linux | 01a05f1f-858c-7880-8c15-d55875da9e6b~01a08868-e766-7df1-9615-e15088c6024d | 5 passed |
| Rust 1.85 test filter linux | 01a05f1f-858c-7880-8c15-d55875da9e6b~01a08869-7a31-7b60-b7f2-d8eae21f2de7 | 5 passed |

[Canonical Linux evidence and exact specs](evidence/mr3-linux-ipc-bootstrap-20260910a/)
prove the typed identity, comm parser, kernel-pinned peer/image check, socket mode
rejection and request deadline. The filtered-out tests were not run. The snapshot
does not yet run a Linux daemon or any Linux Invocation. Next slice is the server
transport and singleton ownership, retaining unavailable execution until its
actual cgroup backend is connected.

### MR-3 Unix server slice b/c (2026-09-10)

Linux now shares the RPC/reactor path behind an owner-only Unix listener. The
connection peer is kernel-pinned before frame input; the endpoint lease is held
independently of the store lock. Stale socket reclamation requires refusal and an
unchanged inode; active listeners, foreign files and unsafe parents are rejected.
Workers and frame I/O have finite bounds. New isolated Linux subjects cover
endpoint/store contention, two independent stores, crash/restart and queued Job
history. Startup still refuses execution capability pending the cgroup backend.

b file-map `6ed0f8b7869a527ae2be3493c4ab34f0c5df54d381e4d933de4dce7e521983a7`
passed native fmt/check/test/Clippy; [native canonical evidence](evidence/mr3-linux-server-20260910b/).
Linux test Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08874-21f7-7d60-93a9-8e7adfd562ef`
failed to compile because `store::open_lock` was still exported only on Windows;
no Linux server test ran. [Failure evidence](evidence/mr3-linux-server-bootstrap-20260910b/).
c removes that cfg restriction, creates new Linux store directories with mode
0700 and adds a live foreign-listener negative control. Its file-map is
`a04f1f2e413266c5daffc53e3c978304bdea737508d44a2d951ca3a623f81666`;
scheduled validation is in progress. Installed Windows remains alpha.17.

c compiled and passed five Linux identity/client controls, but its first daemon
subject failed because tempfile selected tmpfs `/tmp`, outside the required
ext4 store boundary. Linux test Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08878-0730-7550-b5b8-4be2f0edc5d2`
is failed; the unsafe-path test's apparent pass is not accepted, since it could
also result from that blanket startup failure. d uses an ext4 fixture and a
successful server control before negative cases. c native fmt/check passed.
[Canonical c native](evidence/mr3-linux-server-20260910c/) and
[Linux failure evidence](evidence/mr3-linux-server-bootstrap-20260910c/).

d file-map `93592ae5e9c82ba747369ab494b47324e6d1f1b3b5549a35d8b58d923526e343`
also includes explicit bounded nested bootstrap delegation and its ignored live
Windows/WSL acceptance fixture. False/default BootstrapWork serialization remains
unchanged to preserve historical request hashes. Native stable/MSRV/release,
bootstrap fault tests and the Linux server tests are queued as system Jobs.
The in-progress cgroup component in the working tree is newer than d and is not
included in these gate results; it does not yet enable Invocation execution.

d's native fmt/schema/check/test/Clippy/MSRV-check/MSRV-test/release passed;
Linux server test Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0887e-4563-7b63-8ffd-dbf1564ece3b`
passed five identity/client controls and both live isolated server controls.
[Native d evidence](evidence/mr3-linux-server-20260910d/) and
[Linux d evidence](evidence/mr3-linux-server-bootstrap-20260910d/).
The explicit bootstrap gate failed its old post-reset `authority_held` assertion:
MR-2 now correctly reports `authority_reconciliation_required`. Its nested
cgroup and installer cases passed, but the entire gate is failed. e updates the
old reset fixture to require both sealed Linux cleanup AND explicit coordinator
history reconstruction before the canary may run.

### MR-3 installed server/test infrastructure e (2026-09-10)

e file-map `057a0dd0d44fd3dcf1871d0e39a0dc425fe3a8a79d067314bdd013c793f8b4f9`
passed native fmt/check/test/Clippy/MSRV-check/MSRV-test/release and all three
explicit bootstrap fault/installation cases. The exact Jobs/specs are retained
in [native e evidence](evidence/mr3-cgroup-20260910e/).
Protected Linux test Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08886-5055-7050-ac28-eb837fd124ad`
passed the selected Linux controls; live delegated tests were still ignored in
that run. [Linux source/component evidence](evidence/mr3-cgroup-bootstrap-20260910e/).

The installer applied native release Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08886-5162-7303-a177-2e3cbcf02f7d`,
SHA-256 `60b595c07732199f891f8fe76f1f684e4c13c5e599bff7fff06be3bb0aa246f4`,
under a SQLite admission barrier: 886 historical Jobs, no granted Leases or
blocking containments, three historically cleared incidents. Installed default
Windows is now alpha.18 / IPC 23, PID 49740, generation
`01a0888c-4082-7550-96a3-f1288b6210d0`; original store UUID and authority retained,
no authority blocker. [Installation evidence](evidence/mr3-installed-20260910e/).

After installation, protected nested-delegation test Jobs
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0888c-e183-7d71-b0b6-cf9f75fd92bd` (stable) and
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0888c-e18f-71c3-b8fe-2a08f7e0d6b1` (Rust 1.85)
each passed both real cgroup controls: exact identity/reopen/removal and root
exit while a live descendant keeps `populated=1`, followed by recursive kill and
seal. Missing/replaced paths reject inspection; ordinary files cannot spoof
cgroup v2. Linux Clippy Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0888c-e642-7952-ad49-9d8e2b008894`
succeeded on the same source. [Canonical live cgroup evidence](evidence/mr3-cgroup-live-20260910e/).
These component tests do not close M-A08: the actual Invocation lifecycle,
durable seal/Grant integration and installed WSL executor remain incomplete.

The working tree is newer than e: a trusted-stub/kernel-exec-stop component is
under implementation and has no gate result yet. The PID/user namespace prevents
a managed client from inspecting an ancestor server via ordinary /proc; its
namespace-aware server authentication must be explicit before enabling runtime.

The installed native canary Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0888d-81b7-7491-9525-5fbaef324519`
succeeded, reporting alpha.18 and its actual managed parent. Its complete receipt,
spec, status and canonical logs are in installed-e evidence.

Next working slice uses a trusted helper inside the cgroup/interop profile,
`PTRACE_O_TRACEEXEC` to stop after kernel exec before any requested user/loader
entry, exact loaded ELF/interpreter and cwd checks, and sealed memfd bytes for
shebang scripts. Its private one-shot control channel authenticates the
namespace-hidden parent using a per-launch HMAC challenge plus owner/pidfd
liveness; this is not a PID-only authentication fallback. The ordinary public
client still requires exact executable identity; managed namespace client
integration is not supplied by this helper. No Invocation or Grant is enabled
by the primitive. f validation will check no pre-release marker, refusal of a
second release, sealed script byte identity and interop denial on real processes.
Kernel references: [ptrace exec stop](https://man7.org/linux/man-pages/man2/ptrace.2.html),
[execveat script fd semantics](https://man7.org/linux/man-pages/man2/execveat.2.html).

f file-map `2cfc168e7f5c8fc335806439e5b9e46c625e12949eba0d80c577ab420cf29808`:
protected exec-barrier Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08894-8cad-7911-9d96-7b8e5288bcc5`
compiled but failed with ECHILD during preparation. The fixture ran the helper as
a libtest worker, while SO_PEERCRED/ptrace pinned the process leader; exec from
that other thread invalidates this assumption. g invokes the actual CLI helper
and rejects any helper that is not the sole process-leader thread. Also, script
snapshotting moves to a private read-only bind at the requested canonical path:
fexecve of a script would change interpreter argv/__file__ to an fd path and
break relative script resources. g's real test now requires both unchanged bytes
and preserved __file__. Script symlinks require caller canonicalization; nested
shebang interpreters remain explicitly unsupported. Runtime/Grant integration
is still disabled. Neither f failure is accepted as an Invocation safety pass.


g file-map `2b1e2474edffe8542c2f2fa252756d49aebd34036a017434ff56e0e0a3470c23`
passed scheduled fmt and protected exec-barrier controls on stable and Rust 1.85:
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0889a-9b05-7a52-8a76-e6422d6d83cf` and
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0889b-dd32-7253-a040-8c2da4b4b6ca`.
Linux Clippy Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0889b-dd25-7b31-8d07-675df670d6de`
also passed. [Canonical live evidence](evidence/mr3-exec-barrier-live-20260910g/)
and [format evidence](evidence/mr3-exec-barrier-20260910g/).
These controls exercise actual CLI helper, ELF/script exec stop, single release,
sealed script bytes at the original path and interop denial. Runtime integration
is still pending. h adds loader-constructor, rejected-image and abandoned-release
negative controls; no results for h are claimed yet.


h file-map `d4e09861168527c9d17da6e715fce7b8c3308e0cda6624a5346a959df48c9cde`
passed protected exec-barrier tests on stable and Rust 1.85, including a real
LD_PRELOAD constructor, rejected loaded image and abandoned pre-release launch.
Jobs `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088a3-b321-71c3-ad6d-a4db47a94829`
and `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088a3-b32a-7c83-9b94-aece89f3b97f`;
Linux Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088a3-b343-7a83-819e-e6761e2c2a8a` passed.
[Canonical live evidence](evidence/mr3-exec-barrier-live-20260910h/),
[format evidence](evidence/mr3-exec-barrier-20260910h/).
The following working slice adds additive typed Invocation process records and
reset-independent owner-only executor history. It writes creation before cgroup
birth, root identity before ticket use, and release intent before kernel release.
Cleanup is a persisted exact-boundary seal, not root absence. Missing/corrupt
history, foreign store, duplicate launch and removal-before-seal crash fail closed.
This slice is not yet validated or connected to the installed attached runtime.


i file-map `c4fa2c434ec2a284acbed87e0277b79abd48ad0b3095185ac216296a68897dc5`
passed native fmt/check/test/Clippy and Linux Clippy. Protected live journal Jobs
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088ab-5e39-7d23-b9c8-aa6271493da7`
and `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088ab-5e2f-7bb2-b1e8-8520988dcc1f`
failed before journal initialization because the fixture parent had broad mode.
This is not accepted as a cleanup control. j explicitly makes the ext4 fixture
parent 0700, preserves the production check and binds the ticket digest to the
whole prepared root/boundary record. [Native i evidence](evidence/mr3-executor-journal-20260910i/)
and [Linux i failure/Clippy evidence](evidence/mr3-executor-journal-live-20260910i/).
Working alpha.19 additionally contains the persistent bridge and typed-identity
view controls. Installed Windows remains accepted alpha.18; no WSL runtime is
installed yet. j gates will validate these changes before runtime connection.


j file-map `fc91a8ae0b12426b36c62e558069825ce523d043aa8c9bebafb49f4b2ea1abb6`
passed native fmt/schema/test/Clippy/MSRV-check, including persistent native bridge
request correlation, ordinary error recovery and EOF without authority change.
[Native evidence](evidence/mr3-bridge-journal-20260910j/). The first schema Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088b2-ae61-74d0-8c92-c14b1747e70c`
is retained but not used for acceptance: target cache copying was not ordered
strictly before submission. After it finished, its target was moved aside and
replaced from a completed cache before resubmission. Accepted schema Job is
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088b5-2608-7230-a151-d77fb3471932`.

Protected live journal tests passed on stable and Rust 1.85:
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088b7-4858-7e11-add5-6d2a77aedc25` and
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088b7-4862-7e00-b657-ffeeba8a4d33`.
They reopen a live executor obligation, kill/seal the exact boundary, reject
reusing the Invocation, and retain uncertainty after removal before seal publication.
Linux Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088b7-487f-7621-a257-8ed7d2af6677`
also passed; [live evidence](evidence/mr3-bridge-journal-live-20260910j/).
Normal Linux-filter Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088b7-4d5a-7201-819e-873c6ed0f981`
passed typed identity views, corrupt/missing journal rejection, bridge pipe deadlines
and existing Linux server/identity controls; [evidence](evidence/mr3-bridge-journal-normal-20260910j/).

The working tree is newer than j: alpha.19 / IPC 24 adds launch-pinned Ed25519
server attestation for clients in a private PID namespace and connects the
Linux containment registry/reconciler to durable cgroup seals. Shared-key MAC
server authentication was considered but not used: a client must not receive a
server signing secret. New tests include a real nested namespace client and an
outside proxy negative control. These changes await k scheduled validation.
Attached candidate/Grant lifecycle, installation and W-C acceptance remain pending.


k file-map `b32eae1a1b7b323e58b943d54260a539929b2e12cf7e05e65393fdc947a1f312`
passed native fmt/schema/test/Clippy/MSRV-check. [Native evidence](evidence/mr3-namespace-auth-20260910k/).
Real namespace client plus outside-cgroup proxy rejection passed on stable
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088c7-b528-7df1-99fb-8de50df231e6`
and Rust 1.85 `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088c8-8279-73d2-b1da-290111f8cd83`;
[canonical evidence](evidence/mr3-namespace-auth-live-20260910k/).
Registry/seal live control passed Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088c8-8798-7601-807e-a01876745785`;
[evidence](evidence/mr3-namespace-registry-live-20260910k/).
Normal Linux-filter Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088c9-cbd7-7f93-9102-ce5112eb9c97`
passed [evidence](evidence/mr3-namespace-auth-normal-20260910k/). Linux compilation
reported an unused attestation constructor pending runtime installation; this
slice does not claim Linux Clippy. l explicitly marks that temporary boundary.

The working tree now binds primary/probe admission to durable attached candidates
and protects all Lease insert/release paths with SQLite guards. Pending Arm does
not create a local Lease; pending Release acknowledgement retains its debit.
The new lifecycle control awaits scheduled l validation. This is not an installed
WSL runtime or W-C acceptance; Windows alpha.18 remains installed.


l file-map `39615707ce4061ce75f58301b21107be21cee6f770300065101cf951157dd21f`
failed test compilation: the new protocol fixture omitted required Reply.coordinator_revision.
[Native failure evidence](evidence/mr3-attached-lifecycle-20260910l/),
[Linux failure evidence](evidence/mr3-attached-lifecycle-linux-20260910l/).
The fixture was corrected in m; no l runtime pass is claimed.

m file-map `cf7761291897769f18e5e438526a33b5a9881aafbd7d936fc69f3bc0fdf0cd94`
passed native fmt/test/Clippy [evidence](evidence/mr3-attached-lifecycle-20260910m/).
Attached lifecycle Store control passed on Linux stable
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088d3-a11a-79f0-82db-7a24bd729c88`
and Rust 1.85 `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088d5-3d84-7c43-b0ff-779468b8daf8`.
It covers multiple durable candidates, no Lease before Arm, exact prepared IDs,
and retained local debit until Release acknowledgement. This is a pure Store
control with synthetic protocol replies, not cross-OS/live-consumer acceptance.
[Linux evidence](evidence/mr3-attached-lifecycle-linux-20260910m/).
Linux Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088d5-3d75-73b0-8c7c-d01aeeec31d3`
failed items_after_test_module in the registry. n moves that test module after
production items. n also adds reset-independent pairing-anchor checks on open
and admission, rejects standalone fallback for an attached store, and makes
replayed acknowledgements idempotent at the local lifecycle layer. These n
changes await scheduled gates. Native/WSL runtime installation remains pending.


n file-map `4d25930688523c02dea336ea0a3b89264d9e62780a8d9a2add0750d19ca75593`
passed native fmt/check/test/Clippy/MSRV-check/MSRV-test/release, including
attached Store guards and all existing native protocol/runtime controls.
[Native gates](evidence/mr3-attached-anchor-20260910n/). Linux attached lifecycle
and missing/corrupt/replaced pairing/store controls passed stable
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088da-2f2e-7f52-ab3f-9c466248148f`
and Rust 1.85 `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088de-5f33-7d11-a4f0-8dd3d2d5857f`.
Linux Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088da-33b3-7753-a9a5-cb163ec89553`
also passed; [evidence](evidence/mr3-attached-anchor-linux-20260910n/).

Release Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088df-57d1-7b40-894a-3729ebbb9097`
produced the installed Windows alpha.19 / IPC 24 binary. Upgrade held the SQLite
admission barrier with 948 Jobs, zero granted Leases, zero blocking containments
and three historically cleared incidents; original store/authority were retained.
[Installation evidence](evidence/mr3-installed-20260910n/). Installed canary and
real WSL-to-native persistent bridge checks are next. WSL runtime is not installed.

The root working tree is newer than n: it adds a persistent attached outbox driver,
handshake/inventory recovery, transient per-Invocation release barriers, and
explicit journal/configuration installation helpers. These newer changes have
not been gated or enabled in daemon startup. Ready-candidate offer/Arm driving,
Linux Invocation lifecycle, installation and W-C acceptance remain pending.


Installed native canary and three-request persistent bridge test passed Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a088e4-ec84-7940-8e98-1ae6101714d2`.
It verified alpha.19, exact managed parent, request correlation, recovery after
an ordinary participant error, EOF exit and unchanged authority epoch. This
ran on the real installed Windows daemon; it is not a WSL-initiated bridge pass.
[Canonical installed evidence](evidence/mr3-installed-20260910n/).

Storage housekeeping removed only completed Linux target caches for g/h/i/j/k;
source snapshots and their canonical evidence are retained. Linux n/m/l caches
remain available for subsequent scheduled builds. No running daemon image or
user checkout was removed. Next source snapshot o validates the new attached
driver and installation helpers before daemon/runtime integration.


o file-map `224d5a56c8f38c6cf5ea8dd8f5536a028b1059939b4c0f4b97fe9ca11e1fc02d`
passed scheduled fmt and protected Linux check/Clippy/MSRV-check for the attached
driver and explicit journal/configuration installation helpers.
[Format evidence](evidence/mr3-attached-driver-20260910o/),
[Linux gates](evidence/mr3-attached-driver-linux-20260910o/).
This is compilation evidence; the driver has not connected an installed WSL
manager. p adds ready-candidate refresh/offer/Arm driving, bounded outbox
acknowledgement, unused-right retirement, automatic sealed Release enqueue,
and local release application after inventory reconciliation. It also avoids
committing unbound planned Attempts on disconnected preparation. p is unvalidated.
Next: p gates, then consume Invocation tickets in the same local transaction
as lifecycle authorization and connect the real Linux executor to that barrier.


p file-map `3c24fe34a6b44c3abbb859acb7a0f74f59fb7321f4f5c3df4c6e6a2cdb1f7bbb`
passed native test `01a05f1f-858c-7880-8c15-d55875da9e6b~01a088f4-ae09-76c3-b925-63ef9ddf8279`
and Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08965-a603-71e1-900c-4014998e033e`.
Linux attached admission passed stable `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08965-a152-7783-acbd-5c0420180342`
and MSRV `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08966-dda0-7bc1-b0fb-b5c16cd894b0`;
Linux Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08966-dd96-7f82-8c61-e617ed5dd8f5` passed.
[Native evidence](evidence/mr3-attached-queue-20260910p/),
[Linux evidence](evidence/mr3-attached-queue-20260910p-linux/).
These controls use synthetic protocol replies; installed WSL and W-C acceptance remain pending.
q now gates atomic Ticket consumption with local Invocation authorization,
including rollback for missing/altered Tickets and rejection of duplicate consumption.
Next runtime slice must also handle deferred starts whose local Lease remains
retained until coordinator Release acknowledgement; no local-debit shortcut is allowed.


q file-map `39d52ab09c0348b360889f57daf99b023bcad4839be912f18fac8487d93fb7c4`
passed native test `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08968-c890-7e30-96d9-35c83018443f`
and Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08969-d6e0-7703-8e7d-b8aa6814c338`.
Linux atomic attached admission passed stable `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08969-d17f-7131-a14c-15ad021558d1`
and MSRV `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0896b-dc0c-72b3-a2ce-afa52a5f8026`;
Linux Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0896b-dc03-7081-a7bb-d359612d4b36` passed.
[Native evidence](evidence/mr3-attached-ticket-20260910q/),
[Linux evidence](evidence/mr3-attached-ticket-20260910q-linux/).
Root now adds Linux Invocation runtime using the shared lifecycle, immutable
stdin snapshots, shared canonical log drains, Ticket freshness/consumption
through kernel exec release, and journal-sealed whole-cgroup cleanup. A new
protected live-kernel control uses synthetic coordinator replies and a real
root that exits while its descendant stays alive. This is not installed WSL
or live cross-OS acceptance. r validation is next; daemon startup/driver worker,
observation providers, installation and W-C rounds remain pending.


r file-map `62444f77c222096be878078a996f58747b51de763db801cc72ac9ddc0979b295`
passed native regression tests. Protected Linux runtime Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08975-7fa1-7a41-9b92-909296e8bf95`
compiled but FAILED in the synthetic peer fixture before creating a Linux
Invocation: the single-candidate variant tried to acknowledge two candidates.
[Native evidence](evidence/mr3-attached-runtime-20260910r/),
[Failed Linux evidence](evidence/mr3-attached-runtime-20260910r-linux/).
Root fixes that fixture and adds explicit attached daemon startup, a persistent
bridge worker with cancellation fencing/reconnect, and event-driven outbox wake.
Only an installed attachment selects Linux startup identity; ordinary unpaired
Linux opens remain incapable of execution. These changes await s gates.
Reconciliation-to-Ticket cleanup import, public installed diagnostics, platform
observation and the actual Windows/WSL installation still require integration.
Completed Linux l/m/o target caches were removed; their source and evidence remain.


s file-map `93ac88c6dea59d17379c51d326a74898d81b3a9fca423070f872991d1472f691`
passed native test `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08978-ecf6-7e40-881e-6d6a78fa1fc2`
and Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08979-fa10-7040-a2b9-ecfbbd36468c`.
Linux gates found an installation-loader visibility error in the new Store startup
branch (E0603); the kernel scenario has not passed. Root widens that loader only
to the Store module and adds durable-seal import on startup/reconciliation,
cleanup of issued-but-unreceived Tickets, and local plan synchronization with
complete coordinator inventory. t gates are next. This is still MR-3 in progress;
no installed WSL or consumer acceptance is claimed.


t file-map `bb33da4350b4f2da5188ea91d6da3e42a8e527e69615e47ffd924bf0191241ef`
failed native/Linux compilation because the inventory-to-plan match omitted
`GrantState::Expired`. Root now explicitly leaves Offered/Expired rights without
a Lease-release effect. Native test/Clippy Jobs:
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0897e-b643-75e0-8483-efa979f467d7`,
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08980-d254-7353-a843-333cce8e7bc0`.
Linux runtime/Clippy Jobs:
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08980-cd8b-7210-b2db-3f34e4ea4685`,
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08980-cd94-7e60-91c0-91559a94eddd`.
Root additionally implements documented Linux CPU/memory/disk/process evidence
and a 30-second idle bridge wait (active candidates retain 100-ms servicing).
The measured idle budget is still unverified. u compilation gates precede
another live runtime attempt. Installation and W-C acceptance remain pending.
Completed native l/m/o/p/q/r Cargo caches were removed; all source/evidence and
installed n release artifacts remain. No Store, queued Job or daemon was removed.

u file-map `13d366ab35df22183479ea1ff0642d458c01acf0b42f557e2c540dd1081eb4d3`
passed native test/Clippy and Linux Clippy/MSRV-check. Protected live runtime
Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08988-0427-7da0-8eb1-603a324f4e03`
PASSED: a real Linux root exited leaving its child alive, whole-cgroup cleanup
was sealed durably, and the consumed Ticket's local Lease remained held until
Release acknowledgement. This uses synthetic coordinator replies: it is partial
M-A08 evidence, not an installed Windows/WSL connection or W-C acceptance.
[Native evidence](evidence/mr3-linux-observation-20260910u/),
[Linux evidence](evidence/mr3-linux-observation-20260910u-linux/).
Linux observation Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08988-0423-7c52-86cd-10c2d958a13c`
passed 12 controls and failed a Windows-specific provider-name expectation.
Full Linux baseline Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08989-ef62-7a53-ae80-1e2de3be430a`
FAILED (88 passed, 174 failed, 11 ignored): most model fixtures used tmpfs /tmp
for a durable Store; the existing Linux policy-root fallback also identified
paths rather than filesystem objects. Root ports durable fixtures to ext4 with
explicit model identities, fixes platform-dependent display/coverage fixtures,
and adds Linux object identity (device/inode/birth time), hard-link conflict and
symlink-escape controls. Runtime also rechecks local RAM/observation readiness
under the provider barrier for every role before kernel release. v gates next.
MR-3 remains in progress; installed WSL, integrated fault scenarios and consumer
rounds remain pending. Ordinary unpaired Linux opens remain fail-closed.

v fmt Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08991-da44-7563-91e7-57d3f5464a73`
failed on a missing cfg-expression block in a model helper; v2 fixes its syntax.
v2 file-map `1dd658235e283686188a94796412156df7baefc34c3ad76f24473332356bc3b5`
passed native regression `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08993-4c60-7c73-a87e-948d856ea609`
and Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08994-19fb-7370-b445-6eea3ec5f378`.
Full Linux Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08994-153a-76f0-a5a4-03b1fe8bf03b`
FAILED with 246 passed, 17 failed, 11 ignored. Object replacement, hard-link
identity and symlink escape controls passed. Remaining failures exposed model
probe paths/case-insensitive alias fixtures, containment-strength reporting,
and a trailing separator mismatch in missing-path fences. Root fixes these and
adds explicit `wsl-install` setup under both endpoint/Store locks, owner-only
configuration input, installed systemd default auto-start, and real cancel/timeout
runtime controls. Linux session-survival diagnostics now report unverified rather
than an unconditional pass. w gates precede installation; W-C remains not run.

w file-map `dcbe2d30ccfff98d393b86e8939be0b3d77613dee6437ffa21c5aa8c908e0089`
passed full Linux stable regression Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0899a-9e8d-7603-b41f-2e7eb75e7f11`.
Protected live Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0899a-a2d8-7550-b220-910719765cf3`
passed root-exit, cancel and timeout controls with live descendants, sealed cgroup
cleanup and local Lease retention until Release acknowledgement. The coordinator
in these three controls is synthetic; actual installed connection and cleanup
failure/reconnect cases remain pending. Native Clippy
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0899a-ac95-79c2-b19e-06e19f8f36e1`
and Linux Clippy `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0899c-7382-7cc0-a26c-783d845ca1b2`
passed; full Linux MSRV-test is running as
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a0899c-7813-7303-a1d3-0ccf4051d24b`.
[Native evidence](evidence/mr3-wsl-setup-20260910w/),
[Linux evidence](evidence/mr3-wsl-setup-20260910w-linux/).
Root adds IPC-25/alpha.20 coordinator scheduling observations and actual attached
Grant/Lease IDs in existing allocation views. Observations are bounded, retain
capture time and carry disconnected/stale blockers; they never authorize release.
Next: x schema/format and same-source platform gates, release builds, then explicit
installed pairing. MR-3 and all W-C rounds remain open.

w full Linux MSRV-test also PASSED (same source, canonical evidence retained).
x file-map `31ad15da0250f2f2e67ec9444d6f05392626aa889dcb9863df09495b37f8555b`
passed schema generation and native/Linux Clippy. Native regression Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a089a1-9fc9-7662-ad4c-2c3884a1af22`
failed 7 integration controls whose handshake fixtures still sent IPC 24 to the
new IPC-25 isolated daemon; native unit tests passed. Root updates those explicit
wire fixtures and removes internal numeric version duplication. The installed
Windows daemon is still n/alpha.19/IPC24; no live participant has been paired by
this slice. x Linux full regression is running as
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a089a3-2ffe-7cb3-819d-89de5da34f26`.
A checked-in WSL service helper now prepares delegated hard limits and maintains
an explicit Windows-launched interop alias; installation/keepalive acceptance is
pending. x2 is the corrected same-source release candidate to validate next.


## Installed x2 acceptance (2026-09-10)

x2 file-map `4dbfc18c91bbede4df1f6714e442448797df7b064a0ee58a58159c49afba4c99`
passed native fmt/test/check/Clippy/MSRV check+test/release and protected Linux
Clippy/full test/MSRV check+test/three kernel lifecycle controls/release.
[Native Job receipts and logs](evidence/mr3-installed-observation-20260910x2/),
[Linux Job receipts and logs](evidence/mr3-installed-observation-20260910x2-linux/).
Native release Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a089a8-9926-7052-b0b3-8766182865db`
and protected Linux release Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a089a9-a325-72c1-aff3-eb106a41f02c`
produced the installed alpha.20/IPC25 pair. Original Windows history is retained.

Actual installed WSL Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089b7-50f5-7810-ac45-bc01c194ebd6`
passed root exit with a live descendant. Its Windows Grant
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a089b7-5110-7673-b013-793bae2dc0c3`
held the shared cargo slot and was Released only after durable cgroup cleanup.
[Canonical evidence](evidence/mr3-installed-20260910x2/live-canary/) includes
the separate Linux Lease, SQL cleanup row and executor seal. This is partial
M-A08 acceptance; it does not close W-C1..4 or the fault matrix.

The installed systemd helper required a single ExecStart that prepares delegation
from the manager subgroup and execs the installed daemon: separate ExecStartPre
controller activation prevented systemd's next spawn (EBUSY). Unit/helper fixes
are installation artifacts with their own hashes, not changes to accepted Rust.
Linger is enabled. Windows S4U registration was access-denied; the subsequent
UAC elevation was canceled. It has not been retried. An ordinary detached
per-user wsl.exe keepalive was launched as PID 32592; no logout/cold-start
acceptance or permanent Scheduled Task is claimed.


Installed lifecycle acceptance found a real adapter defect: SQL role_index is an
Attempt-wide ordinal, whereas the coordinator expects primary/probe index zero
and postcondition specification index. x2 postcondition Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089c6-4b9a-7d63-944e-9f30c6d65142`
and probe→primary Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089c6-6d35-7ea3-8d6f-39bfd8d49420`
FAILED before second-role user code: the coordinator rejected incompatible order.
Both Grants released after proven cleanup. Root fixes Ticket request and the
local consume transaction; the model regression now uses primary SQL ordinal 7
with wire ordinal zero. y gates and actual reruns are next.

Actual x2 timeout Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089ca-9582-7d23-a97d-456aad0fc392`
and cancel Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089ca-959a-7ca1-8146-c4724f5362af`
PASSED their expected outcomes, one sealed cgroup/SQL cleanup each and remote
Released acknowledgement. [Lifecycle evidence](evidence/mr3-installed-lifecycle-20260910/).
Idle service restart preserved the original store and recovered the bridge, but
its first systemd spawn returned EBUSY while the old bridge process remained;
Restart=on-failure recovered automatically. Active-work restart remains pending.
[Restart evidence](evidence/mr3-installed-20260910x2/idle-restart/).

MR-3.3 now has checked-in installed Linux launchers/templates for all nine gates.
AGENTS/CONTRIBUTING move WSL validation from protected bootstrap to the installed
default attached manager, as explicitly required by the plan. The machine
authority remains the installed default Windows daemon; no direct Cargo or
standalone fallback is allowed. W-C1..4 are still not accepted.


## Installed y2 role-index fix and first consumer failures

y2 Linux installed-system full test
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089cd-10f5-7ec1-bc29-ea58476fed63`,
Clippy `01a089b1-9a6a-7711-a600-39e2b74e495d~01a089cf-ae00-77b3-bd95-6b0fd3c9b4a6`,
MSRV-test `01a089b1-9a6a-7711-a600-39e2b74e495d~01a089d1-88e4-7f00-a158-1af7760d6c9f`,
MSRV-check `01a089b1-9a6a-7711-a600-39e2b74e495d~01a089d2-0042-7441-9c01-b9276d9595ad`
and release `01a089b1-9a6a-7711-a600-39e2b74e495d~01a089cd-55f8-7ff1-b439-7a88efadc3d4`
PASSED through the real paired manager (no bootstrap).
[Evidence](evidence/mr3-role-index-20260910y2-linux/).
Installed postcondition Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089d1-3baa-7421-99df-0d21a2bbd07e`
PASSED primary plus two postconditions under one Work Grant. Probe Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089d1-3bce-75b0-b4be-94f78c51552c`
PASSED with separate probe/work Grants. All Invocations have durable cleanup
and final Released acknowledgements. [M-A09 evidence](evidence/mr3-installed-lifecycle-20260910y2/).

Native y full test passed, y2 Clippy passed. Native y2 MSRV-test failed its
concurrent-ensure integration control with OS error 5; the cause is still under
investigation. A subsequent native test failed missing compiled fixture paths
after moving a completed target cache: cached integration binaries retained their
original snapshot executable path. This is a validation harness defect; z cache
seeding invalidates all workspace fingerprints while retaining dependencies.
Neither failed native run is counted as acceptance.
[Native y](evidence/mr3-role-index-20260910y/), [native y2](evidence/mr3-role-index-20260910y2/).

W-C2 initial actual CLI Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089d2-b175-7750-8cfa-18284eb0c1cf`
survived client loss and completed the real subscription request (Sonnet 5 plus
reported Haiku usage), but its postcondition rejected the mixed modelUsage and
Markdown-wrapped JSON. Root now requests structured JSON Schema output and
requires the selected model family while reporting every other actual model.
The model's claimed shared containment ID is contradicted by the actual distinct
Invocation/cgroup IDs in M-A09; it is not adopted as an implementation defect.

W-C4 initial measurement Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089d4-2925-78d2-8402-b447175bb1df`
FAILED before user code: a transient host CPU detector warming-up result was
treated as a final Ticket failure. Root retains completed waiting outcomes
beyond outbox acknowledgement and retries only explicit transient rejections,
using a new operation with the same kernel-stopped Invocation intent. Unknown
replies retain their original operation. Quiet budget/deadline/cancel remain
bounded, no fresh Ticket means no user code. z gates and actual rerun pending.
W-C3 first actual agent request has been submitted in an isolated scratch worktree.
All W-C rows remain in_progress, no complete consumer round is claimed.


Corrected W-C2 Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089dd-d166-7492-85ce-f54592d1a9cb`
PASSED actual subscription Sonnet review and structured-output postcondition.
W-C3 agent Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089dd-d179-7212-8e74-2c3df93ede77`
PASSED and requested the adapter twice. Its sole managed child
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089dd-ebec-7772-9d2c-18bc9824b22e`
PASSED real Cargo check in scratch worktree, one Attempt, correct parent tuple.
Parent holds only claude2_slots; child independently owns cargo_slots. The first
W-C3 spec was rejected before execution because its script executable was a
symlink; the successful spec resolves the canonical wrapper path explicitly.
[Consumer evidence](evidence/mr3-consumers-20260910/). These are single successful
consumer cases, not the required three complete rounds.

z file-map `b1bb4904704521f535f3337a12a3e53ccc7a02baf6d4f103d1390703eae45115`
passed native full test and full MSRV-test after workspace fingerprint invalidation;
the prior OS-error-5 concurrent-ensure failure did not recur. Installed-system
Linux full test, Clippy and release passed. z is NOT installed: code inspection
found retry also needs to retire the old release barrier only after durable
explicit rejection. z2 adds that retirement and a kernel waiting-response control.
It also separates diagnostic refresh time from ordinary protocol traffic (busy
traffic had indefinitely delayed observations) and uses 20-second idle waits.
Configuration observation changes fence the session and require full authenticated
inventory; they never authorize a new config on their own.


z2 file-map `ae7948926a831aa1de62222281f3477e1b88616c84d27f789787fe46c0e89127`
passed installed Linux full test, MSRV-test and release; the protected kernel
waiting-response control passed as Windows system Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a089e8-f384-70a3-a3d8-2ebb3786283c`.
The root stayed stopped through a transient rejection, retried with a distinct
operation and finally consumed one fresh Ticket before user code; cleanup still
retained the Grant until Release acknowledgement. Native test passed; native
MSRV-test is being collected. Linux Clippy failed one unused-mut warning in
diagnostic refresh. z3 removes it and requires the configured WSL interop alias
to resolve to an actual root-owned /run/WSL socket before bridge spawn, preventing
implicit fallback to an unrelated terminal when the keepalive binding is absent.

Two initial Opus review Jobs were stopped safely before user code because the
250-ms final-release bound expired during concurrent starts; neither invoked
Opus or counts as an independent review. Their canonical failure evidence is
retained, and new review Jobs are being submitted with starts staggered. No
freshness bound was weakened. Repeated normal-load startup deferral behavior
remains an acceptance concern, not a passing result.


## z3 readiness gates and review disposition

z3 source file-map `005ed18d96eb8f5e14cf5b178308b9d7cbb8ec1eeeaef1f16d58b0ca5fffa10a` passed Windows and installed WSL Clippy and full
tests. Native MSRV-test passed; Linux MSRV-test/release and native release are
being collected. No new installation is claimed yet.

Two actual Opus readiness review Jobs succeeded with typed findings verdicts:
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089f1-8d6c-7032-aea1-9f5e273f25b2` and
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a089f1-90a5-72b2-b575-e723160e9110`.
[Verified triage](evidence/mr3-opus-readiness-20260910z2/triage.md) distinguishes
missing review context from confirmed work. These are not MR-3 exit reviews.

Actual same-source full-test pair
Windows `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a0a-8854-77c1-a1f2-d0a66640cecc`,
WSL `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a0a-8363-72d0-be81-e3d8c75518fa`
passed with simultaneous pending requests. Continuous machine history from
sequence 1 shows maximum Cargo debit exactly 1 in their allocation window.
This proves the one-slot case for this pair; two-slot overlap and complete
three consumer rounds remain pending.
[Grant events](evidence/mr3-events-20260910z3-one-slot/).

Python consumer negative controls and AST parsing of the revised upgrade/event
helpers passed as installed system Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a0e-f450-7fa2-b5bd-647b668af39e`.
[Evidence](evidence/mr3-python-controls-20260910z3/). Updated upgrade adds independent
journal checksum/unsealed count and recursive cgroup emptiness barrier, atomic
canonical-image replacement with retained old inode, non-assert integrity checks,
and service start in failure cleanup. Actual use remains to be recorded.


z3 Linux release Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a11-37bd-7190-b3b9-32ad754a1a57`
passed and the strengthened updater installed SHA
`ea916efe8b6adb4816b02f538f3264c74d762b7aee40cfac83f3b3234d42c7bf`,
PID 82828, generation `01a08a12-776c-7cd0-894a-c8da7d5eb992`.
All native and installed Linux z3 test/Clippy/MSRV-test/release gates passed.
[Native](evidence/mr3-quiet-retry-20260910z3/),
[Linux](evidence/mr3-quiet-retry-20260910z3-linux/),
[actual upgrade](evidence/mr3-installed-20260910z3/).
Windows runtime is still x2; native z3 release is built but bridge image-pin
rotation and native installation remain pending. W-C4 is being rerun against
actual concurrent Windows tests.


Installed z3 actual W-C4 passed:
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a13-0642-75b0-b949-31d4f8222beb`.
The concurrent Windows test
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a13-06ba-7631-bb42-5c8f683a8d69`
passed first. Actual coordinator event order proves no overlap of measurement
and cpu_heavy debits. Retained authenticated quiet_waiting outcome plus a later
operation/Ticket proves installed retry, not only synthetic kernel control.
Measurement verified 3,906,336 source bytes per round, 256 rounds, 1,000,022,016
bytes hashed, wall 0.951425941 s / CPU 0.949727458 s, all fixed criteria satisfied.
[Consumer and cleanup](evidence/mr3-consumers-20260910z3/),
[host competition](evidence/mr3-wc4-competition-20260910z3/),
[impact ordering](evidence/mr3-events-20260910z3-measurement/).
This is one successful W-C4, not three complete consumer rounds.


## Both installed sides at z3; explicit pin rotation recovered

Windows release Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a0f-3daa-7fa0-8072-83535e5132b0`
was installed by the native updater after validating the exact target/args and
zero native/remote obligations. [Native upgrade](evidence/mr3-paired-upgrade-20260910z3/).
WSL pin rotation first stopped before mutation on an existing 0644 daemon.lock
inside the 0700 root. Its matching durable intent was retained. The helper now
tightens that owner-verified regular inode to 0600 (does not replace the lock),
and explicit --resume with the original old/new digests completed. Both Store
UUIDs, pairing identities/secret and executor journal were preserved; automatic
reconnect succeeded. [Pin rotation and pair](evidence/mr3-installed-20260910z3/).
The actual interrupted pre-commit path and the SQLite split-commit fault control
are separate evidence; neither is described as whole-VM recovery acceptance.


Two-slot consumer round is now running with both installed z3 binaries. The
Windows config is loaded at daemon startup, so merely replacing config.json
did not change the running capacity. The safe same-image updater restarted the
idle Windows daemon under its admission barrier; WSL performed authenticated
reconciliation and both now report configuration
`416f29b3c89739deab0d632337412ec02539d6d5bdde06ceea9d7c3ad32c3dcf`, Cargo capacity 2.
Original full configuration is retained in
`C:\Development\stillyard-mr3-two-slot-evidence-20260910z3\configuration-before.json`;
restore cargo_slots=1 after the two-slot acceptance, preserving any other settings.

W-C3 round 2 actual agent Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a22-e38a-7652-800c-e55c2e7bc582`
and W-C2 round 2 actual review Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a23-3830-7a80-b3cb-973cfef7acdd`
are running alongside same-source Windows/WSL tests. The W-C2 waiting client was
terminated only after actual CLI provenance appeared; the same receipt/key is
now being awaited from a new client. Final outcome is not claimed yet.


## Three complete installed consumer rounds (labels 2, 3, 4)

All W-C1..4 passed together on installed z3. Round labels 2/3 used two
machine Cargo slots; label 4 used one. Capacity is restored to the original 1.
Round 2 includes deliberate waiting-client loss during actual Sonnet review and
same-key recovery. Before round 3 the idle WSL service restarted with one EBUSY
attempt followed by automatic recovery; this does not close active restart.
Each real agent executed the managed adapter twice, yielding one child/Attempt;
round 4 records the child pending while Windows Cargo runs and its parent waits.
All 256-round measurements passed and coordinator history excludes overlapping
managed cpu_heavy. Public statuses, canonical logs, exact source/specifications,
model usage, adapter audits, cleanup seals and Grant events are retained.

[Round manifest with every Job ID](evidence/mr3-consumer-rounds-20260910z3/rounds.json),
[consumer artifacts](evidence/mr3-consumers-20260910z3/),
[two-slot event proof](evidence/mr3-events-20260910z3-two-slot/),
[round 3 events](evidence/mr3-events-20260910z3-round3/),
[one-slot round 4](evidence/mr3-events-20260910z3-round4/),
[between-round restart](evidence/mr3-round3-restart-20260910z3/).

MR-3 remains in_progress: actual bridge-loss test is now running; active daemon
restart, isolated reset faults, forced cleanup failure, VM/distro/logout/suspend,
priority/no-head-of-line live checks, idle budget and final independent exit review
are not yet complete. Consumer success does not substitute for these gates.


Actual installed bridge-loss acceptance passed. WSL Cargo Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a2e-42db-7a23-913a-fc896145ea13`
was already running when the external harness removed its transport alias and
signalled only the pinned installed daemon's /init bridge proxy. Native Cargo Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a2e-4ac1-75c3-8e66-8ced7568bc9a`
stayed pending both during Linux execution and after Linux succeeded with a
durable kernel cleanup seal. Restoring the alias automatically reconciled the
held Grant/Lease and let native Cargo run successfully. No daemon replacement,
Store reset, synthetic workload or TTL-based release was used in this fault.
[Complete fault harness/results](evidence/mr3-bridge-loss-20260910z3/).

### Installed priority/aging and supervisor review (2026-09-10 z3)

[M-A03 evidence](evidence/mr3-priority-20260910z3/verdict.json): native holder
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a37-9881-73b0-9b6f-345cb06ff5f7`
held cargo_slots=1 while compatible native/WSL Jobs completed. Older blocked WSL
Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a37-98db-7451-b811-31b91edc882d`
aged from priority 1 to 2 and started before newer native priority-2 Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a38-9744-7500-bcde-7521c2dd6cbf`.
Initial harness input errors (missing explicit endpoint, then unsupported priority
10) were corrected; only its own accepted holder was canceled. They are not
product failures or acceptance evidence.

[Supervisor review and disposition](evidence/mr3-opus-supervisor-20260910z3/triage.md)
confirmed exception/stop-budget defects before installation. Initial signal test
Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a38-dd5b-7fd0-a39f-846a993ebee2`
passed; revised helper adds error retention and a failure-injection control.
Active daemon-crash acceptance remains pending until that helper is installed.

### Persistent WSL supervisor installed; active daemon crash passed (z3)

[Helper installation](evidence/mr3-supervisor-installed-20260910z3/) required zero
active Leases/containments plus independent journal seals and empty executor tree.
Exact helper signal/error controls passed Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a3f-fff4-7e01-9b6c-ce5fad39050e`.
Binary, Store, pairing and journal identities were preserved. The persistent
supervisor owns a sibling cgroup and restarts only the daemon/control branch.

[Actual daemon crash](evidence/mr3-daemon-crash-20260910z3/): Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a42-28b3-73c2-997d-f324de09f79a`
had a live primary and descendant before pidfd SIGKILL of the installed daemon.
Supervisor PID stayed fixed, the exact unsealed cgroup remained present, and the
new daemon reconciled it to an independent durable seal. Job became Interrupted;
the original Grant released and Store UUID remained unchanged. No force-clear,
queue reset or generation-as-empty shortcut was used. This closes the active
daemon-crash case, not whole-unit death/VM shutdown/distro termination/logout.

### Installed forced cleanup failure passed (z3)

[M-A08 evidence](evidence/mr3-cleanup-failure-20260910z3/): Linux Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a44-28fa-7413-94b7-a9f65175872c`
ran a primary and descendant. The trusted fault controller added one empty child
cgroup to that exact Invocation boundary (user code has read-only cgroups).
After root exit, recursive cgroup.kill emptied processes but rmdir failed.
Containment became Uncertain, executor seal stayed absent and the Grant remained
held. Native competing Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a44-2a1e-7192-8624-56e42c2e82c5`
remained pending. Removing only the fault child's checked device/inode and empty
boundary allowed the daemon's automatic reconciliation to seal its original
boundary and release the Grant. The native Job then succeeded. No manual
clearance, direct boundary removal or SQL repair was used.

### Five-minute installed idle CPU/memory observation (z3)

[External idle observation](evidence/mr3-idle-20260910z3/scoped-result.json) lasted
300.262 seconds with no submitted Jobs or subscribers (the idle acceptance
requires the observer to remain outside the measured scheduler). Aggregate
0.0431% of one logical CPU and 52.980 MiB across measured Windows/WSL daemon,
bridge/proxy and lifetime processes. Linux daemon 0.00666% / 4.32 MiB anonymous;
native daemon 0.02082% / 28.59 MiB private. Supervisor and keepalive used no
measurable CPU and had zero voluntary context switches during the interval.
Timer wake count is not yet measured: context switches include IPC wakeups and
cannot be relabeled as timer expirations. Complete idle acceptance remains open.

A new cross-OS reset harness is being prepared outside installed z3: native
isolated coordinator exchanges real Tickets with a protected Linux executor
subject using production PreparedLaunch and independent Journal. The subject
will retain a live descendant across isolated coordinator/guest SQLite resets,
then supply its actual durable seal. Compilation and execution are pending.

Idle process inventory correction: the initial name-based Windows observation
also included PID 46556, a separate chequer-private Stillyard instance. Its
1.773 MiB and zero CPU are excluded in scoped-result.json; raw measurements
remain retained. That foreign instance was only observed and never modified.

Cross-OS reset snapshot z4 file-map
`c27a80bb5500416e5e6d6f665fb4299b27ea8b653dc3d1b5ae5d834e96a09ea7`.
Native formatting Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a4e-efe3-7043-a93f-6e3c4f31285e`
passed. First Linux test Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a51-531b-7423-91e3-90284d8b6683`
failed before compiling the crate because cache copying lost executable modes
on build scripts. Restoring source-cache executable modes corrected preparation;
Linux test Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a52-6e0c-7be1-93a4-f9e52bff9722`
then passed and produced the Linux subject binary. The ignored cross-OS scenario
itself has not run yet. README/operator documentation changes are root-only,
after snapshot z4, and do not change installed runtime claims.

Cross-OS z4 native test Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a5a-c70e-73e1-a19d-f167b48e53e6`
passed. The first explicit fault Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a5f-d916-7fd2-b34b-e4b3c54ff82d`
failed before bootstrap or user work: the test Client inferred a daemon image
beside its test executable instead of the installed default image. The next
launcher/helper explicitly pins the installed system CLI. No default Store or
queue was reset. [Opus disposition](evidence/mr3-opus-cross-os-reset-20260910z4/triage.md)
records additional test-evidence and failure-path fixes.

Next snapshot also adds process-local timer-expiration diagnostics and a polling
negative control, so the remaining five-minute wake budget can be measured
without misclassifying IPC context switches. These metrics and the README/
keepalive/initial-installer fixes are not yet installed or accepted.

## 2026-09-10 — completed-snapshot cache cleanup

At the user's request, removed only the eight listed obsolete Linux snapshot
`target/` directories (n, p, q, r, s, t, u, v2). Allocated space removed:
**97.75 GiB (104,954,601,472 bytes)**; observed filesystem free-space
increase: 104,954,208,256 bytes. Both default schedulers had
zero queued/running Jobs, and no visible Linux process command, cwd, executable,
mapping, or open descriptor referenced these targets before removal. Source
snapshots, canonical evidence, installed daemons/stores/journals, z3 rollback
artifacts, current z5 targets, and Windows targets were retained. All eight
directories are absent and their source Cargo.toml files remain. This was
filesystem maintenance, with no Cargo invocation or validation Job.

[Cleanup receipt](evidence/mr3-cache-cleanup-20260910/receipt.json) records exact
paths and before/after values. AGENTS.md now requires disposal of unneeded
worktree/snapshot caches at completion, after checking active and queued work
and preserving evidence and required rollback artifacts.

## 2026-09-10 — z5 completed test gates

Both full test Jobs for z5 file-map
`0fbfca5f4a8dfde177e9e9d8166afb0d7d1680853bdd39ab4c7786c813f1b7fc`
finished `succeeded` with released Grants:

- Windows `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a68-deaf-7a82-8509-193801a56639`: [canonical evidence](evidence/mr3-recovery-metrics-20260910z5/test/).
- WSL `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a68-d9e2-7170-86ec-a261d6c6b417`: [canonical evidence](evidence/mr3-recovery-metrics-linux-20260910z5/test/), Windows Grant `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a68-da02-7272-bce5-69d55cba4dd4`.

These follow successful z5 Clippy Jobs Windows
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a66-710d-7c42-a167-4a43f08637e5`
and WSL `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a66-6bfb-7572-92a1-e1a860ab5ebd`.
The cross-OS reset test is a separately scheduled fault Job; ordinary test gates
do not imply that ignored live fault subject passed. Installed binaries remain z3.

### z5 isolated cross-OS reset attempt: rejected before Linux launch

Fault Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a71-099c-7e50-8f8e-7adb25b573f6`
finished failed: the installed daemon rejected bootstrap attestation from the
native test executable (`bootstrap attestations require this installed daemon
executable`). [Canonical logs and receipt](evidence/mr3-cross-os-fault-20260910z5/).
No Linux subject was launched and no Store was reset. Explicitly selecting the
installed daemon in ClientBuilder fixes server image verification, but does not
make the test executable an authorized bootstrap attester. The bootstrap path
also requires the exact current primary root and its sole native Lease
(`src/store/authority.rs`); simply spawning the installed CLI as a descendant
would not satisfy that contract. The harness orchestration still requires a
compatible design. Keep these production checks intact; M-A06 is not passed by
this attempt. z5 source and targets remain needed for the next acceptance step.

## 2026-09-10 — z6 native companion implementation under validation

The z5 cross-OS harness rejected before Linux launch because the test executable
was not the installed primary bootstrap bridge. z6 keeps all three production
Arm guards intact and runs the native controller as an ordinary child of the
installed CLI primary, within its same native Job. The controller coordinates
the actual isolated Windows Store and protected Linux journal subject via a
private mailbox. The CLI requires the authenticated current root and a finite
Job timeout, bounds the companion wait, and requires both outcomes for success.
The launcher additionally requires the authority-retained outer bootstrap seal.

[Independent Opus design review and disposition](evidence/mr3-opus-bootstrap-controller-20260910z6/triage.md):
Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a75-5555-70e1-bf83-639953a80a97` succeeded.
Source snapshot `stillyard-mr3-bootstrap-controller-20260910z6`, exact file-map
`3afac2fcff6a55201d95bbf9bcc5708f5d0173def42c3f75334d01137526d1c1`.
Native fmt-write Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a7b-a00a-7623-9a8e-f6b6c3b908d4` passed;
Linux Clippy Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a7c-2183-7a23-811c-ef251218bc01` passed.
Native Clippy Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a7c-21eb-7f22-a26a-334d4349afdb` was in progress at this entry.

Both inactive z5 target directories were moved to z6 after both default queues
were empty; no additional full target copy was created. Workspace fingerprints
were invalidated (16 Linux, 15 Windows) because compiled fixtures embed source
paths. z5 source and evidence remain, targets now belong to the active z6
snapshot. Installed binaries remain z3 until candidate gates and upgrade.

### z6 gate result and next corrective slice

Native full test `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a7d-2b22-79e0-9c84-c2392ab6a04c`
and Rust 1.85 test `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a7e-06a7-7c62-8828-8955b3863809` passed.
Linux full test `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a7d-27b0-73c2-9324-876eab74ec87` passed.
Linux Rust 1.85 test `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a7e-06a5-7e22-919b-fa9ab64c668c`
failed in tests/linux_daemon.rs:29 with ETXTBSY spawning a freshly copied fixture
image; preceding library/CLI tests passed. [Canonical Linux gates](evidence/mr3-bootstrap-controller-linux-20260910z6/).
[Canonical Windows gates](evidence/mr3-bootstrap-controller-windows-20260910z6/).

Two parallel libtest fixtures copy and fork independently. A sibling fork can
retain the other fixture's writable copy descriptor until exec, despite CLOEXEC;
this is the documented [Rust issue 114554](https://github.com/rust-lang/rust/issues/114554).
The next slice serializes these two fixture lifetimes within this test binary,
while retaining the actual competing daemon processes inside each test. It also
uses checked monotonic-deadline addition before native-controller spawn, rejecting
an unrepresentable timeout without a panic or process launch.

Superseded z6 release Jobs were explicitly canceled by ID, not accepted as builds:
Windows `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a81-e8bb-7303-84c6-c6b9d7b59c37`
was still pending; Linux `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a81-e8d0-76b1-91eb-bd2906b7cc03`
had started and was canceled through its owning scheduler. No installation occurred.

### z7 validation source

Current snapshot `stillyard-mr3-bootstrap-controls-20260910z7`, file-map
`66df89d2183c4b295ca6995548ef2b552db1de15c8b84c45bbb9994122a7183d`.
Native fmt-write Job `01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a84-f262-7a62-805b-8550baad97ae` passed.
Both queues were empty and no visible Linux process command/cwd/executable/maps/
open descriptor referenced z6 targets before moving both caches to z7. Source
and evidence for z6 remain; no duplicate target was created. Workspace-only
fingerprints were invalidated for the new source path. Installed pair is unchanged.

## 2026-09-10 — z7 installed pair and actual cross-OS reset accepted

All z7 gates passed; canonical evidence is under
[Windows gates](evidence/mr3-bootstrap-controls-windows-20260910z7/) and
[Linux gates](evidence/mr3-bootstrap-controls-linux-20260910z7/).

| Gate | Windows Job (store prefix W) | Linux Job (store prefix L) |
|---|---|---|
| Clippy | `01a08a85-4fac-7893-aff8-d2c0f7401cf3` | `01a08a85-4e26-77e1-a56b-68884422a8a3` |
| Full tests | `01a08a86-ff24-7ca2-9718-14a00b53b9e5` | `01a08a86-fed6-79b0-9c5f-11498412547f` |
| Rust 1.85 tests | `01a08a87-088f-76e1-a450-29ac628d14be` | `01a08a87-080b-7ad2-9321-aaf7a86e0057` |
| Release | `01a08a8c-026e-7b41-9c56-ef11f56c9383` | `01a08a8c-016c-71b2-81ed-afa1f7e16b52` |

W = `01a05f1f-858c-7880-8c15-d55875da9e6b~`; L =
`01a089b1-9a6a-7711-a600-39e2b74e495d~`. Each Linux gate's canonical status retains
its actual Windows Grant, now released. The Linux fixture race correction passed
MSRV as well as stable; no failed z6 result was overwritten or relabeled.

[Installed evidence](evidence/mr3-installed-20260910z7/) records the native upgrade,
explicit bridge pin rotation and Linux binary upgrade with unchanged Store UUIDs,
journal and machine identities. The first rotation attempt stopped before any WSL
mutation: two automatically proven-empty historical incidents were `cleared`,
but old maintenance helpers accepted only `empty`. The correction cross-checks
each such SQL audit with its actual retained Invocation/containment-bound journal
seal and digest, and still requires no Lease and recursive kernel emptiness. It
does not accept forced risk clearance, missing seals, or unmatched identities.
Default Windows Python Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a93-2d10-7561-b0c0-aa29e6efa485` passed
four controls with negative subcases; [evidence and helper hashes](evidence/mr3-maintenance-controls-20260910z7a/).
These helper changes are separate from the immutable z7 Rust build snapshot.
The two accepted historical incident IDs belong to the actual z3 daemon crash
and forced-cleanup-failure tests; no Store rows were rewritten for maintenance.

**M-A06 actual cross-OS reset passed:** default Windows Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08a94-b529-7a00-845e-01bfd7120b1f`,
30.261 seconds, installed CLI primary with native controller descendant and
protected Linux nested executor subject. [Canonical logs, exact subjects, source,
public mailbox evidence and outer proof](evidence/mr3-cross-os-fault-20260910z7/).
Actual Linux user code and descendant were live while guest SQL reset and missing/
corrupt pairing/executor history were rejected, old/new session fencing ran, and
the isolated Windows coordinator restarted/reset. Native admission remained
fenced until actual inner journal cleanup and reconciliation. No installed Store
was reset. The Windows outer containment is `empty`; authority retains released
bootstrap operation `01a08a94-b531-7d63-89d9-a21e9e0e807d` with sealed_empty,
termination=exited and root_exit_code=0, kernel cgroup inode 62452. The successful
private mailbox was removed after allowlisted public evidence export.

Scope: actual coordinator RPC/Store plus actual Linux Store, PreparedLaunch,
Journal and kernel boundaries. Full installed attached Driver/session freshness
is covered separately by prior installed acceptance, not replaced by this subject.
The early capacity-reduction portion still precedes the real Linux Invocation,
so M-A04 live acceptance is not inferred from this pass. M-A07 live session coverage
is recorded in progress pending final review of the required WSL scope.

### z7 five-minute idle observation

External observer ran for 300.743 seconds with unchanged
Job counts/history, daemon generations, process identities and keepalive alias.
No Stillyard Job or subscriber was created by the observer; this follows the
plan's explicit idle-measurement condition, rather than the normal build path.
[Raw snapshots, observer and result](evidence/mr3-idle-20260910z7/).
Aggregate CPU 0.048214% of one core; private/RSS-anonymous
memory 48.809 MiB. Daemons individually and the bridge
meet their CPU/memory limits. The foreign chequer daemon is excluded by exact
installed image path. Both daemon timer counters sum to 15 expirations (all
Linux attached-worker waits), 2.992587/minute;
reactor/subscriber/transport/backoff deltas are zero on both sides. Python
supervisor, Python keepalive and Linux init relay each have zero voluntary
context switches. Context switches of the active IPC processes are retained as
diagnostics and are not relabeled as timer expirations.

CPU/memory budget result: pass. Aggregate timer acceptance still needs its
helper coverage/source-audit disposition and a negative control using the actual
attached wait path; the current isolated Counter unit alone is not adopted as
a full daemon polling-mutant result. Installed pair stays z7.

### Completed snapshot resource cleanup after z7 installation

Removed 82 obsolete target directories whose source manifests identify this
checkout and the MR base commit; retained current z7 and consumer scratch caches.
Both default queues were empty. Visible Linux cwd/executable/maps/descriptors and
Windows executable/command-line references were checked; sources, evidence and
installed state were preserved. Verified z3 rollback images already exist outside
all targets, with the accepted z3 hashes. Observed additional free-space increase:
Linux 134.60 GiB, Windows
348.33 GiB. This is filesystem maintenance,
not a Cargo validation. [Exact paths and receipt](evidence/mr3-completed-cache-cleanup-20260910z7/receipt.json).
Historical target paths in earlier evidence now require rebuilding from retained
source; canonical logs/receipts and required installed rollback binaries remain.

## 2026-09-10 — resumed z8 acceptance controls (in progress)

Installed z7 identities and empty queues rechecked. The next immutable slice adds
capacity reduction/restoration while the actual Linux boundary is populated,
explicit old/new executor incarnation fencing with that boundary still live,
primary-role rejection before native companion spawn, and an ignored control
exercising the real attached wait/doctor counter path. It also includes the
separately accepted maintenance barrier fixes. No new gate result is claimed yet.
Capacity reduction is additional coverage, **not** the M-A04 crash/duplicate
matrix around Arm/local commit/release; that row remains incomplete.
Current target caches will be moved after process/Job checks, not duplicated.

### z8 actual attached wait negative control

Default Linux Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08aad-e725-7271-b1a6-f511afe26408`
passed: the real ReleaseState wait and doctor projection recorded 8 timed waits
over 0.080821 seconds (5939.08 expirations/minute), correctly failing the 6/minute
idle budget, while the notified wait added zero. This is an actual wait-path
control, not a complete daemon configuration mutation.
[Canonical evidence and exact executable](evidence/mr3-attached-idle-control-20260910z8/).
The first collector sampled Job finality before its asynchronous Grant release
acknowledgement and rejected that intermediate state. The launcher now waits for
that separate transition. Receipt recovery reused the same idempotency key/Job;
no second Invocation was launched. Both intermediate and reconciled status remain.
Focused [Opus coverage review and disposition](evidence/mr3-opus-idle-controls-20260910z8/)
keeps aggregate helper timer coverage open; CPU/memory results are unchanged.

### z8 matched gates and live capacity/incarnation acceptance

Source file map `103ff0bcad1df790ade627d2dcde9300044e200ccb1b4a271430a066cb72eef5`.
Both Clippy/full test/MSRV test suites passed.
[Windows gates](evidence/mr3-live-controls-windows-20260910z8/),
[Linux gates](evidence/mr3-live-controls-linux-20260910z8/).

| Gate | Windows Job (W prefix) | Linux Job (L prefix) |
|---|---|---|
| fmt-write | `01a08aaa-2c86-7363-ab9e-77da44aa8374` | formatted bytes copied from Windows |
| Clippy | `01a08aab-2b0f-71c0-90c9-7c45c243c28b` | `01a08aab-259f-7c72-832f-a3c7dd285877` |
| test | `01a08aac-165a-79f2-8164-7366c3ab7053` | `01a08aac-1088-7780-8882-a9886daa9967` |
| Rust 1.85 test | `01a08aad-eda3-7103-9ea0-b96e6fae9837` | `01a08aad-e74b-7100-9fae-f8447b5daa5c` |

W = `01a05f1f-858c-7880-8c15-d55875da9e6b~`; L =
`01a089b1-9a6a-7711-a600-39e2b74e495d~`.

Live cross-OS control passed in 58.861 seconds under default Windows Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08aaf-f72f-7d01-a33e-3384f5846681`.
[Public evidence, canonical logs and actual outer bootstrap seal](evidence/mr3-cross-os-fault-20260910z8/).
The actual Linux root/descendant remained in the same populated, unsealed kernel
boundary during side_lane capacity 1→0→1 and coordinator restarts. A native
canary stayed pending. An explicitly different executor incarnation then fenced
the old writer while retaining its exact Ticket/Grant and live Linux boundary.
Guest/coordinator reset and missing/corrupt history controls still passed; only
actual cleanup/reconcile unblocked the final native token consumer.
Both inner journal and outer bootstrap seals are retained for their distinct
boundaries. Private mailbox removed after allowlisted export.
M-A06 remains pass; M-A07 has explicit incarnation evidence pending final
coverage review. M-A04 remains incomplete, unaffected by capacity coverage.
Installed pair was z7 during this prebuilt-subject Job; subject binaries are z8.

### z8 pair installed

Release Jobs W`01a08ab3-a3c4-7892-9cf7-6ee1bc6a2789` and
L`01a08ab3-9e6f-7ba2-b984-e298d9bfe795` passed. Both installed images now come
from the exact z8 map above. The Windows upgrade, bridge-pin rotation and WSL
upgrade passed without manual recovery, preserving both Store UUIDs, machine
identity and executor journal. Corrected maintenance helpers are part of z8.
[Installed hashes, generations, barriers and doctor](evidence/mr3-installed-20260910z8/).
No target copies were created. Current z8 caches remain needed for acceptance.

## 2026-09-10 — live stale-candidate cancellation/release starvation (z8)

The final installed consumer round exposed a product liveness bug: an unarmed
measurement candidate lost readiness; `queue::maintain` repeatedly enqueued
CancelCandidate revision 1 for advertised revision 1. The actual coordinator
rejected it as stale. Because that branch precedes ready releases, two already
completed/actually sealed Jobs retained local Lease and machine Grant indefinitely.
[Read-only live SQL excerpt, daemon snapshots and matching actual seals](evidence/mr3-stale-cancel-live-20260910z8/).
No cleanup proof is missing; no Store row or Grant has been force-cleared.
Root fix advances cancellation revision and prioritizes sealed release work.
The existing lifecycle test is extended with a stale second candidate so the
old implementation fails before it can acknowledge the cleaned first Job.
The current installed pair remains z8 until scheduled repair gates pass.

### Recovery capacity and update preparation

The same installed Windows image restarted under an admission barrier with
previously accepted machine capacity cargo_slots=2. The two remote Grants remain
charged, including cargo_slots=1; the second token enables protected recovery
Jobs. No Lease/Grant history was edited. Restore capacity 1 after normal release
reconciliation. [Exact process/configuration evidence](evidence/mr3-capacity-recovery-20260910z8/).
The ordinary attached builder is currently affected by this liveness bug; the
repair build uses the already accepted protected Windows bootstrap Job, with
its real shared token and sealed cgroup proof. This is a recorded recovery path,
not a return to direct Cargo or an independent scheduler.

Installed z8 idle observation: 300.729 seconds, unchanged process/generation and
Job counts; aggregate 0.037825% of one CPU and 49.723 MiB including companions.
Daemon timer counters total 2.992728 expirations/minute; companion timer coverage
and actual non-idle counter controls remain open.
[Raw observer and results](evidence/mr3-idle-20260910z8/).

### z8 confirmation disposition and z9 recovery source

[Canonical z8 round evidence](evidence/mr3-consumer-confirmation-20260910z8/):
W-C1 Windows `01a08aba-fe2f-7003-bef0-c63111d73584` and Linux
`01a08aba-f706-7f61-bdfd-c641e6391ca4` succeeded. W-C2 actual review
`01a08aba-f60c-7c50-9827-8a7f3d0739eb` succeeded. W-C3 agent
`01a08aba-f5f2-7d40-adba-877947e79052` exited successfully but **consumer
acceptance failed**: its first managed adapter call exceeded the agent's Bash
wait, became background work, and the agent ended before two completed calls.
Its one actual child `01a08abb-0a17-73f2-9fbb-b4b8aa1d7cc7` did succeed;
this is not a passed agent round. W-C4
`01a08aba-f63a-7153-af16-9b448b4ef08e` was canceled explicitly after diagnosis
of stale-cancel starvation; its pending result is not measurement acceptance.
New validator checks exactly two completed sequential calls, common parent,
key/spec identity and one successful actual child with one Attempt.

z9 repair source map:
`2d9283d9e49f392f8bbf9762fed40e78c9d3aab7ec9a6fc2e9114d98945e0d1c`.
Windows snapshot `C:\Development\stillyard-mr3-release-recovery-20260910z9`;
matched ext4 snapshot `/home/pythonic/Development/stillyard-mr3-release-recovery-20260910z9`.
Completed z8 native/ext4 gate caches moved into z9, not copied. Source and
canonical evidence retained; path-dependent local-package fingerprints removed.
Scheduled fmt-write W`01a08ad0-1f69-7670-abc6-49b5cb404e7d` passed.
Maintenance controls W`01a08ad0-1f0d-7e33-b67f-235235a5bdf7` passed seven
positive/negative tests, retaining SQL history and refusing incomplete seals,
nonfinal work, foreign identities or unconsumed Tickets.
[Canonical Python controls](evidence/mr3-maintenance-controls-20260910z9/).
Rust gates and repair installation are in progress; installed pair is still z8.

### z9 Linux repair installed; original retained Grants released

Protected release Job W`01a08ad3-da12-7372-bc5c-9597a9c9b1cd` succeeded.
Actual default Windows bootstrap Hold binds its work, source target and
sealed-empty exit-0 proof to the installed Linux image
`5d551bc28e4f6937e9db3606ab38084f5e59af284b96657e2e23c7b01cc15fd9`.
The admission barrier verified all actual journal seals, recursive kernel
emptiness, final owner Jobs, resolved Invocations and matching consumed Tickets/
cleanup digests for the two retained work Leases. No resource/history rows were
modified. After replacement, normal daemon reconciliation released both original
Grants automatically, preserving the Store UUID and original Job outcomes.
[Before/after Jobs, upgrade barriers and hashes](evidence/mr3-release-recovery-installed-20260910z9/).
Windows remains z8 temporarily; native final gates and the ordinary attached
Linux test on z9 are in progress. Shared capacity remains 2 until they complete.

### Installed z9 pair and real timer-path controls

Both z9 images are installed and paired. Windows release Job
W`01a08ad8-5875-7c23-913a-b28cd37b779c` passed; native Clippy/test/MSRV-test
and protected Linux Clippy/test/MSRV-test/release all passed.
[Canonical gates and exact map](evidence/mr3-release-recovery-20260910z9/).
[Windows installation and bridge-pin rotation](evidence/mr3-windows-recovery-installed-20260910z9/).
The native MSRV launcher recovered its original accepted Job
W`01a08ad6-778e-7ed2-a65a-c75c2857e42d` after a lost receipt; no duplicate build.
Capacity restored 2→1 only after all retained Grants released.
[Same-image configuration restart](evidence/mr3-capacity-restored-20260910z9/).

Actual installed timer control passed: L`01a08ae0-57ba-79e1-9eeb-f1b1bca59e00`
stayed pending while its exact interop proxy was stopped and alias parked, then
succeeded/released after restoration. Condition/subscriber Job
L`01a08ae0-8179-79f1-9746-c2db80314782` failed on its deadline without launch.
Observed deltas: transport 1, backoff 3, reactor 5, subscriber 2, attached 42.
These are deliberately non-idle timer-path controls, not an idle-rate pass.
[Controller, identity checks, snapshots and results](evidence/mr3-installed-timer-controls-20260910z9/).

Independent Opus recovery review L`01a08ad7-62ed-7dd1-b825-3d982fcf4222`
completed. [Review and verified disposition](evidence/mr3-opus-release-recovery-20260910z9/).
The proposed accounting bugs relied on missing caller/SQL guards; verified
against those guards and rejected. Accepted helper refinements: stopped-journal
recheck, built-in assertion that retained resources released after reconnect,
and canonical cgroup path check. Their next validation is z10b.

### Actual process-crash boundary matrix in progress (z10b)

Test-binary-only checkpoints cover journal ready, durable release intent,
consumed Ticket before kernel release, after kernel release, and durable seal
before local SQL cleanup. A separate test process is actually SIGKILLed;
continuous Store/journal recovery must retain resources until actual kernel
cleanup and the authenticated Release acknowledgement. Starts are counted from
real user output, and the reply is replayed after acknowledgement. Protocol
replies in this specific fixture remain modeled; actual Windows coordinator
crash/replay and installed bridge-loss controls are separate evidence.
Initial z10 Clippy L`01a08ae2-2617-7351-9c5e-af32f24290b1` rejected one unused
test import; corrected in z10b. [Initial diagnostic](evidence/mr3-crash-boundaries-20260910z10/initial-clippy/).
z10b map `f965ff8755571bbf0aa40c9484e0d2fd8551b9afff2e4f5dac033290ab68840b`.
Current gate caches were moved z9→z10→z10b after their Jobs completed; no full
target copies. Source/evidence and installed/rollback images retained.
Final consumer confirmation moves to this final accepted source after its gates.

z10b ordinary attached Clippy L`01a08ae4-60c4-7863-9fda-dc15dea6d1bd` and
full test L`01a08ae4-81f3-7c81-a238-ede29b6478e5` passed.
The first protected matrix W`01a08ae5-6585-7e11-bed8-567ccef752e3` failed
at fixture restart. A lost NTFS receipt was recovered with the original key;
its actual bootstrap Hold was sealed empty and released.
The fixture used a noncanonical journal location and a model Windows host ID.
Moving the journal in z10c was insufficient: W`01a08ae8-a4e1-7192-bfaa-fe0dd2fa4ae8`
still correctly rejected the foreign-host Store on actual Linux reopen.
z10d uses actual Linux startup identity for real-kernel fixture scenarios;
model-only tests retain their explicit synthetic identity. No installed Store
or cleanup history was changed by these test corrections.
The protected bootstrap launcher now recovers unknown acceptance using the same
intent/key into a fresh receipt, matching the native launcher's existing policy.

### z10d actual SIGKILL matrix passed

Protected Windows Job W`01a08aeb-c3ae-7fb3-bce7-1ed711eccd10` passed in
17.52 seconds after compilation, on exact map
`9e636ec3e8802e4d6ff2c4611d65c4fcdb35eb6bd1dc4c7c28bae86af000329b`.
Five exact child processes were SIGKILLed at actual runtime checkpoints:
ready / release-intent / consumed-before-kernel-release / released /
sealed-before-local-cleanup. Actual user-start counts were 0 / 0 / 0 / 1 / 1.
Each retained one local Lease through real kernel cleanup until the authenticated
Release acknowledgement; replaying that same acknowledgement was a no-op.
The seal already durable at the fifth point survived unchanged. Every case
retained exactly one Invocation. These checkpoints exist only in test binaries.
MSRV matrix, remaining matched gates and independent scoped review are running.
This closes the missing real consumed-before-kernel-release crash evidence;
M-A04 as a whole remains pending the composed-coverage review.

The maintenance Python Job L`01a08aec-503d-7163-9691-95d4d9d53bd0` correctly
failed with exit 5 because unittest discovery skipped hyphenated script names
and ran zero tests. Corrected explicit script execution Job
L`01a08aed-a785-7402-8d34-9308523ff056` passed 13 controls (1+7+3+2), covering
pin rotation, retained-seal barriers, recovery-build provenance and supervision.
No product change was needed for the discovery mistake.

z10d completed all native/attached Linux Clippy, full test, MSRV-test and release
gates, plus stable/MSRV protected SIGKILL matrices and the 13 Python controls.
[Canonical completed z10d gates](evidence/mr3-crash-boundaries-20260910z10d/).
Independent review confirms the five actual crash boundaries and found no
product defect. [Review and disposition](evidence/mr3-opus-crash-boundaries-20260910z10d/).
Two test-only refinements follow in z10e: exact Release cleanup bits/digests for
each recovered Ticket, and a 5.5 s native pending-canary assertion after Ticket
issuance. Executor possibly_released and continuous-manager consumed are
intentionally distinct evidence at the release-intent/SQL gap; no forced equality.

z10e map `838f56a3083cc41c4b1349eb8a08f367a65a2cb1474536932a9934932f829ecc`;
current gate caches moved from z10d without copies. Consumer scratch source is
matched z10e; its existing cache moved through the unsubmitted d preparation.
Stable and MSRV crash matrices, attached Linux full tests/MSRV/Clippy passed;
native gates, explicit checks and release are in progress. One Linux launcher
preflight saw a stale authority_held observation just after successful bootstrap
cleanup; no Job was submitted. A fresh query showed no actual Hold and a healthy
pair, then normal submission proceeded. No force-clear or duplicate submission.

Read-only lifecycle preflight found other active work in Ubuntu-SSD: GitHub
Actions runner, unrelated Codex/Claude sessions and a foreign Cargo workload.
Whole-distro/VM/host termination is not authorized over those processes by the
routine implementation scope. No such termination or UAC retry was attempted.
Continue independent acceptance and prepare the remaining controlled lifecycle
procedure before requesting any agreed interruption window.

### z10e gates complete and pair installed

All 13 scheduled gates passed, including native/attached check, Clippy, full tests,
Rust 1.85 tests, release builds and protected stable/MSRV crash matrices.
[Canonical Jobs and exact source](evidence/mr3-crash-boundaries-20260910z10e/jobs.json).
The native canary remained pending beyond TTL after actual Ticket issuance.
Both installed images were upgraded with queue/cleanup barriers; native pin was
rotated and both original Stores retained. Both report healthy machine scheduling
with shared cargo_slots=1. [Upgrade and retained identities](evidence/mr3-installed-20260910z10e/).
The final W-C1..4 confirmation round is running on this installed pair, including
a W-C3 postcondition that requires two sequential completed adapter calls and
one actual child. Full lifecycle and exit review remain incomplete.

The z10e confirmation round is **failed**, not pass: W-C1 Windows/Linux, W-C2,
W-C4 and the actual W-C3 child all succeeded. Parent
L`01a08b05-e2ff-7f51-8de5-89e02ae67be1` failed its acceptance postcondition.
First adapter call retained an accepted receipt but returned unknown/70 at child
cleanup; second call recovered the same successful child
L`01a08b05-f63e-7de2-a821-c57c58cdc402` (one Attempt). The model's claim of a
fresh second submission is contradicted by both retained calls' identical receipt.
[Canonical round and actual call audit](evidence/mr3-consumer-confirmation-20260910z10e/).
Code inspection found membership inspection can race removal of an unrelated
child cgroup before the sealed registry entry is published; a stale candidate
snapshot can also outlive active-entry removal. Next slice serializes membership
with journal cleanup and uses only exact retained seals for negative membership,
with a deterministic real-kernel regression. No client retry masks this race.

Independent composed review accepts M-A04 with explicit actual/model boundaries.
M-A07 needs one new-incarnation duplicate-Ticket assertion after live reconciliation;
added for the next native/cross-OS run. [Review, initial launcher failure and disposition](evidence/mr3-opus-composed-20260910z10e/).

### z11 membership race regression

Protected negative-control Job W`01a08b0d-62db-7ff1-8116-0a0f62ebeb1d`
compiled the new regression with the old membership implementation and failed
with exact ENOENT during real cleanup before sealed-entry publication. Fixed
Job W`01a08b0f-4ae7-72b0-96ba-34752786d3bc` passed both protected registry
controls. Both outer bootstrap Holds are sealed empty and released.
[Failing old implementation](evidence/mr3-membership-mutant-20260910z11/);
[fixed kernel controls](evidence/mr3-membership-20260910z11/).
The fix serializes membership with journal cleanup, then uses exact durable seals
for stale candidate snapshots after clear; unknown entries still fail closed.
Current caches moved to z11b for full matched gates, with the additional
M-A07 new-incarnation duplicate assertion. No target copies accumulated.
Source map `66bdf467b1eaa9b81cc14c044a1ca6b04af1d6ba630178f833dc64924b101c2f`.

The independent membership review confirms correct locking and auth semantics.
Its material extra negative control (existing unsealed journal record with no
active entry stays unknown), plus default-registry unknown, is included in z11c.
The superseded z11b orchestrator was stopped after Linux check/Clippy/full/MSRV
and native check; its already submitted native Clippy was allowed to finish.
No daemon, Cargo process or accepted Job was canceled. Caches move only after
all accepted Jobs finish. [Review and disposition](evidence/mr3-opus-membership-20260910z11/).

### z11c MSRV diagnostic and z11d continuation

Native formatting and attached Linux check/Clippy/full tests passed on z11c.
MSRV-test L`01a08b1a-c67f-7d81-9047-f8f06127cffe` failed the existing quiet
stability fixture: its supplied model clock advances exactly at the gap/stability
threshold, but even a scan of zero conditions added real wall-time jitter.
`ConditionEvaluations::scan_until` now returns the initial zero-provider-work
evaluation for an empty list. Live observation moments continue to reread actual
clock time; real provider scans retain measured elapsed time and stale checks.
This removes a model-clock contaminant rather than retrying until a pass.
[Failed MSRV and completed z11c Jobs](evidence/mr3-membership-20260910z11c/).
The matched z11d gates follow; current caches were moved, not duplicated.

A separate [controlled lifecycle window procedure](mr3-lifecycle-window.md)
records exact fault/restart steps, the current conservative unsealed-boot recovery
limit, and the required external Windows controller/foreign-work agreement.
No whole-VM, distro, host, logout or sleep action has run.

The concrete native interruption controller passed read-only preflight Job
W`01a08b2b-28b8-74b3-9db2-720b48ba6a7a`: Ubuntu-SSD had 153 selected
foreign processes, zero local granted Leases and no unsealed executor records.
The initial controller's Store-path assertion failed before WSL access and was
corrected to the installed `Stillyard/data` path. Both outcomes are retained.
[Controller and unsubmitted terminate/shutdown Jobs](evidence/mr3-lifecycle-window-20260910z11d/).
No destructive Job was submitted; cold-start/handoff/canary steps still require
the separately agreed interruption window specified by the plan.

All 13 matched z11d gates passed, including release and protected registry
controls on stable/Rust 1.85. [Canonical Jobs](evidence/mr3-membership-20260910z11d/jobs.json).
Actual cross-OS Job W`01a08b2d-4ca4-7713-b163-3241e9b4ddd2` passed:
new executor incarnation completed live reconciliation, both generic fences were
clear, then duplicate authorization was rejected with exact `conflict` /
`ticket identity is single-use`. The real old root/descendant boundary stayed
populated and unsealed. M-A07's remaining assertion is now enforced, not just
reported; the rest of the reset/capacity/cleanup sequence passed and the outer
bootstrap Hold was sealed empty. [Public subject evidence](evidence/mr3-cross-os-fault-20260910z11d/).

### z11d installed pair

Both accepted images installed; native bridge pin rotated, original Stores and
executor journal retained, shared cargo_slots=1 and both sides healthy.
The first idle-barrier check correctly refused while another project's native
CI Job was active; no replacement occurred then. After that Job completed,
normal maintenance succeeded. [Installed images and barriers](evidence/mr3-installed-20260910z11d/).
The live W-C1..4 confirmation now runs against the installed membership fix.

### z11d live confirmation: primary fixed, postcondition authentication gap

W-C1 Windows/Linux, W-C2 and W-C4 succeeded. Both actual W-C3 adapter calls
completed successfully with the same child/Attempt/receipt/key; the child
L`01a08b31-34f2-7f20-b245-be8c295ab33d` succeeded once. Parent
L`01a08b31-2016-7af3-94f6-dad66aa471ac` failed only its postcondition:
the read-only installed CLI status request received no authenticated response.
The runtime supplies attestation context for every Invocation role, but the
server incorrectly authenticated that transport through primary-only submission
context. Next slice authenticates exact live Invocation identity and actual
peer containment separately, retaining primary-only child submission/wait.
Non-primary containments must also remain visible to submission authentication
so clearing environment cannot downgrade them to unmanaged callers.
[Failed round, both completed calls and canonical Jobs](evidence/mr3-consumer-confirmation-20260910z11d/).
Final idle measurement was not started after this failed acceptance round.

Old initial scratch target removed after active/queued Job and process-reference checks: `/home/pythonic/Development/stillyard-wc3-scratch-20260910/target`, 462.32 MiB reclaimed. Source/worktree retained. [Cleanup evidence](evidence/mr3-cleanup-20260910z11e/). Current gate caches moved z11d → z11e without copying.

### z11e attestation fix installed

All 13 native/Linux gates passed, including stable/MSRV protected namespace
attestation controls. Both installed daemons upgraded with original Stores and
journal preserved, shared capacity 1 and healthy attachment.
[Exact source and Jobs](evidence/mr3-membership-20260910z11e/jobs.json);
[pair upgrade](evidence/mr3-installed-20260910z11e/);
[independent attestation review and disposition](evidence/mr3-opus-attestation-20260910z11e/).
Final consumer confirmation and real non-primary RPC controls are running.

Actual installed non-primary control passed as
L`01a08b4b-d95a-7fe2-b73d-9fc51866b66c`: real probe and postcondition both
read authenticated status, then raw environment-independent context/submit
requests were rejected by actual kernel membership with primary-only authority.
[Control source, typed results and canonical Job](evidence/mr3-attestation-control-20260910z11e/).

The full z11e confirmation failed at W-C3 startup before user-code release:
L`01a08b4b-3197-7a40-bfce-2d0615f8f7f7`, Attempt `start_failed`, no started
Invocation timestamp or model calls. The local Ticket readiness/freshness barrier
refused release; kernel cleanup was empty, and executor OOM counters stayed zero.
[Full failed round and retained Ticket cleanup](evidence/mr3-consumer-confirmation-20260910z11e/).
W-C1 Windows/Linux, W-C2 and W-C4 succeeded on this same installed source.
Focused W-C3 repeat uses explicit bounded RetryPolicy for `start_failed` (3 Attempts,
1-second backoff); fresh Invocation/Ticket per retry, no TTL relaxation or replay
of user code. Other passed consumers are retained instead of rerun unnecessarily.

Focused W-C3 repeat passed on installed z11e: parent
L`01a08b50-0cab-70d1-bfea-3815f6cd9643`, child
L`01a08b50-2569-7d83-b13d-3975a9770237`, native competitor
W`01a08b50-1246-7163-93c8-04e1e2cb103f`. Both sequential calls completed
with the same child/Attempt/receipt/key, authenticated acceptance postcondition
succeeded, and actual pending-child/active-Windows overlap was recorded.
[Canonical focused confirmation](evidence/mr3-consumer-confirmation-20260910z11e2/).
The next slice removes native busy-pipe one-second timeout/retry polling so
bridge timer expiry cannot remain hidden during otherwise healthy operation;
real occupied-pipe negative control and matched gates follow.

### Git checkpoint and installed z11f

At the user's request, branch `wsl` was created and pushed to `origin`.
Commits `b663d39` (implementation/contracts/ledger) and `8cf08e7` (canonical
evidence) preserve the accumulated work; remote HEAD was verified equal to local.
Future milestones are committed/pushed incrementally; merge into main is deferred
until delivery acceptance. Disposable targets and transient receipt locks are
excluded. Historical manifests retain their original baseline commit and exact
file maps; this checkpoint does not rewrite that evidence.

All 11 z11f matched gates passed as default Stillyard Jobs, including Windows and
Linux check, Clippy, full tests, Rust 1.85 tests, release builds and native fmt.
[Exact Jobs and source map](evidence/mr3-membership-20260910z11f/jobs.json).
The real occupied native-pipe old-loop mutant failed with two waits rather than
one; [negative control](evidence/mr3-busy-pipe-mutant-20260910z11f/).
The installed pair upgrade passed all empty barriers with original Stores and
journal retained; [installed pair](evidence/mr3-installed-20260910z11f/).
Scratch source now matches z11f; its cache moved e → f without copying, after
empty queues/process-reference checks; [cache move](evidence/mr3-cleanup-20260910z11f/).
Current-pair W-C1..4 confirmation is running.

The independent busy-pipe review confirmed the API fix and identified an omitted
client receiving-thread timeout in the original idle argument. The corrected
[complete helper wait audit](evidence/mr3-helper-timer-audit-20260910z11f/audit.md)
includes that path; [review disposition](evidence/mr3-opus-busy-pipe-20260910z11f/triage.md).
Final aggregate idle acceptance is still pending actual interval observations.

The h lifecycle bundle has passed a read-only native preflight and is prepared
for review. Terminate/shutdown remain unsubmitted with no approval file.
[Prepared bundle](evidence/mr3-lifecycle-window-20260910z11h/) and
[earlier review disposition](evidence/mr3-opus-lifecycle-window-20260910z11f2/triage.md).

Final-source z11f W-C1..4 confirmation passed: all six Jobs succeeded, including
Windows and Linux full tests, real review, actual measurement, agent parent and
its one managed child. Both agent adapter calls completed sequentially and
recovered the same child/Attempt/receipt/key; authenticated postcondition passed.
Actual pending-child/active-Windows overlap was captured under shared capacity 1.
[Canonical Jobs and results](evidence/mr3-consumer-confirmation-20260910z11f/).
