# MR-3 controlled Windows/WSL interruption window

Status: prepared procedure; disruptive steps have not run. This is not acceptance
evidence. Current installed identity and the exact accepted build Jobs are in
[machine-resource-implementation-status.md](machine-resource-implementation-status.md).

The scheduling plan requires a native default Windows Job to control whole-VM
faults, and separate agreement if unrelated work is affected. The current WSL
inventory includes other development sessions and services. Do not run the
commands below from the current WSL agent or as an unscheduled shell fault.
Do not retry the canceled S4U/UAC task registration.

## Preconditions and durable record

Prepare the native controller and its receipt on NTFS, outside every target
cache. Use the installed Windows Stillyard CLI and native Python 3.13. The
controller is a default Windows Job with no Cargo claim; its own receipt and
Job ID identify each operation. Keep the same intent/key when reconnecting.
The controller rejects execution from Linux or without its actual native primary
Invocation identity. The controller/starter bundle, revised after independent review, and unsubmitted
JobSpecs are retained in [prepared evidence](evidence/mr3-lifecycle-window-20260910z11j/).

Before submitting the disruptive Job, record the user's separately agreed scope
in an operation-specific `approval.json`: permitted operations, exact running
distributions, selected foreign process identities and an agreement to freeze new
submissions. The approval file is deliberately absent until that agreement exists.
Launch `starter.py --exchange-directory <operation>/exchange --approval-file
<operation>/approval.json` as an ordinary native Windows process outside every
Stillyard Job. It checks actual Windows Job membership and publishes a pinned
PID/creation-time/image record bound to the approval hash. The starter initially
waits; it neither opens WSL nor performs the fault. Keep its Windows session open.
The controller requires this ready record before stopping anything.

Before each interruption, record both daemon statuses and doctor reports,
Windows authority obligations, running distributions, Linux boot ID, current
interop alias/owner and installed binary hashes. Require empty user queues,
no retained unsealed executor obligations, no active bootstrap holds, and no
unresolved cleanup. The controller's own native Job is the only permitted
active native Job at this initial quiescent lifecycle stage. Confirm the agreed
inventory again immediately before the fault; a changed inventory stops the step.
Keep pairing anchors private; hashes and public identities suffice.

The j preflight executes this empty barrier, including the accepted maintenance
helper's SQL/journal cross-check under a rolled-back admission transaction.
Historical automatically cleared `proven_empty` containments are accepted only
with matching executor seals and boundary digests; forced resolutions remain
blocking. Its native default Job `01a08b9f-e7b3-71d0-b8f0-be7a634f1b9d` passed
with zero active Leases, zero blocking containments, no unsealed Invocations and
an empty recursive executor cgroup. The two historical records and their exact
seals are retained in `preflight/result/preflight-clearance.json`.

This first window tests restart/lifetime with already sealed history. Existing
active-daemon SIGKILL and bridge-loss Jobs cover live work. It must not be
reported as active-work whole-VM cleanup acceptance.

The prepared approval scope below covers operations 1–3 and subsequent canaries.
Sleep/resume, logout/login and reboot remain separate unprepared windows.
The bundle includes a non-authorizing `approval.template.json`; copy the exact
running distributions and sorted foreign-work array from a fresh preflight only
after the user agrees. The controller also verifies the external starter has
more than 180 seconds of its original wait budget immediately before the fault.

## Separate operations and recovery

1. **Terminate Ubuntu-SSD:** the native controller records intent, then invokes
   `C:\Windows\System32\wsl.exe --terminate Ubuntu-SSD`. Record its actual exit
   and elapsed time. Absence from the running list is only a runtime observation,
   never an empty-process proof. The current WSL agent session will end.
2. **Cold start:** after the native fault command and stopped-distro observation,
   the controller writes a correlated recovery request. The already-running
   external Windows starter launches `wsl.exe -d Ubuntu-SSD -u pythonic --exec
   /usr/bin/python3 /home/pythonic/.local/share/stillyard/libexec/wsl-service.py
   keepalive --root /home/pythonic/.local/share/stillyard` and remains its owner.
   No controller descendant is detached. The controller waits for that helper's
   own Linux startup record before entering WSL for observations; then it verifies
   live external Windows identity, actual Linux helper/init relationship, strict
   root-owned interop socket, installed daemon identity, unchanged Stores/machine,
   sealed journal and binary hashes, and healthy scheduling. Whole-VM shutdown
   must change Linux kernel boot; distro terminate may retain the shared VM boot.
   There is no singleton-lock handoff or break-before-make interval. A closed
   external starter console still ends that interactive lifetime; no logout/S4U
   persistence is claimed. The external keepalive survives normal controller
   completion because it was launched outside the controller's Windows Job.
3. **Whole WSL VM shutdown:** after another quiescent/foreign-work preflight,
   a separate native default Job invokes `wsl.exe --shutdown`, records outcome,
   then uses the same fresh approval/exchange/starter and cold-start checks. This affects every running distribution,
   including Docker/other development environments, if present.
4. **Windows sleep/resume:** with external native observations already armed,
   the user triggers sleep and resume during the agreed window. Record Windows
   and Linux monotonic/boot observations before/after, bridge reconnect, fresh
   quiet/observation evidence and a real measurement Job. Do not infer VM suspend
   from a timer delay or SIGSTOP of one process.
5. **Windows logout/login:** the user logs out and back in during the agreed
   window. Retain native/WSL receipts and inspect them after login. The current
   interactive Windows daemon/keepalive may disappear. Restart from installed
   paths and verify continuous histories and shared admission; record manual
   recovery as manual, not automatic scheduled-task lifetime. No S4U guarantee
   is claimed. Host reboot is a distinct operation and needs its own agreed
   interruption and before/after bootevidence.

After each restart controller is final, submit actual Windows and attached WSL canaries through their
installed defaults, record their Jobs and Grants, and verify the one-slot shared
resource observation. Stop on unexpected retained obligations: preserve evidence
and diagnose rather than deleting any Store, anchor or journal.

## Explicit recovery boundary

`Journal::cleanup` currently requires the exact recorded cgroup identity.
`Boundary::reopen` rejects a different Linux boot and an absent/reused cgroup.
Consequently a whole-VM/distro stop during an **unsealed** Invocation can retain
its Grant after restart. Neither a new boot nor disappearance of a path is
currently implemented as an executor seal. This is a conservative limitation,
not successful unattended recovery. A future boot-death proof needs authenticated
VM/host continuity and executor fencing. Existing audited domain retirement is
explicit risk acceptance, revokes the old domain, and is not an automatic
substitute for real cleanup or part of this quiescent procedure.

Acceptance remains open until actual observations, controller Job IDs, recovered
canaries, scope limitations and independent review are recorded. Any failed
restart remains a failed case alongside the successful cases.
