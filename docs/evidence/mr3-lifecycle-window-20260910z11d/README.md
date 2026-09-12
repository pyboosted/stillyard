# Prepared interruption window

Only read-only preflight was submitted. Corrected preflight Job
W`01a08b2b-28b8-74b3-9db2-720b48ba6a7a` passed; it observed 153 selected
foreign processes in Ubuntu-SSD and zero local Stillyard Leases/unsealed
Invocations at capture. This is an inventory, not M-A12 acceptance.

Initial preflight W`01a08b2a-5834-7ea0-acd7-70fc192d1fc2` failed a controller
path assertion before any WSL operation: the installed Windows Store ends in
`Stillyard/data`, not `Stillyard`. The corrected exact-path assertion passed.
Both scripts and canonical outcomes are retained.

`prepared/terminate/spec.json` and `prepared/shutdown/spec.json` are concrete
**unsubmitted** default Windows Jobs, each with its own stable intent/key.
Their operational originals remain under
`C:\Development\stillyard-mr3-lifecycle-window-20260910z11d2`.
They must await the scheduling plan's separate agreed interruption window.
The controller requires its actual native primary identity, empty user queues,
no outstanding guest obligations or bootstrap Holds and continuous histories.
A cold-start keepalive remains owned by that native controller Job until the
external agent explicitly hands it off; no descendant is detached from the Job.
After this handoff, collect actual installed native/WSL canaries separately.
The controller does not claim live-work boot cleanup or whole M-A12 acceptance.

Sleep/resume, logout/login and host reboot remain separate manual window steps
with durable before/after observations; no UAC/S4U registration is retried.
See ../../../mr3-lifecycle-window.md for scope and the unsealed-boot recovery limit.
