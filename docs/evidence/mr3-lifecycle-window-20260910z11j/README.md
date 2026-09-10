# Prepared lifecycle window j

Native default Windows preflight Job
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a08b9f-e7b3-71d0-b8f0-be7a634f1b9d`
succeeded. Only preflight was submitted. Terminate/shutdown specifications remain
UNSUBMITTED and no approval.json exists. The template is non-authorizing.

This bundle retains the reviewed i controller/starter safeguards and corrects
its containment inventory. The earlier i preflight did not execute the empty
barrier and counted two historical automatically cleared containments as
nonempty. That observation remains in
[the earlier evidence](../mr3-lifecycle-window-20260910z11i/preflight-after-idle/).

j uses the exact accepted `scripts/wsl_maintenance.py` SQL barrier, included in
the controller's recorded source hashes. It briefly reserves admission using
BEGIN IMMEDIATE and rolls back; it performs no data-changing SQL. A historical
cleared record is accepted only after checking automatic proven_empty resolution,
absence of forced acceptance, released Lease audit and matching actual journal
Invocation/containment, seal and boundary digest. The journal checksum and
recursive kernel emptiness remain separate required checks. Preflight now calls
the same empty predicate used before interruption and retains its clearance.

Observed: zero active Leases, blocking containments and unsealed Invocations;
executor cgroup populated=0 with no children. Two historical records have their
actual seal IDs and boundary digests in
[preflight-clearance.json](preflight/result/preflight-clearance.json).
Ubuntu-SSD was the only running distribution; the inventory found 98 selected
foreign-work process candidates. This preflight grants no permission to stop
those processes. Record a fresh inventory and the user's explicit interruption
agreement before submitting either destructive operation.

The prepared window covers distro terminate, recovery, whole-VM shutdown,
recovery and subsequent shared-slot canaries. Sleep/resume, logout/login and
reboot require separate prepared observations and agreement. Full procedure:
[controlled interruption window](../../mr3-lifecycle-window.md).
