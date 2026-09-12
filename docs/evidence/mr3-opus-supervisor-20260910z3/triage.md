# Supervisor review disposition (in progress)

Review Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a32-f41b-7ec0-a6fc-8ab1eccf7a2f`. Actual subscription Opus; typed findings retained.

Confirmed supervisor error escape and teardown timeout issues: errors now remain inside the persistent loop, log and retry before any next spawn. Child pidfds are closed on all ordinary failure paths. Control subgroup is recreated before spawn. Single-threaded preexec requirement documented. Installed unit will use Restart=no and TimeoutStopSec=infinity: a control process that cannot be killed retains delegation and blocks maintenance instead of causing systemd to discard evidence. Idle has no timer. Explicit stop is permitted by maintenance only after independent journal and kernel barriers; arbitrary unit stop/supervisor death remains a separate unaccepted fault.

Linux reconciliation review lacked registry/journal context: registry::reconcile returns only ProvenEmpty after exact record identity and journal.cleanup; it never returns BoundaryNotEmpty. Thus flattening does not currently discard an observed BoundaryNotEmpty. Explicit force-clear of uninspectable prior-generation Linux boundaries needs further scrutiny; no force-clear is used for acceptance. Commit-race audit attribution is retained for correction. Host/boot proof resides in journal/cgroup identity checks and needs full-context disposition.

Initial-install candidate copy verification and keepalive signal publication findings retained for follow-up. This review is not an MR-3 exit verdict.
