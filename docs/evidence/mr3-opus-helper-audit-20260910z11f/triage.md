# Complete helper audit review disposition

The reviewer confirms the complete native request/error/teardown chain and the
35-second settlement argument. Its raw findings and canonical Job are retained.

1. Added enumeration/rejection of external installed Linux clients, symmetric
   with the native check. The controlled interval additionally requires no new
   clients/subscribers; endpoint inventories are not a process-creation trace.
2. Accepted the transient-thread hole in the opaque Windows helper observation.
   Added QueryProcessCycleTime lifetime totals, bound to creation FILETIME and
   required to have zero delta. A real native default control measured an exited
   worker's thread cycles and proved the process delta retains them. Existing
   persistent-thread counters remain corroborating data, not sole coverage.
3. verify_pipe_server was omitted from the brief; it contains kernel identity/
   image queries and path canonicalization, no retry or timed wait. Supplemental
   source excerpts retain its complete body and every reactor/subscriber/backoff
   instrumentation site. No uninstrumented Stillyard wait was found on these paths.
4. The observer deliberately reports pending instead of granting aggregate PASS.
   Added a separate validator that emits each condition's observed value and
   pass/fail, checks exact installed images/generations, lifetime helper counters,
   settlement, no subscriber/backoff/transport expiration, full expected helper
   inventory, and the aggregate budget. No clean interval exists yet; validation
   against such an interval remains pending the user's watch pause.
5. The memory criticism uses a different metric from the agreed MR-0 norm:
   protocol section 9 explicitly specifies private/RSS-anonymous memory. The
   report keeps that metric and explicitly says endpoint samples, not interval
   peak. Full RSS or all file-backed pages would change the agreed metric.
6. The one-second fallback is counted and would fail acceptance. Normal
   attached_poll_interval returns 20 seconds when there is no uncommitted,
   unreleased attached local plan (otherwise 100 ms); that source is retained. No rate is assumed to
   pass: the actual final delta must satisfy the validator.

No final idle PASS or completed MR-3 is asserted. User watch PID 44148 remains
open; permission to close it temporarily is pending. No VM fault is authorized.
