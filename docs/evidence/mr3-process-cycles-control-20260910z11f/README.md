# Lifetime native process cycles control

A real native default Job measured QueryProcessCycleTime before and after a
worker thread was created, consumed CPU and exited. The worker measured its own
QueryThreadCycleTime delta. The post-exit process delta includes at least that
worker delta, verifying lifetime accumulation on this host. This closes the
endpoint-only thread-inventory hole for the opaque keepalive: require ZERO
process cycle delta in addition to stable native identity and zero thread switch
deltas. Nonzero cycles are not converted to time or labeled as timer counts.

Primary API contracts:
[QueryProcessCycleTime](https://learn.microsoft.com/en-us/windows/win32/api/realtimeapiset/nf-realtimeapiset-queryprocesscycletime)
and [QueryThreadCycleTime](https://learn.microsoft.com/en-us/windows/win32/api/realtimeapiset/nf-realtimeapiset-querythreadcycletime).
The latter explicitly warns against cycles-to-time conversion; none is performed.

This is metrology control, not an idle acceptance interval. The separately
retained observer checks the same process creation FILETIME across probes.
