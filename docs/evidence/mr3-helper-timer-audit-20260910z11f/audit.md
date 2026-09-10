# Complete helper timer-expiration accounting for installed z11f

This source argument must be combined with a measured interval; it is not itself
an idle PASS. Source file map: 9dd388044392f8b7bd991321f5dd80857e449667dc4abd12aabb0d56b3466af0.
Matched gate Jobs are in ../mr3-membership-20260910z11f/jobs.json and installed
image identities in ../mr3-installed-20260910z11f/. It supersedes the native-client
paragraph of ../mr3-wsl-runtime-source-audit-20260910z11c/audit.md; that directory
retains the matching WSL 2.6.3 vendor source, binary/version attribution and MIT license.

## Native Stillyard bridge

machine::bridge::run dispatches six commands. machine_connect_begin,
machine_connect_finish, machine_exchange, machine_participant, authority_status,
and daemon_status all reach Client::request with cancellation=None. Client::request
spawns the transport worker and performs one recv_timeout for the full remaining
deadline. Its separate 25 ms cancellation loop is not selected. Builder startup
ping/retry paths are excluded only by stable process identity throughout the
interval, not assumed absent from the binary.

transport_request opens the pipe, authenticates the server image, then uses
synchronous framed reads/writes. For ERROR_PIPE_BUSY its WaitNamedPipeW receives
the finite remaining deadline (1..MAX-1). ERROR_SEM_TIMEOUT returns DeadlineElapsed;
other wait failures return Unavailable. A successful readiness wake can race with
another opener, recompute the remaining deadline and wait again, but it is an I/O
wake, not a timer expiration. Real occupied-pipe old-loop mutant and fixed gate
results establish removal of the former one-second timeout/retry path.

Either a receiver deadline or a native busy-pipe timeout ends the bridge command
with an error. serve serializes BridgeOutcome::Error; protocol::write_frame flushes.
Linux Bridge::exchange returns Err; every Driver call propagates it, and attached's
error branch sets driver=None, drops Bridge, kills its direct Linux child and
records reconnect backoff. If native response publication stalls, Linux TimedIo's
own deadline expires, is counted and triggers the same driver teardown.

The final interval must show unchanged Linux proxy PID/start ticks and native
bridge PID/creation time, unchanged daemon generations and zero Linux transport/
backoff expirations. Those observations exclude this error/teardown path. They do
not depend on SIGKILL of the proxy necessarily killing a native orphan. Boundary
in-flight requests must also be allowed to settle before this conditional argument
is accepted; an error pending at the final snapshot cannot be silently excluded.
No claim is made that no timed wait was armed; the metric is actual expirations.

## Other helpers

The established WSL interop proxy waits in poll(-1), per matching vendor source.
Its 10-second accept is startup-only and excluded by stable identity. The init
session relay can use a 100 ms pending-stdin retry; require zero actual voluntary
context switches for that exact relay to bound its timer expirations at zero.

wsl-service.py's healthy supervisor blocks in an unbounded selector on signal/
pidfd; timed retries require child/start failure. Require stable daemon and helper
identities, no recovery and zero helper voluntary switches. The keepalive blocks
in sigwait. The opaque Windows wsl.exe is not attributed by source: require exact
per-thread identities and zero raw context-switch deltas. Nonzero switches cannot
be relabeled as timers or discarded without further evidence.

## Composition

Add actual expiration deltas from both daemon metric sets (Windows reactor,
subscriber, backoff; Linux reactor, attached, subscriber, transport, backoff).
Retain inapplicable buckets separately and require zero. Add helpers' bounded
actual expirations only after the conditions above hold. Linux poll rounding may
produce multiple expirations for one logical timeout: retain them all. Compare
this aggregate against 6/min, with CPU/memory budgets separately measured. An
unresolved helper or boundary interval means pending, not PASS.

## Follow-up: process lifetime totals and executable composition

The independent review identified a real hole in endpoint thread lists: a worker
can start and exit between samples. The opaque Windows keepalive now additionally
requires zero QueryProcessCycleTime delta, bound to the same creation FILETIME.
A real native Job proved exited worker cycles remain in that lifetime total;
see ../mr3-process-cycles-control-20260910z11f/. No cycles-to-time or nonzero
cycles-to-timer conversion is used. The original zero persistent-thread-switch
condition alone is insufficient.

Both installed Linux and native external clients are rejected by snapshot
inventory. The controlled interval must have no newly opened clients; snapshots
are not a general process-creation trace. verify_pipe_server and complete daemon
reactor/subscriber/backoff call sites are retained in this directory, together
with attached_poll_interval's 20-second empty-state branch.

`scripts/validate-installed-idle.py` performs explicit per-condition composition
against a real interval and accepted installation plan. Its resulting acceptance
artifact must be retained; observer success alone still means CPU/memory only.
The report expressly measures endpoint private/RssAnon memory, as the MR-0 norm
specifies, and does not claim to measure interval peaks.
