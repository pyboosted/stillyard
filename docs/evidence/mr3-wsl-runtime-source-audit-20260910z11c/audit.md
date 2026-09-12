# Idle helper wait-path audit

This is source/identity evidence, not a replacement for the final installed idle
measurement. Installed WSL reports 2.6.3.0. `/init` and the installed Windows WSL
package's `tools/init` have the same SHA-256 and length (installed-init.json).
Microsoft's matching 2.6.3 source tag is retained with its MIT license; this is
vendor-version attribution, not a locally reproduced binary build.

The WSL2 interop executable proxy follows `CreateNtProcessUtilityVm` in
[binfmt.cpp, tag 2.6.3](https://github.com/microsoft/WSL/blob/2.6.3/src/linux/init/binfmt.cpp).
Its established relay waits with `poll(..., -1)`; the 10-second accept timeout
is startup-only. Stable process identity for the whole interval excludes a
restart through that startup branch. Relay context switches are therefore not
counted as timer expirations. This path also dispatches input/control/signal
activity and closes on remote process exit.

The session relay in
[init.cpp, tag 2.6.3](https://github.com/microsoft/WSL/blob/2.6.3/src/linux/init/init.cpp)
uses an infinite poll when no stdin is queued. Pending stdin uses a 100 ms retry;
this branch must not be assumed absent merely from the source. Zero voluntary
switches of the retained keepalive's Linux init relay during a measured interval
can independently bound that branch's actual timer-driven wakeups to zero.

Stillyard's `scripts/wsl-service.py` waits on signal/pidfd with an unbounded
selector while the daemon is alive. Its timed retry branch is reached only
after failed child/start/teardown. Keepalive itself uses `sigwait`. The final
interval must retain their process identities and observe no such recovery.

The native Stillyard bridge (`src/machine/bridge.rs`) blocks on framed stdin
between requests. During a request the native transport can wait up to one second on a busy
named pipe before retrying until its deadline (`src/client.rs`). Native framed
reads/writes are synchronous; the outer Linux transport enforces its own absolute
response deadline and discards a broken bridge. Busy-pipe retry is a request/failure
path, not an unconditional idle poll, but its actual expirations are not directly
measured by daemon counters. Linux transport-expiration diagnostics and installed
bridge-fault controls cover the outer response deadline only.

The remaining opaque Windows `wsl.exe` keepalive is observed by per-thread raw
context-switch counters, preserving each thread's ID and start identity. A zero
switch delta bounds its actual timer-driven scheduling to zero; nonzero switches
are not relabeled as timer expirations and need separate interpretation.

The final report must combine this audit with interval-specific process/counter
observations. The daemon timer sum alone is not an aggregate-helper verdict.
