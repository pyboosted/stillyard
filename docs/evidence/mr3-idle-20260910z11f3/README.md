# Installed z11f idle acceptance: PASS

The user closed the external Windows watch before this interval. Both managers
had empty queues and no new retained Jobs during measurement or settlement;
installed image hashes, daemon generations, helper identities and the interop
socket owner matched the accepted pair. The viewer was restored only after the
observer and validator completed (watch-restored.json).

| Quantity | Observed | Limit |
|---|---:|---:|
| Measured interval excluding snapshot collection | 300.0016 s | at least 300 s |
| Aggregate CPU, one logical core | 0.0550% | below 1.1% |
| Aggregate endpoint memory | 67.1406 MiB | below 96 MiB |
| Aggregate timer expirations | 3.2000/min | below 6/min |
| Additional boundary settlement including collection | 54.0368 s | at least 30 s |

All 21 explicit conditions passed. Each daemon also met its 0.5% CPU / 40 MiB
budget and the native bridge its 0.1% / 16 MiB budget. Memory is the specified
Windows private-byte/Linux RssAnon metric sampled at endpoints, not an interval
peak claim. Counter deltas include collection windows while rate denominators
exclude them, giving conservative rates.

The 16 recorded timer expirations were all in the Linux attached driver's
20-second wait. Other daemon buckets were zero. The opaque native keepalive's
lifetime CPU-cycle and persistent-thread switch deltas were both zero; Linux
supervisor/keepalive/init helpers had zero voluntary switches. Established
interop/native bridge event waits are composed through the verified source audit,
unchanged proxy/native identities, zero transport/backoff and settlement checks.
Context switches or CPU cycles were not relabeled as timer counts.

Read acceptance.json for each observed condition, before.json/after.json for
raw snapshots, and settled.json/settlement.json for boundary observations. The
exact observer, native cycle probe and validator are retained alongside them.

Evidence dependencies:
- [Accepted pair, build Jobs and source identity](../mr3-installed-20260910z11f/).
- [Complete helper wait audit and source excerpts](../mr3-helper-timer-audit-20260910z11f/audit.md).
- [Independent review disposition](../mr3-opus-helper-audit-20260910z11f/triage.md).
- [Real exited-thread cycle control](../mr3-process-cycles-control-20260910z11f/).
- [Real attached polling negative control](../mr3-attached-idle-control-20260910z8/).
- [Native busy-pipe old-loop negative control](../mr3-busy-pipe-mutant-20260910z11f/).
- [Installed transport/subscriber/backoff controls](../mr3-installed-timer-controls-20260910z9/).

This completes installed idle acceptance for the identified pair. Whole-VM,
distro, sleep/logout/reboot observations and final MR-3 exit remain separate.
