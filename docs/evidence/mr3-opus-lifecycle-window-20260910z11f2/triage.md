# Follow-up lifecycle review disposition

The unmodified review is retained. Its material source-level identity and
post-fault robustness findings were addressed in g/h prepared bundles:
exact native image/creation-time/start-owner binding; actual Linux keepalive PID
under its interop init; private anchor mode/owner and active delegated unit cgroup;
preflight schema validation; all four source hashes retained; explicit barriers;
600-second recovery with JSON/transient stopped-list retry; record/terminate only
an unexpectedly contained child created by the starter. h also records/checks the
native installed binary hash across recovery.

The principal claim about --list --running --quiet is not applicable to installed
WSL 2.6.3. Its matching WslClient.cpp branch emits the empty-list message/error
only when !options.quiet && distributions.empty(); the quiet loop over no results
returns 0. See vendor source retained in the h prepared bundle. This is source
attribution, not a live shutdown result. A bounded 60-second stopped-list settle
loop was nevertheless added in g.

The exchange/stdout files are owner-controlled harness evidence, not a new
cross-user authorization protocol. Source hashes, the starter's actual Popen
identity, native image/creation-time, and actual Linux helper/init relationship
are now checked. A malicious same-owner writer is outside this harness model.
Closing the external starter's console can end its keepalive: the procedure
explicitly declares interactive lifetime and does not claim logout persistence.

The controller cannot prove that foreign work will never start after a snapshot;
an agreed freeze of new submissions is a precondition. No disruptive Job has
been submitted, and no approval file exists. Actual terminate/shutdown, recovery
and subsequent canary results remain pending.
