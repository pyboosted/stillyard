# Attestation repair review disposition

Actual Opus Job L`01a08b3d-7337-7541-ad45-c5cbd0f7bf3c` confirms tuple,
nonce/signature, real-peer containment and independent primary-only authorization.

The suggested availability change is rejected: root/current predicates were
already computed outside the candidate WHERE clause in z11d. The change only
includes all roles. Prepared containments start as `creating`, not `live`.
Unknown possibly live boundaries deliberately prevent classifying a peer as
unmanaged; filtering them out would enable precisely the bypass this repair
closes. Missing history remains a conservative incident; exact sealed journal
records already provide the safe negative membership fallback. No demonstrated
new permanent live-boundary failure was supplied. This restriction is retained.

Unsigned pre-authentication failures remain deliberately untrusted and generic.
Showing explicitly labelled bounded unsigned detail is a diagnostic improvement,
not a remaining authentication correctness blocker. No unsigned response is
accepted as authenticated evidence. The live W-C3 old failure is retained.

Same-Job simultaneous Windows containment matches were not shown reachable:
postcondition release requires empty primary, retries await prior cleanup,
and probe and work use separate sequential lifecycles. Ambiguity stays fail-closed;
no speculative lineage relaxation is made. Existing native membership tests
cover distinct nested Jobs.

Accepted the meaningful additional control: the prepared installed regression
runs an actual probe and postcondition, each reads authenticated status and sends
raw environment-independent SubmissionContext/Submit requests which must be
rejected by kernel-based primary authority. The model role mutation alone is
not labelled live probe evidence.

Canonical serde round-trip is used on both ends; changing signed bytes is a
protocol redesign unsupported by a reproduced failure. Machine exchange carries
its own paired session/MAC/epoch authentication and is outside this repair;
executing the installed image does not reveal pairing keys. Full MR3 exit review
remains separate.
