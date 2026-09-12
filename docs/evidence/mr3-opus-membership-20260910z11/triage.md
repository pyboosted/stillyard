# Membership fix review disposition

Independent review confirms correct journal-before-active lock ordering, no Store
lock inversion, and no false negative membership from exact retained seals.
The actual kernel mutant demonstrates the cleanup publication race.

Accept the material extra negative control: an existing but unsealed journal
record, absent from active, must remain unknown. Also assert a default registry
is unknown. These are in z11c before final gates; no product semantics changed
after z11b. Accepted a comment documenting that peer authentication may wait
up to the cleanup deadline.

Other points are nonblocking: inspect remains conservative during cleanup,
reconcile uses the same journal lock, a timeout in a protected test fails the
test, and sealed active-map history is bounded by retained journal capacity.
No unsafe unknown-to-empty fallback or force-clear bypass is introduced.
The observation that a panic poisons both locks is fail-closed and is not a
concrete panic path. This review is scoped; MR3 exit remains pending.
