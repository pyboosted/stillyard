# MR-2 audited domain retirement — implementation design

Status: implemented in the working tree, under scheduled validation; uninstalled
and not yet accepted. Scope is the
remaining MR-2 liveness gap when a paired manager's continuous store/proofs are
irretrievably unavailable. Ordinary reconnect and lost acknowledgements use the
already implemented recovery path and do not invoke risk acceptance.

1. An owner-authenticated unmanaged administrative client obtains an exact
   domain clearance preview: machine/authority epoch, installation identity,
   manager store UUID, current pairing state and every Armed/Uncertain Grant
   including its complete issued Invocation intent inventory. A checksummed
   inventory digest binds this preview; no pairing secret is returned.
2. `machine retire-domain --spec <request>` is explicit. The request
   has stable operation UUID, domain, expected inventory digest and a bounded
   reason and required `accept_risk` boolean. The server records the actual
   Windows peer SID and process identity, not an
   identity asserted by the JSON caller. A changed preview rejects before mutation.
   This operation is not exposed as an executor-authorized HMAC operation.
3. Retirement covers the entire failed manager registration. Before any debit is
   freed, an external pending administrative commit revokes its session and marks
   the registration retired. All issued rights stay charged until SQL records the
   retirement. The old installation/domain cannot authenticate or pair again.
   Reuse requires a fresh domain/installation/store identity. Other domains and
   native Windows grants keep their accounting and sessions.
4. The external immutable audit retains the exact preview, risk statement, reason,
   operation and requester. SQL grants become Released with an explicit risk-
   clearance annotation, not a fabricated SealedRelease or ProvenEmpty proof.
   Offered candidates can be expired, all candidates/reservations of the retired
   domain withdrawn. Public participant/Grant/event surfaces expose retirement
   and its audit identity. The old manager remains a historical store; it cannot
   turn absence from a new inventory into a fresh launch permission.
5. Retirement uses the same publication ordering as machine commit: flush audit /
   pending commit, publish external fence, commit SQL changes, finish external
   obligation retirement, acknowledge. Each boundary must be fault-injected in an
   isolated default-daemon test Job. Replays use the exact operation ID/payload;
   a reset reloads the pending fenced decision and completes it before admission.
   A lost response never requires issuing another clearance.
6. A pending registration with no committed session/grant can be explicitly
   abandoned without accepting process-cleanup risk; it has never had launch
   authority. The same retirement/fencing mechanism records that distinction.
7. A coordinator SQL reset can then converge after all remaining live participants
   reconcile and native obligations have platform proof. This domain operation
   must not silently clear unrelated native uncertainty or a missing registry.
   Same-store rollback or uncovered native history needs a separately explicit
   inventory-level remediation; assess what MR-2 requires before adding a broad
   force-reset operation. Deleting registry/anchors is never a recovery action.
8. Administrative history is bounded without forgetting active fences. Retired
   registration records retain receipts and immutable audit references. They
   cannot be discarded on executor acknowledgement or an ordinary authority
   epoch transition. The registry reserves 16 KiB per admitted live participant
   for its bounded administrative receipt and 64 KiB per live hold for its future
   release record, in addition to commit headroom. Every increase in encoded bytes
   plus reservations is checked; late metadata cannot spend another obligation's
   reserved cleanup space. AuthoritySnapshot exposes the byte budget and headroom.
   A maximum of 4096 retained/live identities and the registry byte budget stop
   new pairings; already admitted obligations retain retirement space. Safe
   maintenance/compaction of these identity fences is still an open item. No
   implemented command currently permits deleting them to regain capacity.

Review question: Is whole-domain revocation the smallest defensible clearance
scope for manager-store loss, and what precise durable representation and recovery
steps avoid reviving a retired writer or clearing unrelated obligations? Identify
holes in this design and indispensable executable negative controls. Do not claim
that currently unimplemented WSL process cleanup is proven by these protocol tests.
