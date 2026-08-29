# Managed-child capability policy and tree views — review disposition

Date: 2026-08-29
Reviewed baseline: 37acffc
Reviewed brief: docs/managed-child-policy-and-tree-views.md, untracked review candidate
Disposition: all verified initial findings accepted; focused closure review required before freeze

## Review provenance

- Fable: claude-fable-5, xhigh, subscription OAuth through claude-current; session
  da9b0036-59cf-4668-8bc5-61b1787644e6. Verdict artifact:
  C:\Users\User\AppData\Local\Temp\fable-stillyard-managed-child-tree-20260829.json.
- Grok: exact grok-4.6, high reasoning effort, xAI OAuth subscription, no API-key billing.
  Verdict artifact:
  C:\Users\User\AppData\Local\Temp\grok-stillyard-managed-child-tree-20260829.json.

The artifacts are local evidence. This disposition and the revised brief are the durable project
record.

## Initial verdicts

- Fable: not implementation-ready; eight blocking contract gaps and bounded follow-up notes.
- Grok: ACCEPT-WITH-FIXES; six High and two Medium findings.

The reviewers independently converged on fence identity, rejection routing, global-log cursor
invalidation, incomplete tree API, selector-bound child expansion, and bounded tree work. Every
claim below was checked against the current source before disposition.

## Accepted findings

| Finding | Disposition in revised brief |
|---|---|
| Existing exact fence keys cannot prove nested containment and path prefixes follow replacements | Accepted. Policy roots persist volume/file identity plus remaining components. Requested paths produce ancestor candidates; containment is identity plus component prefix. Path keys never authorize. |
| Global event-head cursors go stale on every LogCommitted event | Accepted. Root continuation uses a non-pruned durable tree_order_revision advanced only by ordering/membership changes. Immutable child order uses no event pin. |
| Ordinary ResourceClaims lists remain unbounded | Accepted. Version 2 adds count and byte bounds to claims, labels, impacts, custom names, and fence paths. |
| Maximum legal pages can exceed the 16 MiB frame | Accepted. Maximum fixtures prove flat/public sizes, tree pages cap nodes and stop at an 8 MiB encoded budget. |
| Child-policy codes collapse to rejected or protocol error | Accepted. StoreError::OperationRejected is routed through durable decision, recovery resume, RPC, client, result file, JSON, and exit 27. |
| Recovery resume can abort startup on a new policy error variant | Accepted. OperationRejected is an expected completed rejection and has a restart regression. |
| Identity/policy split made the brief's Unknown rule contradict R-SUB-5 | Accepted. Live identity returns NotReceived for an absent row even with None policy; replay may retain only child_submission_disabled and never create a Job. |
| Child expansion did not bind its selector | Accepted. JobChildrenCursor carries selector_hash, advances over skipped physical children, and returns only eligible descendants. |
| JobChildrenPage and selected-node metadata were undefined | Accepted. Both public response shapes and tree_for_job path/progress behavior are explicit. |
| observe_trees and Gap bounds were undefined | Accepted. The full signature and exact selector/root/node/depth snapshot semantics are explicit. |
| Mutable family classification was unbounded under SharedStore mutex | Accepted. Tree reads use a daemon-owned read connection, three indexed passes, a depth bound, and a fail-closed scan budget. |
| Version 2 breaks the JobSpecs used to build and install version 2 | Accepted. Delivery now defines the alpha.8/v1 build, idle promotion/epoch reset, JobSpec v2 update, and complete alpha.9 rerun. |
| The brief called cascade an existing implementation | Accepted. Alpha.8 cancel is explicit-ID only; frozen cascade remains undelivered and out of scope. |

## Accepted lower-severity controls

- Managed membership must select the immediate policy-None parent, never an outer enabled parent.
- validate_current_parent is split into identity and policy responsibilities; managed wait still
  requires an enabled policy.
- An empty present policy is explicitly the claimless-child envelope.
- CLI file decoding inspects spec_version before decoding removed version 1 fields.
- Alpha.9 orphan behavior is a forward-compatible Store fixture, not a false retention claim.
- Derived postcondition specs clear child_submission_policy.
- Nonmatching descendants of a matching anchor are selected subtree content; unrelated siblings
  remain excluded.
- Filtered families may have hidden activity, which is shown honestly at the root.
- Protocol 14 is published only as the complete alpha.9 surface; intermediate slice commits are
  not consumer pins.

## Gate consequence

Implementation may start only after focused Fable and Grok closure reviews find no remaining
Critical or High contract hole. Any material closure finding is incorporated and reviewed again
before the brief status becomes frozen for implementation.

## Closure round 1

Fable resumed the same claude-fable-5 xhigh session. Grok ran a new exact grok-4.6 high pass.
Both independently found the same remaining High: pure intersection of stored fence tuples cannot
relate an existing parent directory to an existing delegated subdirectory because their stable
file IDs differ. The request-time containment algorithm was correct, but materializing its result
back into a tuple set would false-deny the grandchild.

The High is accepted. Fence intersection is now a predicate evaluated for the requested fence
against every policy-bearing ancestor using one candidate walk. Stored policy tuples are never
intersected with each other and display paths never authorize.

All closure Medium/Low findings are also accepted:

- tree_order_revision is a non-pruned durable meta counter read in the same SQLite snapshot as the
  page, not a query over the pruned events table;
- tree_children uses additional_depth, where zero returns immediate children;
- scan-limit overload has no immediate-retry promise and watch visibly falls back to the bounded
  flat page;
- promotion explicitly terminates the verified idle alpha.8 daemon because no stop command exists;
- tree_node_limit_too_small is InvalidSpec/64 and tree_for_job cursors bind the exact Jobs selector;
- candidate walking pins ordinary reparse opening rules and owner-policy I/O failure is fail-closed;
- the TUI follows tree pages up to its existing visible-Job budget;
- tests cover existing delegated subdirectories, junctions, event pruning, depth-zero expansion,
  and scan-limit fallback.

Closure-round-1 artifacts:

- Fable:
  C:\Users\User\AppData\Local\Temp\fable-stillyard-managed-child-tree-closure1-20260829.json.
- Grok:
  C:\Users\User\AppData\Local\Temp\grok-stillyard-managed-child-tree-closure1-20260829.json.

A second narrow closure is required because the fence authorization and cursor revision contracts
changed materially.

## Closure round 2

Fable resumed the same claude-fable-5 xhigh session. Grok ran a new exact grok-4.6 high pass.
Both found no remaining Critical or High finding. Grok returned IMPLEMENTATION-READY/PASS; Fable
said the brief could freeze after two paragraph-level corrections.

All residual Medium/Low findings are accepted before the final confirmation:

- a same-mode fence candidate match also rejects any reparse component strictly between the
  matched policy ancestor and requested leaf; the leaf may itself be fenced as a reparse object;
- tree_order_revision is advanced by two schema-validated SQLite triggers for inserts and actual
  ordering/parent changes, rather than by distributed Rust call sites;
- omitted tree_children additional_depth means zero and therefore returns immediate children;
- tree_children shares the 16,384-row scan budget, including skipped filtered siblings;
- the TUI follows both root and truncated-branch child cursors, completing each represented family
  within its existing visible-Job budget;
- the earlier shorthand about an effective fence root is removed so only the per-request,
  every-ancestor predicate states the authorization contract.

Closure-round-2 artifacts:

- Fable:
  C:\Users\User\AppData\Local\Temp\fable-stillyard-managed-child-tree-closure2-20260829.json.
- Grok:
  C:\Users\User\AppData\Local\Temp\grok-stillyard-managed-child-tree-closure2-20260829.json.

A final narrow confirmation checks only these accepted edits. No architecture question is reopened.

## Closure round 3 and frozen disposition

Fable resumed the same claude-fable-5 xhigh session and returned implementation-ready/freeze
approved with no Critical or High defect. Grok ran exact grok-4.6 high and returned
implementation-ready/freeze approved with an empty findings list.

Two optional Fable wording improvements are accepted without reopening the gate: an intermediate
reparse disqualifies that candidate rather than precluding a separate legitimate candidate, and
the child_fence_not_permitted table now names failure of the every-policy-ancestor containment
predicate instead of the removed effective-root shorthand.

Closure-round-3 artifacts:

- Fable:
  C:\Users\User\AppData\Local\Temp\fable-stillyard-managed-child-tree-closure3-20260829.json
  (session da9b0036-59cf-4668-8bc5-61b1787644e6; modelUsage claude-fable-5).
- Grok:
  C:\Users\User\AppData\Local\Temp\grok-stillyard-managed-child-tree-closure3-20260829.json
  (grok-4.6, high; findings empty).

The gate is closed. The brief is frozen for implementation.

## Implementation clarification

The first TUI integration exposed one missing evidence field: root order alone does not tell a
filtered client which aggregate attention bucket contains a family, especially when the active
branch is hidden. JobTreeNode therefore carries family_attention: Option<TreeAttentionBucket>,
Some only on roots. This is the minimal public evidence needed to implement the already frozen
server-classified grouping and hidden-activity contract; it adds no authority and changes no
admission or cursor semantics.

Implementation also made two cursor/query mechanics explicit before shipped review:

- JobChildrenCursor identifies the next unreturned eligible physical child and continuation is
  inclusive, which gives a representable first-child cursor when depth or node budget cuts before
  any child is emitted. The token still binds the exact normalized selector and never exposes a
  filtered sibling.
- Instead of the brief's proposed three indexed classification passes, alpha.9 uses a separate
  read-only connection and one coherent, bounded summary-model scan of at most 16,384 rows. Row
  16,385 fails before classification, so the safety, scheduler-isolation, and memory bounds are
  preserved; the tradeoff is that a narrower selector cannot bypass an oversized retained store.
  The implementation review must explicitly approve or reject this bounded simplification.

The encoded response implementation additionally reserves worst-case selector-cursor bytes per
node and truncates with continuation before 8 MiB, followed by an exact fail-closed serialization
check.

## Implementation review round 1

Fable ran a fresh claude-fable-5 xhigh implementation review and Grok ran an exact grok-4.6 high
audit of the policy/tree risk surface. Both returned REQUEST_CHANGES. The bounded read-only
single-scan implementation clarification above was explicitly approved by both reviewers.

Confirmed findings were accepted and corrected:

- every child_* RPC decision is now written immediately as RecoveryResult::Rejected in the result
  file, with a direct regression;
- an invisible depth-cut child interleaved before a visible connector stops the physical-child
  walk and exposes an inclusive continuation, so no eligible child becomes unreachable;
- tree_for_job stops at an unavailable retained parent and returns the child as an explicit
  orphan family root;
- the 64-Job managed-ancestry bound now counts the proposed child and is tested at both sides;
- requested/delegated fence resolution I/O propagates as an ordinary invalid specification;
  only a successful non-match becomes a child fence denial/escalation;
- TUI context ancestors are dimmed, orphans are explicit, effective policy evidence is shown,
  user expand/collapse overrides survive refresh, and watch --job observes future descendants;
- CLI tree indentation skips the synthetic root guide column;
- schema validation requires parent/state/accepted-order tree indexes, and the accepted-order
  index makes SQLite's LIMIT apply to the physical scan order;
- deterministic mutants now cover row 16,385, encoded-budget root continuation, mixed-depth child
  continuation, orphan focus, 64/65 ancestry, tree indexes, and TUI expansion persistence.

The suggestion to add a distinct tree_response_limit wire code is declined: the frozen public
contract intentionally has two read-view codes, tree_cursor_stale and tree_scan_limit, mapping
all bounded-view availability failures to Error::ViewUnavailable/exit 69. The exact detail retains
the tree_response_limit prefix for diagnosis. Other Low performance hardening remains optional.

Round-1 implementation artifacts:

- Fable:
  C:\Users\User\AppData\Local\Temp\fable-stillyard-managed-child-tree-implementation-20260829.json
  (session d9a85502-fcdf-4faf-8531-a9096d750f97; modelUsage claude-fable-5).
- Grok:
  C:\Users\User\AppData\Local\Temp\grok-stillyard-managed-child-tree-implementation-20260829.json
  (grok-4.6, high).

A focused closure review is required on the corrected implementation before promotion.

## Implementation closure

Fable resumed the implementation session and found every round-1 High/Medium closed except one
mechanical client-variant regression: child_* had also been added to the earlier
ManagedWaitRejected match arm. Grok approved the corrected implementation surface but noted the
same line as optional hardening; it was treated as blocking because the frozen contract requires
Error::Rejected. The stray match was removed and response_errors_preserve_known_wire_rejections
now pins child_claim_not_permitted to Error::Rejected. The full test Job passed afterward.

A final one-line confirmation then returned APPROVE from both reviewers with no findings:

- Fable:
  C:\Users\User\AppData\Local\Temp\fable-stillyard-managed-child-tree-implementation-closure2-20260829.json
  (session d9a85502-fcdf-4faf-8531-a9096d750f97; modelUsage claude-fable-5).
- Grok:
  C:\Users\User\AppData\Local\Temp\grok-stillyard-managed-child-tree-implementation-closure2-20260829.json
  (grok-4.6, high; findings empty).

The implementation review gate is closed. Platform-specific reparse candidate behavior remains
covered by the shared identity resolver's Windows tests and code review rather than a junction
fixture that depends on host symlink privilege. TUI child-continuation completion and flat-fallback
signalling are additionally exercised during installed-image acceptance; this limitation is
recorded rather than represented as deterministic unit coverage.
