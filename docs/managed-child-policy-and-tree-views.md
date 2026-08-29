# Managed-child capability policy and tree views — implementation brief

Status: frozen for implementation after Fable xhigh and Grok 4.6 high closure
Date: 2026-08-29
Baseline: 37acffc, Stillyard 0.1.0-alpha.8, spec version 1, protocol 13
Target: Windows v0.1 alpha.9, spec version 2, protocol 14

## 1. Objective and decision

A managed parent Job must be able to declare the maximum capabilities that its descendants may
request without reserving those resources in the parent's Lease. Managed Jobs must also be
observable as parent/child trees through the public crate, CLI, and TUI.

The declaration is an authorization envelope, not capacity and not a Lease:

- it consumes no resource and does not delay the parent;
- a permitted child makes an ordinary claim and receives an ordinary independent Lease;
- an out-of-policy request becomes a durable rejected Submission before any Job, Attempt,
  dependency, artifact, or Lease row is created;
- a permitted request may still queue or fail under ordinary host capacity, impact, fence,
  observation, quiet, and managed-wait rules;
- the boundary remains cooperative same-owner orchestration, not an OS sandbox.

This is a public requirements revision after the delivered v0.12 phases. It does not reopen
authenticated OS parentage, Submission idempotency, ordinary Lease arithmetic, managed-wait
deadlock checks, containment, or the observed/quiet admission state machine.

Delivery has two independently reviewed slices:

1. managed-child capability policy, durable admission evidence, schema, and protocol;
2. bounded tree read model, tree observation, CLI rendering, and TUI forest.

Both slices ship in alpha.9. Moot repins only a published alpha.9 commit and does not modify or
simulate Stillyard policy in its current batch.

## 2. Compatibility and version boundary

This increment deliberately makes a versioned breaking JobSpec change:

- SPEC_VERSION becomes 2 and the generated fixture becomes schema/stillyard-spec-v2.json;
- JobSpec replaces allow_child_submissions with child_submission_policy;
- protocol becomes 14;
- the crate and binary become 0.1.0-alpha.9;
- the greenfield SQLite schema epoch changes and an older epoch is reset by the existing
  whole-store rule; no migration is introduced.

The alpha.9 daemon accepts only spec version 2. An alpha.8 client fails the protocol handshake
instead of silently submitting version 1. The alpha.9 CLI reads the top-level spec_version from a
file before decoding the version 2 body, so a version 1 file receives an explicit unsupported
spec-version error even though its removed fields would also fail deny-unknown-fields decoding.
Direct serde use of JobSpec remains a typed Rust API and makes no separate diagnostic promise.

Existing command meanings and the flat JobSummary, JobListPage, and list JSON shapes remain
compatible. The status command remains a JobSnapshot command, but JobSnapshot.spec now contains
the version 2 JobSpec and is therefore a declared versioned schema break. No byte-for-byte or
field-for-field compatibility is claimed for the nested spec object.

Changing policy data changes normalized payload hashing and result-file identity. Policy data is
persisted with the immutable accepted Job and survives daemon restart byte-for-byte in the
accepted spec.

Spec version 2 also makes Job label keys unique. Duplicate keys are invalid even when values are
equal. This removes ambiguity from mandatory descendant labels.

## 3. Public policy contract

JobSpec contains:

    pub child_submission_policy: Option<ChildSubmissionPolicy>

None means that a process inside the primary Invocation may be authenticated as managed but is not
authorized to create children. Some(policy) authorizes child submission within the envelope.

    pub struct ChildSubmissionPolicy {
        pub max_claims: ResourceClaimLimits,
        pub allowed_impacts: Vec<String>,
        pub required_labels: Vec<Label>,
        pub fences: ChildFencePolicy,
        pub allow_observed: bool,
        pub allow_quiet: bool,
        pub allow_delegation: bool,
    }

    pub struct ResourceClaimLimits {
        pub cpu_units: Option<u32>,
        pub ram_mb: Option<u64>,
        pub cargo_slots: Option<u32>,
        pub gpu_slots: Option<u32>,
        pub custom: BTreeMap<String, u64>,
    }

    pub struct ChildFencePolicy {
        pub shared_roots: Vec<PathBuf>,
        pub exclusive_roots: Vec<PathBuf>,
    }

All three types use serde default and deny unknown fields. An absent scalar or custom key means
zero capability. Empty allowed_impacts permits no impacts. Empty fence roots permit no fences.
False booleans deny the corresponding optional admission policy.

The parent Job's own ResourceClaims are independent. Allowed cargo_slots, gpu_slots, impacts,
observed thresholds, quiet policy, or fence roots are never copied into or debited from the
parent's Lease.

The first moot policy is expected to permit one cargo slot and cpu_heavy, require its exact project
label, allow only the shared repository metadata root needed by the child, deny GPU, observed,
quiet, and exclusive fences, and deny further delegation. Reviewer Jobs retain only their account
scalar and their own actual filesystem fences.

## 4. Shape validation, bounds, and resolved fence identity

Pure, filesystem-free policy shape validation occurs as part of JobSpec version 2 validation:

- every present scalar and every custom maximum is positive;
- custom names use ResourceClaims rules, including built-in-name exclusion and canonical
  vram_mb GPU UUID rules;
- allowed impacts use the existing policy-name grammar and are unique;
- required label keys are unique and every label uses the version 2 label bounds;
- shared and exclusive root spellings are independently unique before path resolution;
- every root is nonempty, absolute, contains no NUL, and is at most 512 UTF-8 bytes in its JSON
  representation;
- allow_delegation controls only further policy carriage; it does not reserve aggregate family
  capacity.

Version 2 bounds are chosen together with the 16 MiB protocol frame:

- at most 16 custom maxima;
- at most 16 allowed impacts;
- at most 32 required labels;
- at most 8 roots in each fence mode;
- label keys are 1 through 64 UTF-8 bytes;
- label values are 1 through 128 UTF-8 bytes;
- custom resource and impact names are 1 through 64 UTF-8 bytes;
- every resource or policy fence path is at most 512 UTF-8 bytes in JSON;
- managed ancestry, including the proposed child, is at most 64 Jobs.

ResourceClaims version 2 has at most 16 custom entries, 16 impacts, and 8 shared plus exclusive
fences in total. JobSpec retains at most 32 labels. These count and length limits apply to the
ordinary child request as well as its policy, so policy evaluation and JobSummary serialization
cannot be driven by an unbounded list.

The public schema has a maximum-size fixture. Tests serialize a maximally populated JobSummary,
MAX_OBSERVATION_PAGE flat page, ManagedPolicyAdmissionSnapshot, and maximum tree page and prove
each legal wire response remains below the 16 MiB frame. Tree assembly additionally stops before
an 8 MiB encoded-response budget and exposes continuation rather than relying only on node count.

Policy fence roots are resolved during server acceptance of the Job that owns the policy, after
received and outside JobSpec::validate. Resolution uses the same no-replaceable-leaf,
stable-ancestor, canonical-component, and Windows case-folding rules as ordinary resource fences.
Roots that collide after resolution reject. The daemon persists for each root its mode, volume
identity, stable file identity of the longest existing ancestor, and canonical remaining path
components. It also retains a display-safe canonical path for public evidence.

Descendant admission never authorizes by string-prefix comparison of path keys and never opens the
stored ancestor path again. For a requested child fence, the resolver walks the requested path's
currently existing ancestors and produces candidate tuples of volume identity, stable file
identity, and remaining components from that candidate to the requested fence. A stored policy
scope contains the request only when one candidate has the same volume/file identity and the
stored remaining components are an exact component prefix of the candidate remainder. If an
accepted existing policy root was deleted and recreated, the new file identity cannot match. If a
policy root was missing at acceptance, creation beneath the same retained stable ancestor and
canonical remainder remains authorized as ordinary fence identity already intends.

The existing path key may continue to support exact ordinary fence collision detection. It is
never sufficient evidence for child-policy containment.

Each candidate for a truncated ancestor path uses the ordinary resolve_fence opening semantics:
intermediate ancestors are followed with FILE_FLAG_BACKUP_SEMANTICS and the requested leaf is
opened as the reparse object. After a candidate matches a stored policy root, that candidate does
not authorize if any component strictly between the matched ancestor and the requested leaf has
FILE_ATTRIBUTE_REPARSE_POINT. Another independently matching candidate may still authorize. The
leaf itself may be a reparse object and is fenced as that object; authority never follows it.
Junction/reparse tests prove that the candidate walk neither crosses an identity or volume
boundary silently nor grants a path-prefix escape. An I/O failure while resolving the owner policy
or a requested/delegated scope rejects acceptance exactly as an ordinary fence resolution failure
does; it never falls back to display paths.

Shared and exclusive modes are separate capabilities:

- a requested shared fence must produce a shared containment match in every policy-bearing
  ancestor;
- a requested exclusive fence must produce an exclusive containment match in every
  policy-bearing ancestor;
- permission in one mode never implies permission in the other;
- a policy may list overlapping roots across modes when it intentionally grants both.

Fence intersection is a request predicate, not a materialized set of stored tuples. Existing
nested directories have different stable file identities, so comparing their stored tuples cannot
prove containment. For each requested or delegated fence root, the daemon builds its candidate
ancestor tuples once and requires a same-mode containment match in every policy-bearing ancestor.
One ancestor denial denies the request. The immediate parent's display-safe roots are the
narrowest accepted display envelope, but they are never used alone as authorization.

## 5. Effective descendant policy and delegation

Every proposed managed Job is checked against the intersection of the immutable resolved policies
of its authenticated parent and all retained ancestors:

- scalar and custom maxima use the component-wise minimum;
- a name absent from any ancestor is forbidden;
- allowed impacts use set intersection;
- required labels use a union keyed by label key;
- each requested shared or exclusive fence must satisfy the same-mode containment predicate for
  every ancestor policy;
- allow_observed, allow_quiet, and allow_delegation use logical AND.

The ancestry walk is cycle-detecting and fails closed. A chain longer than 64 is rejected with
child_policy_depth_exceeded. Live current managed ancestors cannot be evicted under the existing
retention rules; a missing ancestor during admission is an internal fail-closed inconsistency,
not authority to use only the surviving suffix.

A child without its own child_submission_policy may run but cannot submit grandchildren. A child
may carry a policy only when the effective parent policy allows delegation. The child policy must
be no broader on every axis:

- every maximum is no greater than the inherited effective maximum;
- allowed impacts are a subset;
- each same-mode fence root satisfies every inherited ancestor policy through the request-time
  candidate predicate;
- required labels are a consistent superset;
- a boolean may remain true only when inherited true and may always be turned false;
- allow_delegation false prevents the next generation from carrying a policy.

A wider or misleading delegated policy is rejected as child_policy_escalation. It is never
accepted and silently capped.

Conflicting values for a required key in the submitted child's own delegated policy are a policy
escalation. A structurally valid child Job whose actual labels omit an effective required key is
child_required_label_missing; one whose unique value differs is child_required_label_conflict.
A conflicting stored ancestor union is an internal integrity failure and fails closed.

Limits are per descendant Job. They are not an aggregate family reservation or quota.

An explicitly present empty object, child_submission_policy: {}, is the canonical envelope for
claimless child Jobs only. It permits a child that has no scalar/custom claims, impacts, fences,
observed or quiet policy, and carries no further child policy. It is distinct from None, which
disables managed submission.

## 6. Authentication, durable admission, and recovery

OS containment continues to authenticate the immediate parent Job, Attempt, and Invocation.
Identity and authorization become separate decisions.

The containment registry and submission_context return the one current live immediate managed
parent even when its child_submission_policy is None. Resolution always selects the immediate
containing primary, never the nearest policy-bearing ancestor. A disabled or restricted managed
peer is never downgraded to unmanaged and cannot skip a policy-None leaf to inherit authority from
an outer parent. Stale, root-exited, uncertain, ambiguous, foreign, or non-current containment
continues to reject authentication exactly as before.

The current validate_current_parent responsibility is split:

- validate_current_parent_identity proves the exact live current parent and contains no policy
  decision;
- load_current_parent_policy returns the immutable resolved policy or disabled;
- received insertion and recover-no-row use identity;
- acceptance uses identity and the complete policy walk;
- managed wait uses identity and requires Some policy, preserving the existing rule that only a
  submission-enabled primary may perform a managed descendant wait.

For a managed Job or Batch:

1. authenticate the immediate parent from OS containment;
2. validate and normalize the version 2 JobSpec or BatchSpec;
3. create or recover the received Submission in the authenticated managed scope;
4. in the acceptance transaction, revalidate the live current parent;
5. read the private immutable resolved policy for every ancestor and derive the effective policy;
6. check every Batch member and every carried delegated policy;
7. retain one atomic policy rejection when any member violates the envelope;
8. only after policy success create Jobs, dependencies, resolved claims, and ordinary admission
   state.

The entire Batch is atomic. A violating member rejects all members and is named in bounded detail.
No Job, Attempt, Lease, dependency, artifact, or partial Batch row remains.

The liveness and policy checks run both on first processing and when daemon recovery resumes a
retained received Submission. A retained policy rejection is returned unchanged by same-key
replay, recover_submission, and result-file recovery.

When no matching Submission was ever received, a live authenticated parent receives NotReceived
whether its policy is Some or None, preserving R-SUB-5 and exact result-file replay. Replaying from
a policy-None parent may create only the received row and its durable child_submission_disabled
decision; it can never create a Job. Once a received row contains that rejection, recovery returns
it exactly and never reinterprets it as NotReceived.

Admission classifications remain separate:

- a detached policy-permitted Job above current or total host capacity is accepted and reports the
  ordinary resource_capacity blocker;
- a policy-permitted managed submit --wait that can never run during the live ancestor wait is
  durably rejected under the existing resource_capacity or blocked_by_ancestor contract;
- an out-of-envelope request is rejected with a child-policy code before ordinary Job admission.

## 7. Stable rejection codes and evidence

The following durable codes use the existing scheduler rejection exit code 27:

| Code | Meaning |
|---|---|
| child_submission_disabled | Immediate parent has no child policy |
| child_claim_not_permitted | Scalar/custom name or quantity is outside the effective maximum |
| child_impact_not_permitted | An impact is outside the effective allowed set |
| child_fence_not_permitted | A same-mode resolved fence fails the containment predicate for at least one policy ancestor |
| child_observed_not_permitted | observed is present but denied |
| child_quiet_not_permitted | quiet is present but denied |
| child_required_label_missing | An effective required label key is absent |
| child_required_label_conflict | The Job carries another value for a required key |
| child_policy_escalation | A carried policy widens or delegation is disabled |
| child_policy_depth_exceeded | Managed ancestry exceeds the public bound |

The stable code is automation data. Detail is bounded to 4096 UTF-8 bytes and includes the
immediate parent Job/Attempt, Batch member when applicable, requested field and value, effective
permitted value or not-permitted, and the deciding ancestor Job. No environment, secret, command
line, or unrelated ancestor data is included.

Policy evaluators return StoreError::OperationRejected { code, detail }. The implementation must
preserve that typed decision through every existing layer:

- rejection_decision writes the exact code and detail to the received Submission;
- retained_rejection reconstructs OperationRejected for every child-policy code;
- recovery resume treats OperationRejected as a completed retained rejection, never daemon-start
  failure;
- daemon RPC maps it to the exact wire code;
- client response_error maps known child-policy codes to Error::Rejected and CLI exit 27;
- persist_submit_decision writes RecoveryResult::Rejected to a result file for every policy code;
- same-key submit, recover_submission, and result-file recovery return the same pair.

tree_cursor_stale and tree_scan_limit are read-view errors, not Submission rejections. The public
client exposes ViewStale and ViewUnavailable; the first is immediately retryable from a fresh
page, while scan-limit availability may require a narrower selector or smaller queue. The CLI uses
unavailable exit 69 rather than scheduler rejection exit 27.

Accepted managed Jobs persist:

    pub struct ManagedPolicyAdmissionSnapshot {
        pub parent: ManagedParent,
        pub evaluated_unix_millis: i64,
        pub effective_policy: EffectiveChildSubmissionPolicy,
        pub policy_ancestors: Vec<JobId>,
    }

EffectiveChildSubmissionPolicy is the bounded public canonical form of the resolved maxima,
impacts, required labels, immediate parent's display-safe canonical fence scopes, and booleans.
Fence authorization still means verified against every policy_ancestors entry; the display scopes
are evidence, not a replacement for that predicate. The snapshot contains no private filesystem
IDs.

JobReceipt and JobSnapshot gain:

    pub managed_policy_admission: Option<ManagedPolicyAdmissionSnapshot>

It is Some only for an accepted managed Job and None for an unmanaged root. The snapshot is
separate from host admission evidence and never implies that a capability was reserved.
Policy ancestors are unique, ordered immediate-parent first, include every policy that participated
in admission, and are bounded by 64.

## 8. Tree identity and public read model

Managed parentage is a tree over Jobs because each accepted Job has at most one authenticated
ManagedParent. The edge retains parent Job, Attempt, and Invocation. Batch dependencies are DAG
edges and never become tree edges.

Add:

    pub enum TreeAttentionBucket {
        Running,
        Queued,
        Finished,
    }

    pub struct JobTreeNode {
        pub summary: JobSummary,
        pub depth: u32,
        pub family_attention: Option<TreeAttentionBucket>,
        pub context_only: bool,
        pub parent_retained: Option<bool>,
        pub has_children: bool,
        pub descendants_truncated: bool,
        pub next_children_cursor: Option<JobChildrenCursor>,
    }

    pub struct JobTreePage {
        pub nodes: Vec<JobTreeNode>,
        pub next_root_cursor: Option<JobTreeRootCursor>,
        pub selected_job_id: Option<JobId>,
        pub event_cursor: EventCursor,
    }

    pub struct JobChildrenPage {
        pub parent_job_id: JobId,
        pub nodes: Vec<JobTreeNode>,
        pub next_children_cursor: Option<JobChildrenCursor>,
        pub event_cursor: EventCursor,
    }

    pub struct JobTreeRootCursor {
        pub store_uuid: Uuid,
        pub order_revision: u64,
        pub selector_hash: String,
        pub bucket: TreeAttentionBucket,
        pub accepted_unix_millis: i64,
        pub root_job_id: JobId,
    }

    pub struct JobChildrenCursor {
        pub store_uuid: Uuid,
        pub selector_hash: String,
        pub parent_job_id: JobId,
        pub accepted_unix_millis: i64,
        pub child_job_id: JobId,
    }

Tree nodes are a flat depth-first pre-order. A retained parent always precedes included children.
family_attention is Some only on a family root and carries the server-classified aggregate bucket;
it is required so filtered clients can group a family whose active branch is hidden.
An unmanaged root has parent_retained None. A retained child has Some(true). A retained Job whose
immediate parent row is unavailable is emitted as an orphan root with Some(false); no synthetic
parent data is invented.

Alpha.8 has no production Job-row eviction, so alpha.9 orphan behavior is a forward-compatible
read contract exercised with a direct Store fixture that removes parent context while preserving
referentially valid test evidence. It does not falsely claim that ordinary retention currently
creates orphans.

Children are ordered by accepted time ascending and Job id ascending. Root families are ordered by
attention bucket Running, Queued, Finished and then root accepted time descending and Job id
descending.

The family bucket is computed over every retained descendant, including nodes omitted by the
current filter or node limit:

1. Running if any retained family node is active or finalizing;
2. Queued if none is running and any retained node is non-final;
3. Finished otherwise.

Every child row retains its own state, blocker, queue rank, estimate, claims, logs, Attempt, and
Containment. Family state is used only for root ordering and group placement. A filtered family
may therefore be grouped as Running while its running nonmatching branch is hidden; the root note
states activity outside filter rather than fabricating a visible running child.

Because the bucket is mutable, the schema stores a non-pruned tree_order_revision in meta,
initialized to zero. SQLite trigger jobs_tree_order_insert advances it after every Job insertion.
Trigger jobs_tree_order_update advances it after an actual change to state, outcome,
parent_job_id, parent_attempt_id, or parent_invocation_id. The triggers therefore run in the same
transaction as the ordering or membership change rather than relying on scattered Rust call
sites. Schema validation requires both exact trigger definitions. The counter is independent of
the pruned events table. LogCommitted, AttemptChanged, InvocationChanged, and ContainmentChanged
do not advance it.

The first tree page reads tree_order_revision inside the same SQLite read transaction as root
classification and stores it in JobTreeRootCursor. Continuation starts another read transaction,
compares the cursor before scanning, and retains that snapshot revision in its returned cursor.
A changed revision rejects with tree_cursor_stale and requires a fresh page. The cursor also binds
a normalized selector hash; reusing it with another selector rejects. This prefers an explicit
refresh over duplicate or skipped families without making pagination impossible while Job logs
prune ordinary event history.

Child order and labels/parentage are immutable, so JobChildrenCursor does not use an event
revision. It binds the store, normalized selector token, exact parent, and next unreturned
physical child position; continuation starts inclusively at that position. A removed cursor row
or selector mismatch rejects. Later child acceptance sorts after the cursor and may be returned
normally.

## 9. Tree selectors, bounds, and expansion

The public crate adds:

    Client::tree(
        selector: JobSelector,
        root_cursor: Option<JobTreeRootCursor>,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
        ...
    ) -> Result<JobTreePage>

    Client::tree_for_job(
        job_id: JobId,
        node_limit: u32,
        max_depth: Option<u32>,
        ...
    ) -> Result<JobTreePage>

    Client::tree_children(
        cursor: JobChildrenCursor,
        node_limit: u32,
        additional_depth: Option<u32>,
        ...
    ) -> Result<JobChildrenPage>

Bounds:

- root_limit is 1 through 256 and counts root families actually emitted;
- node_limit is 1 through MAX_TREE_PAGE_NODES, initially 256, and bounds all returned nodes;
- encoded tree response size is at most 8 MiB and may truncate before node_limit;
- max_depth is 0 through 64; omitted means 64;
- tree_children additional_depth is 0 through 63; omitted means zero, zero returns immediate
  children only, and each larger value permits one further generation;
- tree_for_job identifies the retained family root, includes the retained ancestor path, and marks
  it as selected_job_id;
- tree_for_job rejects with tree_node_limit_too_small and the required ancestor count when
  node_limit cannot contain the complete retained root-to-selection path; this is InvalidSpec and
  CLI exit 64;
- cursors returned by tree_for_job bind the normalized Jobs selector containing exactly the
  selected job_id;
- all ancestry and expansion walks detect cycles and fail closed.
- a Jobs selector used by a tree read contains at most 64 anchors, keeping its stateless opaque
  selector token and every returned child cursor within the encoded response budget.

For All, every retained root family is selected. For Labels, Jobs, or Batch, matching Jobs are
anchors. The server includes each anchor, its retained ancestor path, and retained descendants of
the anchor within the requested depth. A nonmatching ancestor included only to connect an anchor
is context_only true. A nonmatching descendant of a matching anchor is ordinary selected subtree
content and context_only false. Unrelated nonmatching siblings are omitted. All matches every
node, so its families are complete subject only to bounds.

When node_limit or max_depth cuts a branch, the included parent has descendants_truncated true and
a cursor for its next unreturned immediate child. The server stops descending that branch and may
continue with later selected roots only while node_limit remains. next_root_cursor advances after
the last root whose root node was emitted; branch completion uses tree_children and never presents
remaining children as global roots.

Every nonempty root page emits at least the first selected family's root node before truncating, so
a valid continuation always makes progress. For a filtered view, has_children means that at least
one child or descendant connector is eligible under the same selector. descendants_truncated means
eligible nodes, not unrelated filtered siblings, remain beyond a bound.

tree_children returns only eligible descendants beneath its exact parent, in depth-first order,
and reports depths relative to that parent: immediate children have depth 1 even when
additional_depth is zero. It scans physical children in immutable order, applies the selector
bound into its cursor, and advances the cursor past skipped nonmatching siblings. It never reveals
an unrelated sibling from a filtered page. Its scan of physical and ancestor rows shares the
16,384-row per-request budget below; exhaustion returns tree_scan_limit even when skipped rows
produce fewer than node_limit visible nodes.

The store adds an index equivalent to:

    CREATE INDEX jobs_parent_accepted
        ON jobs(parent_job_id, accepted_ms, id);

plus an index beginning with Job state and accepted order for attention classification, and an
accepted-order index for the bounded physical scan. Schema validation requires all three.

Tree reads open a daemon-owned read-only connection while briefly holding SharedStore, release the
scheduler mutex before traversal, and use one deferred SQLite read transaction for revision,
classification, summaries, and event-head evidence. The alpha.9 implementation performs one
accepted-order scan capped at 16,385 rows and retains at most 16,384 lightweight rows containing
identity, parent, accepted time, state, Batch, and labels. It builds no JobSnapshots or recursive
JSON. Parent and state indexes remain required for future narrower query plans and ordinary
parent/state access; the accepted-order index lets SQLite stop the model query at row 16,385.

Each request therefore has a hard 16,384-row classification, ancestry, and physical-child model
budget in addition to depth and response bounds. Seeing row 16,385 returns tree_scan_limit before
classification; it never returns a root in a false lower-attention bucket or lets filtered child
expansion walk without bound. In alpha.9 a narrower selector does not avoid this whole-model safety
bound; reducing retained history or adding a maintained aggregate in a later schema may. Immediate
retry is not promised. list --tree reports the bounded error. Because watch uses trees by default,
it falls back to the existing flat JobListPage, preserves selection where possible, and shows a
persistent TREE VIEW UNAVAILABLE banner with the scan-limit reason instead of rendering an empty
queue.

The node emitter accounts for each serialized base node plus a worst-case selector-bound child
cursor, reserves response metadata, and truncates with ordinary root/child continuation before the
8 MiB encoded budget. A final exact serialization check fails closed if that accounting invariant
is ever violated. A transactionally maintained family aggregate may replace the bounded model
later only with equivalent crash/recovery evidence and without changing the public contract.

## 10. Tree event observation

Flat observe and ObservationFrame remain unchanged for existing JobSelector modes. Tree
observation is a separate typed API so a Gap never ambiguously carries a flat snapshot:

    pub struct JobTreeSelector {
        pub root_job_ids: Vec<JobId>,
    }

    pub enum TreeObservationFrame {
        Events {
            events: Vec<SchedulerEvent>,
            cursor: EventCursor,
        },
        Gap {
            gap: EventGap,
            snapshot: JobTreePage,
            cursor: EventCursor,
        },
    }

    Client::observe_trees(
        selector: JobTreeSelector,
        cursor: Option<EventCursor>,
        event_limit: u32,
        root_limit: u32,
        node_limit: u32,
        max_depth: Option<u32>,
        max_wait_millis: u32,
        ...
    ) -> Result<TreeObservationFrame>

JobTreeSelector contains 1 through 64 retained root or anchor Job ids. Membership contains each
named Job and every authenticated descendant accepted before or after observation starts. Event
matching walks durable parentage with cycle and depth checks. A child acceptance event is visible
only after its parent edge is durable in the same transaction.

A wrong-store or expired cursor produces TreeObservationFrame::Gap with a coherent current tree
snapshot using the exact JobTreeSelector and the caller's root_limit, node_limit, and max_depth.
event_limit is 1 through MAX_OBSERVATION_PAGE; root/node/depth use the ordinary tree bounds.
max_wait_millis uses the existing bounded long-poll rule. Named anchors are returned with retained
ancestor context and their selected subtrees. A foreign selector Job id rejects; a wrong-store
event cursor still returns the current-store snapshot for valid current-store anchors. If the
bounded snapshot itself truncates, its ordinary tree cursors expose that fact. Clients complete or
refresh that snapshot before applying later events.

Label/project TUI views may continue using label-based tree refreshes. Mandatory project labels
normally make both parent and conforming descendants match independently.

## 11. CLI contract

Keep every existing flat command. Add:

    stillyard list --tree [--label KEY=VALUE] [--root-limit N]
        [--node-limit N] [--depth N] [--json] [--ascii]
    stillyard tree JOB [--node-limit N] [--depth N] [--json] [--ascii]

Human tree output is depth-first and retains ordinary STATE, RANK, CLAIMS, COMMAND, and NOTE
columns. Tree guides occupy the Job/command identity area, never the state, rank, or claim column.
ASCII uses |-- and \--. An orphan uses ?-- and names its missing immediate parent Job.

JSON returns JobTreePage or JobChildrenPage with relation metadata and no presentation glyphs.
status remains the ordinary JobSnapshot command. logs, wait, status, and cancel on a child id
continue to act on that child only.

The delivered alpha.8 cancel command remains explicit-ID only. Cascade is present in frozen
R-JOB-5/R-NEST-2 requirements but is not implemented yet, as recorded by the delivered phase-6
contract and current README. This increment neither implements cascade nor introduces another
subtree cancellation selector.

## 12. TUI contract

stillyard watch uses the tree read model by default while preserving the interaction and visual
language introduced at 37acffc:

- root families remain grouped as Running, Queued, and Finished by aggregate attention;
- each expanded root is followed immediately by its children in depth-first order;
- every represented branch starts expanded and remains visible across state transitions, so a
  child finishing cannot make rows disappear or cause an automatic layout jump;
- only an explicit user collapse hides a subtree; the collapsed row carries a bounded outcome
  summary;
- Left collapses or selects the retained parent; Right expands or selects the first child;
- selection remains keyed by JobId across refresh, reordering, collapse, and Gap recovery;
- the TUI follows root cursors and every emitted truncated-branch JobChildrenCursor until its
  existing MAX_OBSERVATION_PAGE visible-Job budget is full; it finishes each represented family
  before moving to later roots, so the per-page tree bound does not silently shorten today's
  retained history;
- selecting a child drives the existing detail and log panes for that child;
- a context-only ancestor is dim; an orphan is explicit;
- a finished parent with an active child stays in Running and is rendered dim without changing
  the child's state;
- multiple represented parent Attempts may use a synthetic dim divider, but the divider is not a
  Job and is never selectable.

The parent detail pane shows CHILD POLICY separately from CLAIMS and labels it not reserved. A
managed child detail pane shows parent Job/Attempt/Invocation and its effective policy admission
snapshot. Rejected submissions never appear as ghost Jobs.

Showing recent managed-submission rejections on the parent detail pane and emitting a dedicated
parent-targeted rejection event are deferred follow-ups. Durable recover/result-file output is the
alpha.9 rejection interface.

## 13. Non-goals and safety boundary

This increment does not:

- prevent same-owner code from invoking an undeclared executable or consuming undeclared CPU,
  RAM, disk, or GPU;
- create aggregate family quotas or reserve a child's future capacity;
- introduce priority inheritance, preemption, general wait graphs, subtree cancellation, or a
  new cascade meaning;
- turn Batch dependency edges into tree edges;
- add a store migration chain;
- implement Linux containment or remote scheduling;
- make queue positions, cursors, views, or expansions durable entities.

The public documentation and doctor continue to describe the cooperative boundary honestly.

## 14. Implementation ownership

- spec owns version 2 public policy types, bounds, uniqueness, schema generation, and
  filesystem-free shape validation;
- resources owns pure claim-limit comparison and canonical same-mode fence containment;
- store owns resolved-policy persistence, ancestor intersection, atomic rejection, snapshots,
  parent/state indexes, bounded read-connection tree queries, cursor validation, and tree event
  membership;
- daemon authenticates managed identity independently of policy and routes the new RPCs;
- api and client own bounded public tree/policy types and blocking runtime-neutral methods;
- CLI renders human/JSON tree commands through the public crate only;
- TUI consumes tree pages and tree observation without SQLite or private-file access.

When the scheduler derives a postcondition Invocation spec from its Job, it clears
child_submission_policy exactly as alpha.8 clears allow_child_submissions. Only the primary
Invocation may authenticate managed submissions.

No new source file may exceed 3000 lines. The existing large TUI module must be split by functional
ownership before or during tree work rather than growing past its current size.

## 15. Required verification and mutants

Policy and admission:

1. A parent with only account/fence claims and max cargo_slots 1 starts without consuming Cargo.
2. Its permitted cargo child obtains an ordinary Lease only when scheduled.
3. Two cargo slots reject durably with no Job or Lease row.
4. Every unlisted scalar, custom resource, impact, fence mode/scope, observed policy, and quiet
   policy has a negative cell with the exact code.
5. Missing/conflicting mandatory project labels reject; exact plus unrelated labels succeeds.
6. A mixed Batch rejects atomically and names the violating member.
7. Grandchildren use the full ancestor intersection.
8. Equal/narrow delegation succeeds; every widening axis and disabled delegation rejects.
9. Replaced policy fence paths cannot change previously accepted authority.
10. Detached capacity failure is an accepted blocker; combined managed wait retains the existing
    resource_capacity or blocked_by_ancestor decision.
11. Recovery returns the exact retained policy rejection and creates no work.
12. Disabled managed identity is never downgraded to unmanaged or replaced with an outer enabled
    ancestor; absent recovery is NotReceived and exact replay can retain only disabled rejection.
13. Policy changes alter normalized payload hashes and survive restart.
14. Duplicate version 2 label keys reject before acceptance.
15. A fake child can still invoke an undeclared absolute tool, proving the cooperative boundary.
16. Daemon restart with a retained received policy violation starts successfully, resumes the row,
    and retains the exact policy rejection.
17. Same-key submit, recover, result file, JSON, and CLI exit 27 preserve every child-policy code.
18. Existing-root replacement and missing-root later creation distinguish file identity from
    component-prefix authority.
19. An existing parent root, delegated existing subdirectory, and grandchild below it succeed only
    when the requested grandchild fence satisfies every ancestor policy.
20. A reparse leaf is fenced as that object, while a reparse component strictly between the
    matched policy ancestor and requested leaf rejects instead of crossing an inherited fence or
    volume boundary.

Tree API, CLI, and TUI:

1. Parent, child, and grandchild return in depth-first order with exact edge Attempt/Invocation.
2. A queued child remains adjacent and retains its rank and blocker.
3. A finished parent with an active child is a Running family.
4. Multiple parent Attempts remain unambiguous.
5. Root pagination never emits a normal detached child; branch truncation is explicit and
   resumable.
6. A JobChanged event between root pages makes the mutable-order cursor stale; repeated
   LogCommitted events do not prevent root or child continuation.
7. Label matches include only required ancestor context and matching anchor subtrees.
8. Parent eviction creates an explicit orphan.
9. Collapse/expand preserves JobId selection through refresh.
10. Child selection updates child detail/log panes.
11. Tree Gap recovery supplies a coherent tree snapshot before later events.
12. Future descendants of an observed root produce events.
13. Flat list/events/status command meanings remain unchanged except for the declared nested
    JobSpec version break.
14. JSON contains no box-drawing text and ASCII rendering contains no Unicode tree glyph.
15. A cycle/cursor/store/selector mismatch fails closed within public bounds.
16. Child expansion preserves its selector across skipped nonmatching siblings and future accepted
    children.
17. A minimum node limit always emits one root or returns the exact tree_for_job required-path
    error.
18. Maximum legal flat/tree pages serialize below the frame bound and the 8 MiB tree budget
    truncates with continuation.
19. Classification scan exhaustion reports bounded unavailability and never mis-buckets a family.
20. Classification scan exhaustion makes watch show the bounded flat fallback and visible banner.
21. Pruning every ordinary job_changed event with LogCommitted rows does not alter the durable
    tree_order_revision or invalidate root continuation.
22. tree_children additional_depth zero returns immediate children and always advances.
23. The schema requires both tree-order triggers, and the revision advances on every actual
    insertion/state/outcome/parent transition but not unrelated lifecycle writes.
24. Omitted additional_depth is identical to zero; filtered expansion that scans 16,385 physical
    siblings returns tree_scan_limit.
25. The TUI uses child continuations to complete a truncated represented family before consuming
    the next root cursor, up to its visible-Job budget.

Mutation controls must kill at least:

- treating policy capability as a parent Lease debit;
- authenticating a disabled managed peer as unmanaged;
- selecting the nearest enabled ancestor instead of the immediate policy-None parent;
- checking only the immediate parent;
- silently capping a wider delegated policy;
- re-resolving an ancestor fence after path replacement;
- creating one member of a policy-invalid Batch;
- mapping policy denial to resource_capacity;
- paginating mutable attention order without a JobChanged order revision;
- invalidating root or child continuation on LogCommitted alone;
- resuming a filtered child cursor without its selector hash;
- flattening a truncated child as a root;
- applying post-Gap events before tree refresh;
- replacing a child's state/rank/blocker with family state.

## 16. Delivery order and gates

1. Freeze this brief after independent Fable xhigh and Grok 4.6 high closure.
2. Add version 2 policy/public types, bounds, schema, protocol/version evidence, and payload tests.
3. Separate managed identity from authorization; implement resolved policy persistence,
   full-ancestor evaluation, durable codes, Batch atomicity, recovery, and snapshots.
4. Add tree storage index, bounded tree/children queries, cursor pinning, and tree observation.
5. Add CLI tree commands and split/extend the TUI forest.
6. Run deterministic policy, recovery, paging, observation, CLI, and TUI snapshot mutants.
7. Bootstrap the breaking daemon/spec boundary without bypassing the scheduler:
   - keep the tracked JobSpecs at version 1 while the installed alpha.8 daemon schedules the first
     fmt/check/test/clippy/build-release Jobs for the alpha.9 source;
   - verify the canonical daemon PID/image/store and wait until it has no active or queued work;
     because alpha.8 has no stop command, terminate that verified process explicitly, then install
     the scheduled alpha.9 release binary at the canonical path and acknowledge the development
     epoch reset; kill-on-close and recovery remain the safety boundary if the idle proof was
     wrong;
   - update every checked-in JobSpec to version 2, including fmt-write and schema-update;
   - rerun fmt-write, fmt, check, test, clippy, schema verification, and build-release through the
     now-installed alpha.9 system daemon.
8. Run shipped installed-image managed-child, recovery, tree paging, JSON, and TUI acceptance
   against that system daemon. Every gate names its Job ID.
9. Publish one coherent Stillyard alpha.9 commit for moot to pin; moot adoption occurs in a later
   batch with regenerated schema, fixtures, prompts, and required project labels.

Every handoff names each Stillyard Job and resulting Job ID. Direct Cargo evidence is inadmissible.
Protocol 14 is the only published alpha.9 protocol. Intermediate slice commits are not published
or installed as consumer pins; the complete protocol surface is present before shipped-path
acceptance.

## 17. Review questions

Independent reviewers must specifically challenge:

1. whether separating managed identity from policy preserves fail-closed containment while making
   child_submission_disabled durably reachable;
2. whether any replay/recovery sequence can turn a denied capability into NotReceived or new work;
3. whether resolved ancestor fence authority remains immutable across path replacement;
4. whether delegation comparison and required-label union have any widening or ambiguity axis;
5. whether policy and ordinary capacity/managed-wait codes remain observably distinct;
6. whether mutable family ordering plus durable tree-order revision cursors is complete and usable;
7. whether selector anchors, context-only ancestors, branch expansion, orphaning, and Gap recovery
   can duplicate, detach, hide, or misclassify Jobs;
8. whether the public surface is the smallest complete breaking revision and existing flat APIs
   remain honest;
9. whether the two delivery slices can be reviewed independently without a half-enforced public
   contract.
