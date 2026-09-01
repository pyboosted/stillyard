# Stillyard Product Requirements

Status: Managed-execution amendment v0.13 (2026-08-30)

Product name: Stillyard

Tagline: *A quiet, local-first scheduler for real processes.*

## 1. Scope

Stillyard is a lightweight per-user process scheduler for developer workstations and execution hosts. It runs ordinary non-interactive programs, admits them from declared CPU, memory, GPU, and compatibility needs, persists their state and output, and exposes one public Rust client crate, one CLI, and one terminal queue viewer.

Product v0.1 targets Windows 10 version 1809 or newer and Windows Server 2019 or newer. Product v0.2 adds Linux x86_64. Requirements marked Windows or Linux are platform-specific; the remaining requirements apply to both releases.

Stillyard is host-local. Every host owns an independent queue and store. A caller or owning agent chooses a host and may invoke the same CLI through SSH. Stillyard opens no network listener and performs no remote placement, SSH lifecycle management, or cloud control.

The words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are normative.

## 2. Product shape and public API

R-PKG-1 One Cargo workspace MUST produce one installable binary, `stillyard.exe` on Windows and `stillyard` on Linux, plus one public library crate named `stillyard`. No external database server, language runtime, container runtime, privileged helper, or installed system service is required.

R-PKG-2 The public crate is a runtime-neutral client facade. It MUST expose owned types and blocking operations for connect/start, submit, atomic ensure-or-recover for one Job or Batch, submission recovery, typed wait, inspect/list, cancel, events, logs, daemon status, and administration. Every blocking operation accepts a deadline and optional cancellation token. Dropping a client operation never cancels a Job.

R-PKG-3 CLI and TUI MUST use only the public crate and local protocol. They MUST NOT read SQLite or private files directly. The daemon is the store's only writer and the only process that launches or contains user work.

R-PKG-4 The binary auto-starts one per-user daemon only when an unmanaged local client selects the complete default store/endpoint and no daemon exists. The daemon is detached from the invoking terminal/SSH channel and owns all child pipes; closing that client does not stop accepted work. A managed child and every explicit or environment-selected endpoint are connect-only and never auto-start a daemon. `daemon --store <absolute-dir> --endpoint <local-endpoint>` may run a foreground isolated instance; one lifetime lock protects its canonical store and a second owner-scoped lifetime lease protects its endpoint. Local IPC is an owner-only named pipe on Windows and an owner-only Unix-domain socket on Linux v0.2.

R-PKG-5 JobSpec and BatchSpec use one versioned JSON Schema generated from the same public Rust types used by client and daemon. Unknown fields reject. The crate ships the schema and `stillyard schema spec` prints it byte-for-byte.

R-PKG-6 `Client::submission_context` is a supported public query returning the selected store UUID and the server-authenticated immediate managed parent, if one exists. `stillyard [--endpoint PIPE] context --json` MUST return the same public `SubmissionContext { store_uuid, parent }` document. Both surfaces use the same OS peer/Containment authentication as submission. `parent: null` means the daemon established that the caller is unmanaged; peer-observation failure, missing containment evidence, ambiguity, and claimed-coordinate mismatch MUST return an error and MUST NOT be converted to `parent: null`.

## 3. Explicit guarantees and non-goals

R-SCOPE-1 Stillyard guarantees atomic submission decisions, crash-consistent lifecycle state under the platform filesystem contract, idempotent submission within retained history, complete-tree termination inside its cooperative containment boundary, and honest reporting when cleanup or history is unknown.

R-SCOPE-2 Product v0.1 does not claim physical-power-loss survival when hardware, firmware, RAID, a filter, or a volatile cache acknowledges a flush without stable persistence. It does not defend against deliberately hostile code using the same owner's independent authority to duplicate daemon handles, create work through WMI or Task Scheduler, or otherwise bypass containment. These boundaries MUST be visible in `doctor`.

R-SCOPE-3 Neither release provides distributed scheduling, preemption of healthy work, interactive console jobs, privilege elevation, containers, CPU or GPU hard partitioning, multi-host transactions, arbitrary process policing, or exactly-once delivery to an external stdout/stderr handle.

R-SCOPE-4 Labels and process-name rules are scheduling hints and policy inputs, not security identities. Executable identity, owner identity, process start identity, and containment are obtained from the operating system.

## 4. Domain model

The implementation is organized around nine durable entities. Each has one owner and one explicit lifecycle.

| Entity | Purpose | Lifecycle |
|---|---|---|
| Submission | Idempotent request to create one Job or atomic Batch | received -> accepted or rejected |
| Batch | Immutable atomic group and local-name mapping | accepted -> retained -> reaped |
| Job | User intent, dependencies, Conditions, retry policy, and final outcome | pending -> active -> finalizing -> final |
| Attempt | One retry quantum and its admission/execution state | planned -> admitting -> starting -> running -> checking -> settled; planned/admitting/starting may settle; admitting/starting may return to planned |
| Invocation | One primary, probe, or postcondition process run | prepared -> started -> exited -> resolved; prepared may resolve without starting |
| Containment | OS-owned process-tree boundary and cleanup evidence for one Invocation | creating -> live -> empty; creating may become empty after proof that no process was created or remains; creating/live -> uncertain -> cleared |
| Condition | External prerequisite for an Attempt | waiting <-> satisfied; waiting/satisfied -> failed; re-evaluated for each Attempt |
| Observation | Immutable timestamped Condition or resource evidence | immutable value with freshness and provenance |
| Lease | Atomic ownership of all resources and exclusions for an Attempt or probe | requested -> granted -> released; retained while Containment is uncertain |

R-DOM-1 Every lifecycle transition MUST be a guarded SQLite transaction. Terminal Submission decisions, Job outcomes, Attempt verdicts, and Invocation results are immutable. First durable terminal decision wins.

R-DOM-2 Job owns Attempts and Conditions. Attempt owns its primary and postcondition Invocations and work Lease. Condition owns probe Invocations and their temporary Leases. Invocation owns exactly one Containment. An uncertain Containment owns the affected Lease until proof or explicit operator clearance.

R-DOM-3 Submission is the durable idempotency boundary. Containment is the durable process-safety boundary. Neither client sessions, queue positions, watches, waits, nor TUI views are durable entities.

R-DOM-4 Durable Job dependencies and authenticated parent-child relationships form acyclic graphs. A Job's final outcome is published only after every Invocation owned by its Conditions and current Attempt is resolved and every Containment is empty or uncertain with the affected Lease transferred.

## 5. SQLite store and byte storage

R-STORE-1 One fixed per-user directory contains one SQLite database and versioned subdirectories for staged inputs and canonical logs. The database holds every entity, normalized specification, dependency, idempotency record, resource decision, committed log offset, event, and retention decision. Clients never open it.

R-STORE-2 The daemon MUST enable SQLite foreign-key checks, use a documented crash-safe journal/synchronous configuration, and express each cross-entity decision in one transaction. Before the first stable release, the SQLite store has exactly one current schema epoch and no migration chain. While holding the daemon singleton lock, startup MUST silently delete only the SQLite database and its SQLite sidecars and create a fresh store when the schema epoch is absent or different, required schema validation fails, or the database is corrupt. Configuration and canonical log files are not part of that reset. Changing the current schema epoch is therefore an explicitly destructive development operation.

R-STORE-3 Large stdin snapshots, staged files, stdout, and stderr live in ordinary files rather than SQLite blobs. The daemon writes and flushes a staged blob or log range before the SQLite transaction that references its hash/length/offset; it never acknowledges the reference in the opposite order. Recovery collects unreferenced staged files, ignores or truncates an uncommitted log tail, and reports a committed missing or corrupt range as an explicit Gap.

R-STORE-4 Product v0.1 supports only the fixed store on local fixed NTFS. Result files may use local fixed NTFS. ReFS, removable, redirected, remote, and unknown filesystems reject. Linux v0.2 names its supported local filesystems separately. The daemon uses documented file flush/rename primitives but performs no privileged device-cache inspection.

R-STORE-5 Retention has finite configurable limits for Jobs, rejected Submissions, events, logs, staged inputs, and cleared Containments. Live Jobs, active Submissions, current Attempts, creating/live/uncertain Containments, membership of a retained Batch, and idempotency records for retained Jobs cannot be evicted. Every Submission decision and idempotency record scoped to a current managed parent Attempt is retained until that Attempt settles. When history may have expired, recovery returns `unknown`; it never automatically resubmits.

R-STORE-6 The store UUID appears in every durable public ID and cursor. A foreign ID/cursor rejects. Every R-STORE-2 reset creates a new store UUID, so IDs, cursors, result files, and idempotency history from the discarded database become foreign or unknown and MUST NOT cause automatic resubmission. Stillyard never tries to preserve or reconstruct selected rows from an incompatible store.

## 6. Submission, batches, and idempotency

R-SUB-1 A submit request contains a caller-selected or CLI-generated cryptographically random idempotency key, a normalized payload hash, and optional staged input hashes. Scope is `(store, owner, authenticated parent Job and Attempt or unmanaged, key)`. Same scope and hash returns the same Submission decision; another hash returns typed `idempotency_conflict` with both existing and requested payload hashes.

R-SUB-2 File inputs are completely streamed, hashed, and staged before the `received` Submission transaction. Disconnect before that point leaves only bounded collectable temporary data and no Submission. After `received`, the daemon owns progress independently of the client and eventually commits `accepted` or `rejected`. Startup resumes every `received` Submission.

R-SUB-3 Acceptance of one Job or an entire Batch is one SQLite transaction. A Batch may refer to members by local name and contain an acyclic dependency DAG. It commits every Job, immutable label, dependency, parent relationship, staged-input reference, Batch mapping, and receipt, or none. For a managed child, that transaction revalidates that the authenticating parent Invocation is still started with a live root in the current daemon generation and that its parent Job and Attempt have no terminal selection; otherwise the Submission rejects.

R-SUB-4 Acceptance means durable ownership, not immediate process start. The receipt MUST immediately report Submission/Job/Batch IDs, current lifecycle, blockers, queue position or rank, and a start estimate with confidence and the assumptions used.

R-SUB-5 `recover_submission` reads a retained Submission by exact scope, key, and hash. It returns received, accepted, rejected, conflict, `not_received`, or unknown and never creates work. `not_received` is returned only for an authenticated current managed parent Attempt protected by R-STORE-5 when no matching Submission or idempotency record exists; it proves that this logical operation had not reached `received` at the recovery transaction. `unknown` means absence is not proof because history may have expired. A conscious unrelated rerun normally uses a new key.

R-SUB-6 `--result-file` is an optional small atomic JSON receipt, not a second transaction log. Before send it records key, payload hash, selected host/daemon identity, and authenticated parent Job and Attempt when managed; after response it is atomically replaced with the latest Submission/Job identity and result. Reopen uses it first to call `recover_submission`. A missing, corrupt, or `unknown` result never authorizes resubmission; authenticated `not_received` authorizes only the exact managed replay in R-SUB-7.

R-SUB-7 Fresh `submit --result-file FILE` requires an absent file; `recover --result-file FILE` never itself creates work. A managed wrapper derives its key/path from parent Job, current Attempt, and operation identity; including Invocation is recommended provenance. A later parent Attempt therefore creates fresh child work. Within the same current Attempt, `received` or a later decision reattaches to the same Submission, while `not_received` permits the wrapper to restage and resubmit the exact same normalized payload and input hashes with the same key and result file. Concurrent completion of the original upload is harmless under R-SUB-1. `unknown` fails the current operation closed; it neither deletes the receipt nor retries, and a later parent Attempt uses its new key/path.

R-SUB-8 A set target for wait, status, or cancel is a bounded immutable snapshot selected by explicit Job IDs, retained Batch ID, or an AND-set of exact labels over currently retained Jobs. JSON set-wait emits one settlement per member as it becomes final and then a worst-outcome aggregate ordered `succeeded < skipped < canceled < interrupted < timed_out < failed`; `--any` returns after the first final member without changing the rest. Passthrough is valid only for one Job. Missing explicit IDs/Batch reject; label discovery deliberately makes no completeness promise about already-evicted history.

R-SUB-9 `Client::ensure_job`, `Client::ensure_batch`, and `stillyard ensure` own the complete submit/recover choice. Their caller supplies the complete immutable JobSpec/BatchSpec, stable idempotency key, explicit selected instance, client-only waiting deadline, optional result file, and the daemon-authenticated managed-parent context when present. Rust ensure rejects a Client whose endpoint was not explicitly selected, and CLI ensure requires `--endpoint`; ambient `STILLYARD_ENDPOINT` authenticates matching parent coordinates but never selects the ensure instance. They return `accepted`, `pending`, `final`, typed `rejected`, daemon-backed typed `conflict`, or `unknown`. A retained result file is recovered before any replay; a local payload mismatch queries the daemon and cannot manufacture or reverse a conflict. Only authenticated `not_received` permits exact replay, while `unknown`, missing/corrupt identity, a foreign store, or a changed managed parent fail closed. Both result-file locks obey the client deadline/cancellation token. Concurrent exact callers converge through R-SUB-1. A client deadline or disconnect never cancels accepted work. The result file remains only the atomic R-SUB-6 receipt projection; consumers do not publish a separate immutable spec file or implement a `Test-Path -> submit|recover` transaction.

## 7. Job specification and dependencies

R-JOB-1 JobSpec contains an executable, exact argument vector, working directory, stdin policy, explicit environment changes, resource claims, impact tags, optional observed-resource thresholds, optional quiet policy, expected duration, Conditions, timeout, retry policy, postconditions, labels, and declared artifact paths. It may contain at most 32 bounded nonempty `key=value` labels without NUL; labels are immutable and included in the normalized payload hash. Shell parsing occurs only in an explicit shell mode.

R-JOB-2 Stdin is immediate EOF or a staged immutable file snapshot. The daemon never inherits client stdin. Working directory and executable are checked again immediately before launch; unsafe replacement or disappearance yields `start_failed` without running user code. A successful predecessor may replace directory contents under the same exclusive path fence without triggering this failure, provided the fenced working-directory object's stable identity is preserved; deleting and recreating that root is still unsafe replacement. On Windows v0.1 an executable is unsafe when it is missing or the prelaunch/created-image check resolves it as a directory, reparse point, or unsupported file type. Replacing an ordinary file at the same canonical path is allowed; R-RUN-2 records the stable identity and hash of the image actually created before release.

R-JOB-3 Dependency edges are `success`, `failure`, or `terminal`. All incoming edges must be satisfied. An impossible edge finalizes the Job as skipped. Retries are owned by the predecessor Job and remain invisible to dependency satisfaction until its final outcome.

R-JOB-4 Job outcomes are succeeded, failed, timed_out, interrupted, canceled, and skipped. Attempt verdicts are succeeded, process_failed, start_failed, timed_out, interrupted, safety_failed, postcondition_retryable, postcondition_failed, and canceled. Retry policy names retryable verdicts, maximum Attempts, and finite backoff. When no retry remains, the deciding Attempt maps `succeeded` to succeeded; `process_failed`, `start_failed`, `safety_failed`, `postcondition_retryable`, and `postcondition_failed` to failed; and `timed_out`, `interrupted`, or `canceled` to the same-named Job outcome. Dependency-impossible Jobs become skipped without creating an Attempt.

R-JOB-5 Cancel selects the requested Jobs canceled once, terminates their live Containments, and suppresses retry. Plain cancel does not select independently scheduled children. `cancel --cascade` additionally covers the bounded authenticated-child closure and dependency successors selected by the request. Its report returns the complete selected/traversed closure so a consumer can build a proof-sensitive reset DAG. Repeating cancel is idempotent and reports already-final members.

R-JOB-6 The daemon snapshots each declared artifact after an Attempt settles: path, presence, size, modification time, and bounded SHA-256 or an inspection error. Artifact observation does not itself change success; postconditions may enforce it.

R-JOB-7 Probe and postcondition diagnostic tails, exit classification, and every Invocation root exit are visible in the public snapshot and JSON CLI, not only in the TUI. A same-path executable identity/hash change while queued or between Attempts is recorded per Invocation as provenance, not automatic tampering.

## 8. Conditions and external readiness

R-COND-1 v0.1 supports path exists, path absent, transition-after-submission, not-before time, and executable probes. Conditions are an AND-set and declare a finite deadline or explicit none. A relative deadline is resolved to a durable absolute deadline at Job acceptance and is not reset by a later Attempt.

R-COND-2 An Observation records value, monotonic and wall time, boot/daemon generation, freshness, and source. Filesystem notifications are hints; the daemon always rescans. Restart, detected resume, watcher loss, overflow, and freshness expiry invalidate affected evidence and trigger re-evaluation.

R-COND-3 Conditions are evaluated before each Attempt obtains its work Lease and rechecked immediately before launch. If evidence changed, the Lease releases and the same Attempt returns to planned with finite backoff. A store-configured finite deferral count/deadline prevents livelock; exhaustion yields `safety_failed/readiness_unstable`.

R-COND-4 A probe is an Invocation with its own finite timeout, claims, accepted exit codes, and bounded diagnostic output. It acquires only its own Lease and releases it before the Job requests its work Lease. At most one probe per Condition is unresolved. Probe cleanup uncertainty makes that Condition blocked until its Containment clears.

R-COND-5 A Condition deadline continues across daemon downtime. Expiry before a primary launch selects the configured failed or canceled Job outcome with reason `condition_deadline_expired`, regardless of remaining retry budget. Once the primary starts, that Attempt no longer depends on its pre-start Conditions.

## 9. Resource and quiet scheduling

R-RES-1 Host configuration defines integer capacities for `cpu_units`, `ram_mb`, `cargo_slots`, `gpu_slots`, and `vram_mb:<gpu-uuid>`, plus custom integer resources, named shared/exclusive fences, and incompatibility rules between impact tags such as `cpu_heavy`, `gpu_heavy`, and `measurement`. A Job may claim any bounded combination of built-ins, multiple custom scalars, fences, and impacts. One Lease grants the complete requested set atomically; partial acquisition is forbidden.

R-RES-2 RAM and VRAM admission combines configured capacity, fresh observed availability, safety margin, and already granted debits. A Job may also request a fresh upper bound on external CPU/GPU load without requesting a full quiet window. Missing or stale required evidence blocks with an explanation. Stillyard does not claim hard memory or load enforcement; runtime overuse is observed and reported.

R-RES-3 CPU-intensive builds and GPU work use the same scheduler. A sidecar-tolerant Job needs no quiet policy and may start whenever its claims fit. A strict measurement declares quiet detectors, a stable interval, maximum sample age, and finite wait budget.

R-RES-4 Quiet detectors are in-process providers for CPU, GPU, disk, and configured process rules. Host policy may explicitly block or ignore executable identities/patterns; unmatched processes follow one documented default. Rules affect admission only and never kill an external process. `doctor` reports detector coverage and unsupported strict policies fail closed.

R-RES-5 Quiet admission samples immediately before process release. If quiet evidence or resume/provider generation changed after reservation, the never-run Invocation is cleaned, the Lease releases, and the same Attempt returns to planned with finite backoff. Exhaustion yields `safety_failed/quiet_unattainable`. While a strict measurement runs, its Lease retains the configured impact exclusions, so a later incompatible cargo/GPU Job queues even if instantaneous metrics look idle.

R-RES-6 Scheduling is non-preemptive. Effective rank combines explicit bounded priority, original acceptance order, and aging. Reservations prevent starvation but have finite hold time. A blocked high-ranked Job cannot indefinitely prevent compatible lower-ranked work.

R-RES-7 Every Job may declare expected duration. The estimate uses running remaining estimates, queued claims, reservations, Conditions, quiet stability, and recent calibration. It MUST distinguish estimated, lower-bound-only, and unknown; it never invents a precise time from an external predicate.

R-RES-8 A path fence resolves without following a replaceable leaf. Existing paths use stable filesystem identity. A missing leaf uses the stable identity of the longest existing ancestor plus its platform-canonical remaining components (case-folded on Windows), so two different missing children do not alias and later creation cannot evade the fence.

R-RES-9 `Client::daemon_status` and `stillyard daemon-status` MUST expose an authoritative scalar-resource snapshot for every built-in and configured custom resource as `{ capacity, granted, reserved }`. `granted` is the checked sum of every currently granted Lease, including Leases retained by uncertain Containments after their Jobs become final. `reserved` is the checked sum of the same complete, non-partial FIFO queue reservations used for blocker reporting, excluding granted Leases; a Job whose complete Lease does not fit reserves none of its scalar claims. An omitted snapshot means a protocol-compatible older daemon, never an all-zero current observation.

## 10. Invocation and Containment

R-RUN-1 Attempt admission atomically grants its Lease and creates a prepared primary Invocation plus creating Containment whose durable record names the platform boundary and boot generation. Process creation begins only after that transaction commits. User code begins at most once per Invocation. A never-started Invocation may resolve and its Containment may become empty only after the recorded boundary proves that no process was created or remains.

R-RUN-2 Windows v0.1 uses one fresh unnamed Job Object per Invocation, kill-on-close, no breakaway, and born-contained suspended process creation. The daemon records root PID/creation identity and executable hash before resume. Ordinary descendants inherit containment. Deliberate same-owner handle duplication is outside R-SCOPE-2.

R-RUN-3 Root exit is committed before outcome interpretation. Root exit, timeout, cancel, force stop, or safety failure causes complete-tree termination. The daemon drains stdout/stderr through EOF and marks Containment empty only after the live Job Object reports zero processes and every recorded root identity no longer matches.

R-RUN-4 If a live daemon cannot prove emptiness within the cleanup bound, Containment becomes uncertain and retains its Lease and incident evidence. Periodic reconciliation retries proof. `doctor clear-containment ID --force` may release the Lease only after explicit risk confirmation and preserves an audit record. Reboot proves prior-boot processes gone.

R-RUN-5 If the daemon crashes, Windows closure of the daemon-owned recorded Job handle with kill-on-close is proof of emptiness within the cooperative threat model; Linux uses the recorded cgroup proof in R-LINUX-2. A recovered creating Containment with no recorded root could not have released user code, but it settles `start_failed` only after the applicable platform proof; an absent or uninspectable boundary without such proof becomes uncertain and retains its Lease. Every started Invocation without a durable root exit becomes interrupted; the daemon waits boundedly for each recorded root identity to disappear. A still-matching root becomes uncertain. Stillyard does not invent success or launch a replacement primary during recovery.

R-RUN-6 Postconditions run after primary cleanup, in specification order, using the still-held work Lease. Before any postcondition release, the daemon durably records one immutable `PrimaryInvocationResult` containing schema version, matching Job/Attempt/Invocation IDs, primary verdict, optional root exit, termination reason, empty Containment proof, mandatory resolution time, and process start/exit times when those events exist. The same typed value appears in the public Attempt snapshot and in postcondition-only `STILLYARD_PRIMARY_RESULT` JSON. It contains no secret or private daemon path. Preparing a postcondition rechecks the document schema and identities, the primary's current empty Containment, and the same granted Attempt Lease; a persisted document alone cannot authorize release. Verdict/termination/root-exit combinations are validated before persistence. Executable postcondition codes classify accepted, retryable, or failed. Cleanup uncertainty takes precedence over normal exit interpretation, records no false empty result, starts no postcondition, and retains the Lease.

R-RUN-7 `stop --drain` commits a cutoff that rejects later managed-child acceptance and stops new independent Attempt/probe starts. It may still start already-accepted Attempts, probes, and postconditions in the immutable target or dependency closure of an active managed wait owned by a live Attempt, so that Attempt can finish; all other queued work remains durable for the next daemon. `stop --force` additionally interrupts live Attempts but leaves independent queued Jobs intact. The daemon exits only after every live Containment is empty or uncertain and every transition is committed.

R-RUN-8 The postcondition launch matrix is normative: primary exit 0 and primary nonzero exit run configured postconditions after R-RUN-6; start failure, timeout, interrupt, cancel, safety failure, and cleanup uncertainty run none. Cancel or timeout observed before the next postcondition release stops the sequence. Daemon recovery never launches a new postcondition for the interrupted Attempt: a postcondition already prepared/started is resolved under R-RUN-5, the Attempt settles interrupted, and any pre-crash `PrimaryInvocationResult` remains byte-equivalent in the snapshot. A crash after empty proof but before result persistence settles interrupted without fabricating a result or launching a postcondition. Retry, when configured for that final Attempt verdict, creates a new Attempt with a new primary result.

## 11. Managed children without a general wait graph

R-NEST-1 A process proved inside one running submission-enabled primary Invocation may submit child Jobs. The daemon derives immutable parent Job and Invocation IDs from OS containment; JobSpec cannot assert them. Probes, postconditions, uncertain containments, ambiguous peers, and disabled primaries cannot submit.

R-NEST-2 Children are ordinary independently admitted Jobs. They inherit no Lease, priority, timeout, environment, or cancellation policy. Ordinary parent success/failure and plain parent cancel do not cancel them. Explicit cascade, parent timeout, force interruption, or uncertain parent cleanup MUST select every still-unselected authenticated descendant canceled with an ancestor reason under R-JOB-5.

R-NEST-3 v0.1 deliberately has no durable WaitEdge or general wait-for hypergraph. An unmanaged client may wait on any visible Job/Batch. A managed primary may synchronously wait only on a bounded immutable snapshot of its already-accepted authenticated descendant closure. Descendants accepted later are not added to that wait.

R-NEST-4 Before a managed wait begins, the daemon conservatively rejects it if the target or any unfinished Job in its durable dependency closure depends on the waiter Job's completion, or if any such Job's next primary, probe, or postcondition may require a scalar, fence, or impact exclusion incompatible with a Lease held or retained by the waiter or its authenticated ancestors. Otherwise the wait is a disposable client operation. Disconnect releases the wait but never cancels the child. This restriction may reject a theoretically runnable graph; it MUST never silently accept an ancestor-resource deadlock.

R-NEST-5 Managed `submit --wait` performs the R-NEST-4 check against the proposed child before the Submission acceptance transaction. Unsafe combined submission rejects without creating work. Plain `submit` remains available when the parent intentionally wants detached work.

## 12. Logs, events, and observation

R-OBS-1 The daemon is the only reader of child stdout/stderr. It drains both concurrently into canonical binary files regardless of clients. Public log chunks carry stream, byte offset, checksum, and commit order. Arbitrary bytes are never assumed to be UTF-8.

R-OBS-2 `logs --follow` and passthrough read canonical committed ranges. Reconnection resumes from explicit offsets or replays from zero. Stillyard never claims exactly-once output to a terminal or redirected external handle. A scheduler result is emitted separately from child bytes.

R-OBS-3 A bounded SQLite event table drives watch/subscription. Cursors contain store UUID and sequence. Expired history produces an explicit Gap followed by a current snapshot; clients never infer missing transitions.

R-OBS-4 Receipt, snapshot, and final provenance include host, boot, Stillyard version, spec/input hashes, Attempt and Invocation IDs, executable hash, root exit, timestamps, effective non-secret environment, resource decisions, parent/Batch IDs, Conditions/Observations, containment strength, GPU UUID/driver when used, and artifact observations. Secret values are always redacted.

R-OBS-5 List and label watch are dynamic views over retained Jobs. Explicit Job IDs and retained Batch IDs provide exact membership; labels are convenient discovery and deliberately make no historical-completeness promise after retention.

R-OBS-6 Every `InvocationChanged` event MUST identify its owning Attempt and Invocation and MUST name the provider-reported `started` or `exited` transition committed by that same lifecycle transaction. Preparing or resolving an Invocation without provider-reported start/exit does not fabricate one of these transitions. Other event kinds carry no Invocation-transition identity. A wire-shape change to this strict nested response requires a local protocol increment rather than exposing old clients to an event they cannot deserialize.

## 13. Environment, identity, and secrets

R-ENV-1 Every Invocation starts from a small documented clean environment plus explicit per-Job set/unset changes persisted in the accepted Job. A Job may set PATH; the daemon's ambient PATH, SSH variables, billing credentials, display variables, and unrelated environment are never inherited implicitly. Account and toolchain selection belong to the submitting consumer rather than host-local daemon presets.

R-ENV-2 The daemon injects non-secret `STILLYARD_JOB_ID`, `STILLYARD_ATTEMPT`, `STILLYARD_INVOCATION_ID`, `STILLYARD_ROLE`, its effective `STILLYARD_ENDPOINT`, and daemon identity. A postcondition additionally receives the immutable `STILLYARD_PRIMARY_RESULT` JSON from R-RUN-6; primaries and probes do not. These coordinates are server-reauthenticated and are not bearer authority. Managed coordinates are scoped to the endpoint that injected them: an explicit different endpoint does not present foreign-instance coordinates, while same-endpoint membership is always derived again by the selected daemon from the pipe peer's OS containment.

R-ENV-3 Secrets are referenced by name, stored with the platform owner-protection facility, materialized only for permitted primary/postcondition launch, and never returned in scheduler logs, snapshots, events, or diagnostics. User program output remains an explicit confidentiality boundary.

R-ENV-4 Local clients authenticate as the daemon owner and bind both peer and daemon PID to process-start identity and the client-selected expected executable identity. The expected daemon may be a pinned `stillyard` binary at an arbitrary canonical path; this rule does not require the default installation directory. Lower-integrity, other-user, remote-pipe/socket, stale-PID, and wrong-binary peers reject.

R-ENV-5 The Linux v0.2 clean base is exactly account-derived HOME, USER, LOGNAME, SHELL, TMPDIR, and LANG plus explicit Job changes; runtime socket coordinates are injected separately. SSH, display, Wayland, WSL, and daemon ambient variables are absent unless explicit. Linux accepts anything `execve` accepts, including a valid shebang script, without implicit shell parsing.

## 14. CLI and TUI

The minimum CLI is:

```text
stillyard submit (--spec FILE | --batch FILE) [--idempotency-key KEY] [--wait] [--silent] [--passthrough] [--result-file FILE] [--json]
stillyard ensure (--spec FILE | --batch FILE) --idempotency-key KEY [--wait] [--silent] [--passthrough] [--result-file FILE]
stillyard recover --result-file FILE [--wait]
stillyard wait (JOB... | --batch ID | --label KEY=VALUE...) [--any] [--passthrough] [--json]
stillyard status [JOB | --batch ID | --label KEY=VALUE...] [--json]
stillyard list [--label KEY=VALUE...] [--limit N] [--cursor CURSOR] [--json]
stillyard logs JOB [--attempt N] [--invocation ID] [--stdout | --stderr] [--follow] [--since OFFSET]
stillyard watch [--job JOB | --batch ID | --label KEY=VALUE...]
stillyard events [--since CURSOR] [--label KEY=VALUE...] --json
stillyard [--endpoint PIPE] context --json
stillyard cancel (JOB... | --batch ID | --label KEY=VALUE...) [--cascade]
stillyard secret (set NAME | remove NAME | list) [--json]
stillyard stop (--drain | --force | --local)
stillyard doctor [clear-containment ID --force]
stillyard schema spec
stillyard schema managed-execution
```

R-CLI-1 Human output is stable for operators; `--json` uses public crate types. Acceptance/receipt flushes before waiting. `--silent` emits no scheduler bytes and requires `--result-file`; passthrough child bytes are still child output.

R-CLI-2 Exit codes are stable: 0 success, 20 failed, 21 timed out, 22 canceled, 23 interrupted, 24 skipped, 25 still pending/client deadline, 26 empty, 27 rejected/conflict/unsafe managed wait/`not_received`, 64 usage/spec, 69 daemon/store unavailable, and 70 Gap/unknown/internal. `wait` and `ensure --wait` emit public `WaitReport`/`EnsureReport` JSON with `exit_source`, scheduler exit, and the final snapshot/root exit. A primary root exit is returned directly only for a final `succeeded`/`process_failed` Attempt and only when it does not collide with the scheduler namespace; root exit 25 therefore emits `root_exit_code: 25` but returns scheduler-owned 20, never pending. JSON distinguishes every exit-27 reason. Client wait deadline, cancellation, or disconnect is disposable and never cancels the Job.

R-TUI-1 `watch` shows queue rank, estimate, lifecycle, blockers, claims, Conditions, parent/children, Attempts, running time, outcome, and live stdout/stderr tails. Detail view shows the complete public snapshot, Submission, Containment incidents, artifacts, and decisions.

R-TUI-2 TUI is disposable and bounded-memory. Detach never cancels work. Reconnect uses snapshot plus event cursor or visibly reports Gap.

## 15. Linux v0.2

R-LINUX-1 Linux v0.2 preserves the same public entities and SQLite schema semantics. It uses an owner-only Unix-domain socket, peer credentials plus PID identity, a fixed per-UID local store, and no IP socket.

R-LINUX-2 Each Invocation uses a dedicated delegated cgroup v2 leaf, recorded before process creation, and a born-contained process path. `cgroup.kill`, `cgroup.events populated=0`, pidfd/root identity, and boot ID implement Containment. Recovery proves even a creating/no-root Containment empty from the recorded leaf; an absent or uninspectable leaf becomes uncertain rather than implying emptiness. A systemd user manager with linger may keep the host-local daemon alive across SSH logout; absence is a doctor prerequisite failure rather than a weaker silent fallback.

R-LINUX-3 Linux path, procfs, cgroup, CPU/RAM, and NVML providers are in-process. Native Linux and WSL2 publish separate capabilities; WSL2 strict quiet/GPU policies reject unless host-wide evidence is available. Linux applies the R-JOB-2 provenance rule using `execve` semantics: same-requested-path replacement is allowed when the resolved target remains an executable regular image or valid shebang script, and the launched target's actual identity/hash is recorded; disappearance or a non-regular/non-executable target yields `start_failed` before release.

R-LINUX-4 Linux release runs every platform-neutral acceptance scenario plus native socket, peer, cgroup, pidfd, environment, detach, cleanup, and performance variants. Windows evidence never substitutes for Linux evidence.

R-LINUX-5 Stillyard does not keep a WSL2 virtual machine alive. `doctor` MUST report the observed host keepalive/`vmIdleTimeout` capability or unknown; session-survival acceptance on WSL2 is inadmissible without an asserted external keepalive.

## 16. Acceptance contract

Acceptance uses the shipped public crate and CLI path. Each row includes a negative control that must make the test fail.

| ID | Required scenario and negative control |
|---|---|
| A-01 | Concurrent default clients produce one daemon and one SQLite writer. Distinct explicit store/endpoint pairs coexist without touching the default instance; same-store/different-endpoint and same-endpoint/different-store contenders fail promptly. A split-store-lock, store-lock-only, or foreign-pipe-family mutant fails. |
| A-02 | Kill client/daemon at every staging and Submission boundary: no Submission or one resumable received/terminal decision exists, and the same key never creates two Jobs. A check-then-insert idempotency mutant fails. |
| A-03 | Atomic Batch fan-out/fan-in is wholly visible or absent, dependencies fire only from the stable R-JOB-4 final-outcome mapping, and impossible edges skip. Partial-batch and retry-exhausted-outcome mutants fail. |
| A-04 | CPU build, sidecar GPU Job, VRAM-limited Job, fences, and custom scalars overlap only when the complete Lease fits. A partial-grant and stale-memory mutant fail. |
| A-05 | Strict quiet waits for covered stable evidence and rechecks before release; sidecar-tolerant work skips quiet. Ignore/block process rules are visible. A stale-quiet launch mutant fails. |
| A-06 | Every receipt immediately returns blockers, rank, and honest estimated/lower-bound/unknown ETA. A fabricated external-condition ETA mutant fails. |
| A-07 | Path transition, restart/overflow/resume rescan, not-before, probe, acceptance-anchored finite deadline, and unstable prelaunch deferral behave without stale launch or livelock. A notification-is-truth and per-Attempt-deadline-reset mutant fail. |
| A-08 | Primary descendants are born in one Windows Job Object, timeout/cancel kills the tree, output drains, and Lease releases only after empty. A post-create assignment or root-only kill mutant fails. |
| A-09 | Daemon crash interrupts an unrecorded-exit Invocation, trusts Job kill-on-close only inside the declared boundary, proves a creating/no-root boundary empty on each platform, and never guesses success or relaunches it. A crash-means-success and no-root-means-empty mutant fail. |
| A-10 | Cleanup timeout creates uncertain Containment retaining its Lease; later proof or explicit audited clearance releases it. A timeout-means-empty mutant fails. |
| A-11 | Managed child submission derives parentage and rechecks a live unselected parent at acceptance. Safe descendant wait snapshots its targets; conflicts through their dependency closure or any Lease component reject before combined acceptance; disconnect leaves accepted child alive. Hidden ancestor-impact, predecessor-claim, and late-child-after-parent-terminal mutants fail. |
| A-12 | Canonical stdout/stderr survive client loss, resume by offset, report corrupt/missing Gap, and never claim terminal exactly-once. An external-handle-exactly-once claim fails. |
| A-13 | Closing a local terminal or SSH session leaves accepted/running work and daemon containment intact; a later client inspects by Job/Batch/key. A client-owned-process mutant fails. |
| A-14 | Drain starts no independent work after its cutoff, permits already-accepted work needed by an active managed wait to finish, rejects later managed children, and preserves the remaining queue; force interrupts only live work; plain cancel leaves independent children while cascade reaches selected children/successors; restart resumes queue. Drain-blocks-required-child and force-cancels-independent-queue mutants fail. |
| A-15 | Clean explicit environments, staged stdin, named secrets, injected IDs, executable identity, same-path ordinary-file self-update, artifacts, and redaction work through public APIs. Ambient-environment-inheritance, same-path-update-rejected, and reparse-image-accepted mutants fail. |
| A-16 | CLI/TUI import only the public crate, detach safely, recover from event Gap, show logs and Containment, and use bounded memory. A private-store-read mutant fails. |
| A-17 | SQLite/file fault injection at every commit/log publication boundary produces a valid prior/new state or explicit diagnosis/Gap. The report states the physical-power-loss boundary. An epoch mismatch, missing required schema, invalid store identity, or corrupt database atomically selects whole-database reset on the next singleton-daemon start, creates a new store UUID, preserves config/logs, and never imports old rows. Partial-preservation, migration, stale-ID-reuse, and reset-without-singleton-lock mutants fail. |
| A-18 | Consumer fleet submits parallel reviewers as a Batch, gates later rounds on results/reset Jobs, runs nested cargo/GPU spikes without ancestor conflicts, replays the same managed operation after authenticated `not_received` but never after `unknown`, and collects logs/artifacts. Reset replaces slot contents while preserving the fenced root identity; the later reviewer depends on reset success and takes the same fence. Duplicate-spike, resubmit-after-evicted-history, recreated-reset-root, and reset-before-containment-clear mutants fail. |
| A-19 | Five-minute idle measurement uses event waits, no helper process, below 0.5% of one logical CPU, at most two wakes/minute, and below 40 MiB private working set excluding mapped SQLite pages. A polling mutant fails. |
| A-20 | Linux v0.2 independently passes platform-neutral and native containment/identity/detach scenarios, including same-path executable replacement with actual-target provenance and rejection of a non-`execve`-able target. Process-group-only-containment and stale-executable-provenance mutants fail. |
| A-21 | Two concurrent `ensure` callers with one key and exact Job/Batch payload converge on one durable decision; a different payload returns both conflict hashes; received recovery reattaches; managed `not_received` permits only exact replay; `unknown` permits none; Batch remains all-or-nothing; managed scope changes with the parent Attempt; and result-file replacement/TOCTOU cannot alter identity. Check-then-submit, replay-after-unknown, partial-Batch, cross-Attempt-key, and result-file-overwrite mutants fail. |
| A-22 | Typed wait returns pending at a client deadline without cancellation, later returns final, and terminal primary exit 25 is final with `root_exit_code=25` while CLI returns scheduler-owned failed rather than pending. Exit-code-only classification and disconnect-cancels-Job mutants fail. |
| A-23 | A primary grandchild is absent before postcondition release; the same Attempt Lease remains granted; postcondition receives immutable typed root exit/verdict through `STILLYARD_PRIMARY_RESULT`; snapshot exposes the same value; uncertain cleanup records no false empty; and crash between primary exit/proof/postcondition selects only R-RUN-8 recovery. Postcondition-before-empty, released-Lease, private-store-read, mutable-result, and cleanup-timeout-means-empty mutants fail. |

## 17. Delivery order

Product v0.1 — Windows:

1. Public crate types, schema, local protocol, singleton daemon, current SQLite schema epoch/reset, Submission idempotency, and lifecycle tests.
2. Job/Batch DAG, Conditions, Lease scheduler, estimates, explicit clean environments, staged input, logs, and events.
3. Windows Containment, timeout/cancel/drain/force, recovery, uncertain cleanup, nested children, and quiet admission.
4. CLI/TUI parity, consumer-fleet acceptance, security review, fault injection, performance evidence, and packaging.

Product v0.2 — Linux:

5. Unix socket identity, fixed per-UID paths, cgroup/pidfd Containment, systemd-user detachment, and Linux providers.
6. Native Linux/WSL2 acceptance, performance evidence, compatibility tests, and packaging of the same crate and one Linux binary.

## 18. Deferred extensions

The following require a later requirements revision rather than hidden v0.1 complexity: general managed wait graphs, power-loss-qualified storage classes, embedded in-process engine hosting, service installation, multi-GPU Jobs, distributed placement, remote APIs, preemption, hostile same-owner isolation, and exactly-once delivery to arbitrary external streams.
