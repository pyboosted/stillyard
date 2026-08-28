# Observed resource and quiet admission — implementation brief

Status: frozen for implementation after passing Fable and Grok closure review
Date: 2026-08-28  
Target: Windows v0.1, one host-local per-user daemon

## 1. Objective

Make Stillyard the owner of admission, serialization, and quiet for Debrix device lanes and cargo
gates. Debrix keeps its measurement protocol inside the child: clock lock, 1 Hz sampling,
Vulkan-layer filtering, process priority, sacrificial run 0, and five-run MAD.

This increment implements the already accepted R-RES-2, R-RES-3, R-RES-4, R-RES-5, R-OBS-4,
A-04 stale-memory mutant, and A-05 stale-quiet mutant. It does not reopen those requirements.

Success means Debrix can delete its custom GPU mutex, `MinFreeGb` precheck, and quiet-wait loop.
Stillyard never interpolates the child measurement specification, never waives quiet, and never
kills an external process.

## 2. Required behavior

1. RAM admission uses configured capacity, a fresh Windows observation, a configured safety
   margin, and every already granted RAM debit. The observation is the smaller of physical
   availability and commit headroom. Physical-only admission is forbidden.
2. `vram_mb:<gpu-uuid>` uses configured custom capacity, fresh NVML free memory, a configured
   safety margin, and granted debits for that UUID. Missing device data, NVML failure, or stale
   evidence blocks.
3. A sidecar-tolerant Job without `quiet` starts whenever its ordinary and observed claims fit. It
   does not enter the quiet state machine.
4. A strict Job waits for a covered stable quiet window without holding its work Lease. The work
   Lease is taken only after the stable window, and all static resource and impact constraints are
   rechecked in the reservation transaction.
5. The child is created born-contained and suspended. Root identity and executable hash are
   durably recorded while the Invocation remains never-run. A fresh synchronous quiet sample is
   taken immediately before release.
6. If the final sample is contaminated, stale, missing, or has a different host/provider
   generation, the suspended child is terminated, the Job Object is proven empty, the never-run
   Invocation resolves, the Lease releases, and the same Attempt returns to `planned` after finite
   backoff. User code is never released.
7. Quiet budget or pre-release deferral exhaustion settles that Attempt as
   `safety_failed/quiet_unattainable`. There is no waive path.
8. A strict running Job retains its complete Lease and declared impacts. Later incompatible work
   queues even if instantaneous host metrics become quiet.
9. GPU UUID and driver version are persisted and exposed whenever `gpu_slots` or a
   `vram_mb:<uuid>` claim is granted.
10. `doctor` reports coverage and freshness for every detector/provider. Unsupported or degraded
    strict policies fail closed.

## 3. Explicit non-goals

This increment does not add or change:

- `gpu_slots`, `cargo_slots`, or `impact_incompatibilities` semantics;
- runtime timeout, Windows Job Object containment, labels/wait, Conditions, secrets, artifacts,
  cascade, drain, retention, template expansion, or overlay environment;
- `submit --spec FILE` CLI shape;
- runtime RAM/VRAM caps or process termination outside Stillyard containment;
- NVIDIA clock lock, measurement CSV, Vulkan layer policy, process priority, run 0, or MAD;
- Linux, distributed placement, or multi-GPU placement.

## 4. Current implementation gaps

The current public types contain `QuietPolicy`, but `JobSpec::validate` rejects it and there is no
observed-threshold field. `HostConfig` contains only scalar capacities and impact rules.

`ResolvedClaims::blockers` performs configured-capacity and granted-debit checks only.
`Store::prepare_job_inner` creates an Attempt, prepared primary Invocation, creating Containment,
and granted Lease in one transaction. There is no durable `planned` Attempt or pre-release
deferral state.

The Windows runner creates the child suspended, then `mark_started_with_identity` changes the
Invocation to `started` and Attempt to `running` before `ResumeThread`. A never-run contaminated
Invocation therefore cannot currently return its Attempt to `planned` honestly.

There is no Windows memory provider, NVML provider, CPU/disk/process quiet provider, detector
coverage, observation decision provenance, or GPU provenance.

## 5. Public Job contract

### 5.1 Observed load thresholds

Add an optional self-contained policy to `JobSpec`:

```rust
pub observed: Option<ObservedResourcePolicy>,

pub struct ObservedResourcePolicy {
    pub max_sample_age_seconds: u64,
    pub cpu_utilization_percent_at_most: Option<u8>,
    pub gpu_utilization_percent_at_most: BTreeMap<String, u8>,
}
```

GPU map keys are full NVML UUIDs. Percentages are `0..=100`. A nonempty policy requires
`max_sample_age_seconds` in `1..=30`. This policy is an instantaneous pre-grant gate; it does not
request a stable quiet interval.

This increment supports one configured GPU placement. Every GPU UUID named by `observed`,
`quiet`, or `vram_mb:<uuid>` in a Job must canonicalize to the host's `gpu_slot_uuid`; a mixed or
different UUID rejects rather than observing one device and granting another. Custom VRAM keys
use the exact lowercase `vram_mb:` prefix and one case-folded canonical NVML UUID debit identity.
Duplicate spellings of that identity in a Job or host config reject.

A bare `gpu_slots` claim is implicitly bound to `gpu_slot_uuid`. Admission must find that exact
UUID in the current NVML topology and persist its driver; it may never satisfy provenance from
the first or any other enumerated device. If placement is absent, mismatched, or disappears after
a card/topology change, every GPU-dependent gate blocks and observation generation changes.

RAM and VRAM claims implicitly require their corresponding fresh observations and do not need
duplicate entries in `observed`.

### 5.2 Quiet policy

Replace the currently unimplemented string detector list with typed detector declarations:

```rust
pub struct QuietPolicy {
    pub stable_seconds: u64,
    pub max_sample_age_seconds: u64,
    pub wait_budget_seconds: u64,
    pub detectors: Vec<QuietDetector>,
}

#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuietDetector {
    CpuUtilization { max_percent: u8 },
    GpuUtilization { gpu_uuid: String, max_percent: u8 },
    DiskUtilization { max_percent: u8 },
    ForeignGpuCompute { gpu_uuid: String },
    BlockedProcesses,
}
```

Bounds:

- `stable_seconds`: `1..=3600`;
- `max_sample_age_seconds`: `1..=30`;
- `wait_budget_seconds`: `stable_seconds..=86400`;
- detector list: `1..=16`, no exact duplicate;
- all percentages: `0..=100`;
- all UUIDs: bounded, nonempty, canonicalized case-insensitively to the NVML value.

The Debrix strict policy is:

```json
{
  "stable_seconds": 30,
  "max_sample_age_seconds": 2,
  "wait_budget_seconds": 600,
  "detectors": [
    { "kind": "gpu_utilization", "gpu_uuid": "GPU-...", "max_percent": 0 },
    { "kind": "foreign_gpu_compute", "gpu_uuid": "GPU-..." },
    { "kind": "blocked_processes" }
  ]
}
```

Sidecar Jobs omit `quiet` entirely.

`JobSpec::validate` lifts only the rejects for `quiet` and `observed`. Conditions and artifacts
remain independently rejected until their providers exist; enabling this increment must not make
an unrelated unimplemented declaration admissible.

## 6. Host configuration

Add a host observation section:

```rust
pub struct HostObservationConfig {
    pub sample_interval_millis: u64,
    pub quiet_max_sample_gap_millis: u64,
    pub generation_max_cadence_gap_millis: u64,
    pub memory_max_sample_age_millis: u64,
    pub ram_safety_margin_mb: u64,
    pub vram_safety_margin_mb: u64,
    pub gpu_slot_uuid: Option<String>,
    pub process_rules: ProcessRules,
    pub pre_release_max_deferrals: u32,
    pub pre_release_backoff_millis: u64,
    pub admission_wall_clock_limit_seconds: u64,
    pub gpu_provider: GpuProviderConfig,
}

pub struct HostConfig {
    pub resources: ResourceCapacities,
    pub impact_incompatibilities: BTreeMap<String, Vec<String>>,
    pub observation: HostObservationConfig,
}

pub struct ProcessRules {
    pub block: Vec<String>,
    pub ignore: Vec<String>,
}

pub enum GpuProviderConfig {
    Nvml,
    Disabled,
}
```

Validation and defaults:

- sampling interval defaults to 1000 ms and is bounded to `100..=5000`;
- the quiet sample-gap threshold is at least one interval; the provider-generation cadence-gap
  threshold is explicit and no smaller than the quiet threshold, so one missed quiet sample can
  reset stability without ambiguously changing generation;
- memory max age defaults to 2500 ms and must be at least one sampling interval;
- RAM and VRAM margins are explicit positive values whenever the respective configured capacity
  is nonzero; there is no silently unsafe zero default for an enabled observed resource;
- `gpu_slot_uuid` is required when `gpu_slots > 0` or any configured `vram_mb:<uuid>` capacity is
  nonzero; at admission NVML must expose that exact canonical device and its driver, never merely
  any enumerated GPU;
- `pre_release_max_deferrals` is `1..=64`, default 16;
- pre-release backoff is `100..=60000` ms, default 1000 ms;
- admitting wall-clock limit is `1..=86400` seconds, default 3600; host-aware Job acceptance
  requires it to be at least that Job's `stable_seconds`;
- `gpu_provider = disabled` exists to make unavailable coverage explicit and testable; it never
  waives a GPU-dependent gate;
- process patterns are case-insensitive executable-basename globs with literal characters and `*`
  as the only wildcard; separators, NUL, empty patterns, `?`, character classes, and paths reject;
- default ignores are `dwm.exe` and `LogonUI.exe`;
- an exact pattern cannot appear in both lists; ignore takes precedence at evaluation;
- unmatched processes are allowed by `BlockedProcesses`;
- Debrix config supplies `cargo.exe`, `rustc.exe`, `rust-analyzer.exe`, `obs*`, `nsight*`, `ngfx*`,
  and `renderdoc*` in `block`.

`observation` is a real `HostConfig` field and appears in the committed config schema. Serde may
construct its harmless defaults only while corresponding capacities are zero. Startup/reload
validation rejects nonzero `ram_mb` with a zero RAM margin, nonzero configured VRAM with a zero
VRAM margin or mismatched/missing canonical UUID, or nonzero `gpu_slots` without `gpu_slot_uuid`;
an old config is never silently accepted with unsafe zero policy. A GPU-dependent Job on a host
without configured placement remains accepted but blocked with `gpu_placement_unconfigured`.
`gpu_provider=disabled` remains a valid explicit diagnostic configuration, but every `gpu_slots`,
VRAM, observed-GPU, or quiet-GPU admission then blocks. Restart validates retained pending Jobs
against the new host policy and refuses an incompatible config rather than resetting their bounds.

The daemon config remains host policy. No Job may weaken or replace its block/ignore rules.

## 7. Host observation ownership

Create a platform-neutral `host_observation` module with small provider boundaries and Windows
implementations. It owns provider I/O, sample cadence, freshness, and quiet-window tracking. It
does not own durable lifecycle state or launch processes.

```text
host_observation/
  mod.rs          sample/evidence types, evaluator, service, quiet trackers
  memory.rs       Windows physical + commit observations
  utilization.rs CPU and disk delta providers
  process.rs      process inventory and basename glob rules
  nvml.rs         dynamically loaded in-process NVML provider
```

The daemon reactor owns one `Arc<HostObservationService>`. Provider sampling never occurs while
holding the Store mutex. Each completed sample wakes the scheduler and observation clients.
Lock order is release barrier before Store mutex, never the reverse; ordinary status/doctor paths
must not hold Store while requesting an observation refresh.

Store methods receive an immutable bounded `AdmissionContext` plus the transaction's current
wall/monotonic time; Store never calls Windows or NVML and never owns provider handles. The
context is an operand bundle, not a precomputed Boolean. It contains:

- the observation generation and the wall plus unbiased-monotonic capture coordinates;
- independently typed component status, capture coordinates, value, and bounded diagnostic;
- canonical RAM/VRAM headroom operands and GPU UUID/driver provenance;
- evaluated observed-load and quiet-detector operands;
- the stable-window generation/token, qualifying interval, and applicable max ages.

Every reservation transaction, and the pre-release transaction for a strict Job, independently
rechecks component status, checked age, generation equality, canonical device identity,
arithmetic, and current granted debits using those operands. At pre-release, granted debits
explicitly exclude the evaluating Attempt's own already-granted Lease; every other granted or
retained Lease remains included. A context that omits an operand required by the Job is unusable.
Jobs without `quiet` have no second release gate: their observed checks happen atomically with
their one reservation/grant, so hashing a large executable cannot make them livelock on an old
reservation sample. Public
status/receipt paths are routed through the reactor so the same pure evaluator produces launch
decisions and visible blockers.

The sampler is demand-driven. It runs at the configured cadence only while a retained Job needs
observed/quiet evidence or while a bounded explicit diagnostic is collecting it; at idle it parks
on events. `doctor` may request a bounded one-shot refresh but does not leave a 1 Hz timer alive.
This preserves the existing A-19 idle wake budget.

Tests inject provider and clock traits. Production has no environment-variable or hidden test
bypass.

## 8. Evidence model and generation

One host sample contains independently timestamped results for:

- physical and commit memory;
- total CPU utilization;
- aggregate disk utilization;
- bounded process inventory;
- for every discovered GPU: UUID, driver version, memory, utilization, and compute PIDs.

A failure in one component does not fabricate or invalidate unrelated evidence. Each component is
`available`, `unavailable`, or `error`, with a bounded diagnostic and capture time.

An in-memory `observation_generation` changes and every quiet window resets when any of these
occurs:

- daemon/provider initialization or reinitialization;
- NVML device topology, canonical UUID mapping, or driver version changes;
- a suspend/resume discontinuity is detected;
- the sampler misses `generation_max_cadence_gap_millis`.

Suspend/resume detection compares wall-clock progress with an unbiased monotonic Windows clock.
A discontinuity larger than `generation_max_cadence_gap_millis` increments the generation. The final
pre-release sample must have the same `observation_generation` as the stable window and
reservation evidence. This is distinct from the durable daemon generation used for process and
containment ownership; suspend/resume need not restart the daemon and therefore cannot be guarded
by daemon generation.

No historical sample is reused after restart. Quiet stability restarts from zero, while the
durable wait budget continues.

## 9. Windows providers

### 9.1 Memory

Use in-process Windows APIs:

- `GlobalMemoryStatusEx` for `AvailablePhysical`;
- `GetPerformanceInfo` for `CommitLimit`, `CommitTotal`, and page size.

All subtraction, multiplication, and byte-to-MiB conversion is checked and rounds available
capacity down. Overflow or underflow makes the component unusable; it must never saturate into an
apparently enormous headroom. The sample is usable only when both calls succeed and
`CommitLimit >= CommitTotal`.

```text
host_headroom_mb = min(
    available_physical_bytes,
    (commit_limit_pages - commit_total_pages) * page_size
) / MiB
```

Windows event 2004 is not itself an admission provider; commit headroom prevents the known
physical-only failure mode.

### 9.2 NVML

Load `nvml.dll` in process. Use the minimum required NVML ABI for initialization, driver version,
device enumeration/UUID lookup, memory information, utilization, and compute-running-processes.
Missing DLL/symbol/device, initialization loss, size races, or API errors produce unavailable/error
evidence; no `nvidia-smi` child is launched.

Process enumeration resolves NVML PIDs to executable basenames. An inaccessible or vanished
compute PID is contamination/unknown, not proof of quiet.

### 9.3 CPU, disk, and processes

CPU uses delta-based Windows system times. Disk uses an in-process aggregate delta provider over
available physical disks; if the host denies or does not expose the required counters, disk
coverage is unavailable and a disk-dependent strict policy cannot pass.

CPU and disk delta components require a prior sample from the same observation generation. The
first sample after initialization, reinitialization, suspend/resume, or a generation cadence gap
is `unavailable/warming_up`, never fabricated as 0%. A quiet tracker cannot qualify on it.

Process inventory is obtained in process, bounded to 4096 entries, and records PID plus executable
basename only. Overflow or enumeration failure makes `BlockedProcesses` unavailable for that
sample. Pattern evaluation is deterministic and case-insensitive on Windows.

The suspended root PID undergoing the final pre-release check is excluded from the process block
detector. No other process is silently excluded. `dwm.exe` and `LogonUI.exe` are ignored for both
configured process rules and foreign-compute identity classification.

## 10. Admission arithmetic

Configured capacity and observed availability are independent gates. For a RAM request `r`,
configured capacity `C`, already granted RAM debits `g`, observed host headroom `h`, and margin `m`:

```text
r <= C - g
r <= h - m - g
```

Every subtraction and conversion is checked. If `g > C`, `m > h`, `g > h - m`, or any conversion
overflows, admission fails closed with a bounded arithmetic/provider reason; it never saturates to
a value that can pass. The same two-gate formula applies to the one configured canonical VRAM UUID
with NVML free memory and that UUID's case-folded granted debits.

Subtracting all granted debits from live headroom is deliberately conservative even when some
actual allocation is already reflected in the host observation. A Lease reserves possible future
use; Stillyard does not try to infer materialized working sets or relax a declared reservation.

The gates do not cap a running process. Runtime overuse remains observable provenance only.

Blockers distinguish:

- `resource_capacity`: request exceeds configured total;
- `resource_busy`: configured capacity is currently consumed by granted debits;
- `observation_missing`: required provider/device has no evidence;
- `observation_stale`: evidence exceeds the applicable max age;
- `observed_resource_busy`: fresh headroom/load does not satisfy request or threshold;
- `quiet_waiting`: policy is covered but has not remained satisfied for the stable interval;
- `quiet_contaminated`: a named detector currently fails;
- `detector_unavailable`: a required detector has no coverage.

Details identify the resource/detector, observed value or unavailability, threshold, margin,
granted debit, sample time/age, and observation generation without process command lines or
secrets.

## 11. Durable Attempt and release lifecycle

### 11.1 Planned and admitting Attempt

Create the next Attempt as `planned` after dependency and retry-backoff eligibility, then move it
to `admitting` when static configured capacity permits observed/quiet evaluation. The Job remains
`pending`, stores that Attempt ID, and reuses it through every pre-release deferral. This preserves
the required `planned -> admitting -> starting` domain states rather than hiding admission inside
`planned`.

The Attempt row has a durable creation time, but process `started_ms` and runtime `deadline_ms` are
nullable until release authorization. Public `AttemptSnapshot.started_unix_millis` is therefore
optional for planned/admitting/starting work. Quiet wait never consumes the process/postcondition
runtime timeout.

The Attempt persists:

- accumulated quiet-budget milliseconds and the current eligible interval start, if any;
- admitting wall-clock start and absolute host-policy deadline;
- pre-release deferral count and finite backoff;
- the current admission reason and observation generation when relevant.

Quiet budget advances only while every non-quiet gate currently passes: dependencies, actual
granted resource/impact fit, fresh RAM/VRAM, and standalone observed thresholds. Resource races or
missing/stale non-quiet resource evidence pause it. Once otherwise eligible, quiet contamination,
missing detector coverage, stale quiet evidence, and stability accumulation consume the finite
budget. A Job with only a RAM/VRAM claim and no quiet remains blocked rather than acquiring an
unrelated quiet failure.

The independent admitting wall-clock deadline never pauses. If it expires while non-quiet gates
are blocking, the Attempt settles `safety_failed/admission_starved` with the last blocker set; if
the consumed quiet budget expires, it settles `safety_failed/quiet_unattainable`. Thus resource
contention is not mislabeled as quiet failure, but an admitting Attempt still cannot wait forever.
Budget consumption is durably checkpointed at most every five seconds and on every eligibility
transition. Restart discards an open volatile eligible interval and resumes from the last durable
consumed value; downtime never counts as quiet evidence.

### 11.2 Stable wait without Lease

The reactor maintains bounded volatile quiet trackers keyed by Job/Attempt. A tracker records only
observation generation, first qualifying monotonic instant, latest sample, and current failures.
It does not retain process inventories over the full window.

Every qualifying sample must:

- cover every declared detector;
- be within the Job's max age;
- have the same observation generation;
- be separated from the prior sample by no more than `quiet_max_sample_gap_millis`.

Any failure or gap resets stability. Restart also resets stability but not the durable consumed
budget. A quiet sample gap and the separately configured provider-generation gap use their exact
thresholds from host policy. No work Lease or work Containment exists during this wait.

An admitting Attempt does not reserve configured capacity, impacts, or FIFO occupancy. Only
granted Leases count as resource use in admission and public blockers. A later sidecar may start,
causing the strict waiter to pause its quiet budget and rebuild stability after resources fit
again; this is the necessary consequence of not holding a work Lease during quiet.

### 11.3 Atomic reservation

After a stable window, one Store transaction rechecks Job/Attempt state, cancel request,
dependencies, configured claims, granted debits, every typed `AdmissionContext` operand and age,
quiet observation generation, budget, and deferral count using transaction-current time.
It then atomically:

- changes Attempt `admitting -> starting`;
- grants the complete Lease;
- creates a new prepared primary Invocation and creating Containment;
- records the immutable admission evidence and GPU provenance for this reservation;
- changes Job `pending -> active` and points it at the current Invocation/Containment.

One Attempt may own multiple primary-role Invocation records only when earlier ones provably never
ran because of pre-release deferral. Every Invocation receives
`MAX(role_index)+1`, so `role_index` remains unique and monotonically increases across primaries
and postconditions. A separate nullable `postcondition_index` binds a postcondition Invocation to
its JobSpec entry; postcondition selection never infers identity from global `role_index`.

For Jobs with quiet, the acceptance-time bound includes possible deferrals:
`max_attempts * (1 + postconditions + pre_release_max_deferrals) <= 256`. Jobs without quiet keep
the ordinary `max_attempts * (1 + postconditions) <= 256` bound because they cannot create a
pre-release replacement primary. A host policy/Job combination that cannot satisfy its applicable
bound rejects instead of overflowing the durable Invocation limit.

### 11.4 Suspended root and final recheck

Split the current `mark_started_with_identity` operation while preserving its safety ordering:

1. `record_suspended_root` persists PID, exact creation identity, executable hash, daemon
   generation, reserved observation generation, and live Containment while Invocation stays
   `prepared` and Attempt stays `starting`.
2. The runner enters an observation-service release barrier. While holding it, the service takes a
   synchronous fresh sample excluding only that suspended PID, updates any provider generation,
   and evaluates the complete quiet policy plus every strict RAM/VRAM/load operand. Provider I/O
   occurs before the Store mutex is acquired.
3. Still under the barrier, `authorize_release` revalidates all component ages, arithmetic,
   canonical GPU identity, and observation generation inside the Store transaction. Release-time
   debit sums exclude this Attempt's own Lease. It persists the final evidence and atomically
   changes Invocation to `started`, Attempt to `running`, records first Job/process `started_ms`,
   and creates the runtime deadline before user code can run, preserving R-RUN-5 and managed-child
   authentication.
4. Immediately before `ResumeThread`, the barrier compares its generation with the service's live
   generation and checks wall-vs-unbiased-clock discontinuity against
   `generation_max_cadence_gap_millis`. Its expiry is the earliest capture time plus the minimum
   applicable component/quiet max age; no arbitrary longer TTL is permitted. Provider
   reinitialization/generation change takes the same barrier exclusively, so it cannot race this
   compare-and-resume. The runner calls `ResumeThread` while the barrier is still held, then
   releases it.

Any final sample/barrier failure before step 3 uses the never-run cleanup/replan path and leaves
process timestamps null. A failure after the durable step 3, including `ResumeThread` failure,
cleans the never-run boundary but settles this Attempt `start_failed` with a bounded release reason;
it never returns `running -> planned` and ordinary retry policy decides whether a new Attempt is
allowed. Every failure releases the observation barrier before the bounded process/Job Object
cleanup wait, so a provider-generation change is not stalled behind cleanup.

### 11.5 Contaminated final sample

Before any durable replan:

- terminate the suspended root through its Stillyard Job Object;
- wait boundedly for both root exit and empty Job Object;
- retain the registered Job Object authority until emptiness is proven.

Then one transaction:

- resolves the never-started Invocation with no root exit classification;
- marks its Containment empty;
- verifies attempt-wide that no other open Containment exists before releasing the Lease;
- gives cancel precedence; runtime timeout is not applicable because this replan path is reachable
  only before release authorization and its process deadline is still null;
- otherwise releases the Lease, changes Attempt `starting -> planned`, changes Job
  `active -> pending`, clears current Invocation/Containment pointers, and sets finite backoff;
- increments the deferral count and records the exact contamination/stale/generation reason.

Pre-authorization cancel and admission-budget exhaustion use this never-run cleanup path and never
call the ordinary root-exit settlement path, because a suspended root has no truthful user-code
exit classification.

If cleanup cannot be proven, the Containment becomes uncertain, retains the Lease, and the Attempt
settles `safety_failed/pre_release_cleanup_uncertain`; no replacement Invocation is launched. The
uncertain-settlement API accepts the explicit safety verdict/outcome rather than hard-coding
`interrupted`. This settlement is unconditionally final and suppresses retry regardless of whether
`safety_failed` appears in the Job retry list, because its retained Lease belongs to an unproven
Containment.

If budget or deferral count is exhausted, the clean never-run path settles the Attempt as
`safety_failed/quiet_unattainable` and the Job follows its ordinary retry policy. A fresh Job retry
creates a new Attempt and a new quiet budget.

## 12. Durable evidence and public observation

Bump the greenfield SQLite schema epoch. Add bounded durable admission-decision records keyed by
Attempt and reservation index. Each record contains:

- decision state (`waiting`, `reserved`, `replanned`, `released`, `failed`);
- observation time, age, and observation generation;
- configured/observed/granted/margin operands used for RAM and VRAM;
- load and quiet detector outcomes;
- GPU UUID and driver version;
- bounded reason code/detail and final-sample marker.

Add explicit public `AdmissionDecisionSnapshot`, `ObservedOperandSnapshot`,
`DetectorEvidenceSnapshot`, and `GpuProvenance` types. `JobReceipt`, `JobSnapshot`, and
`AttemptSnapshot` carry bounded optional admission data; `GpuProvenance { uuid, driver_version }`
is mandatory in the decision whenever `gpu_slots` or VRAM was granted. A waiting receipt can
therefore explain stale evidence or the detector preventing quiet; a final snapshot preserves the
grant and release provenance. If NVML cannot provide both canonical UUID and driver, even a
sidecar `gpu_slots` grant blocks: R-OBS-4 is not waived merely because the Job omitted quiet. The
persisted UUID is always equal to host `gpu_slot_uuid` as revalidated against live NVML evidence.

`AttemptSnapshot` gains a bounded `reason_code` so `safety_failed/quiet_unattainable` is not reduced
to an unqualified verdict, and its process start/deadline timestamps are optional before release.
`DoctorCoverage` entries explicitly carry provider/detector identity, coverage status, last
observation time, observation generation, and bounded remediation. TUI/detail views omit absent
optional sections rather than printing `None`, `null`, or `?` placeholders as product data.

No process command line, environment value, or unbounded process list is persisted or exposed.

## 13. Doctor coverage

Extend `DoctorSnapshot` with bounded detector/provider coverage entries. At minimum report:

- physical-memory observation;
- commit-headroom observation;
- CPU utilization;
- disk utilization;
- process inventory and block/ignore rule validity;
- NVML initialization and driver version;
- per-GPU UUID memory, utilization, and compute-process coverage;
- sampler freshness and observation generation.

Each entry is `pass`, `warning`, or `fail`, has a stable code, last observation time when available,
and bounded remediation. Optional unavailable hardware is warning. It becomes fail when configured
capacity or any retained Job requires the missing coverage. This affects admission only; doctor
never starts a helper process or reads a consumer's files.

## 14. Recovery and cancellation

- A daemon restart resets volatile quiet stability and observation generation, then resumes
  planned/admitting Attempts with their original durable consumed budgets. Recovery excludes
  those states from the blanket interruption settlement used for started work.
- Recovery of `starting` before release follows existing Containment proof. It never assumes the
  child ran and never creates another Invocation until the old boundary is proven empty. A durable
  release authorization is treated as possibly run after a crash and is never silently replanned.
- A cancel during quiet wait settles the planned/admitting Attempt canceled without a Lease or
  Invocation; pending-Job cancellation explicitly settles its attached Attempt.
- A cancel after reservation but before authorization wins through the never-run cleanup path; a
  cancel after durable authorization uses the ordinary started-root stop path. Both transactions
  check `cancel_requested` and cannot lose it.
- An Attempt timeout continues to cover process and postconditions after release. Quiet wait budget
  is independent and does not consume runtime timeout.
- A retained uncertain Containment continues to retain every granted debit used by admission.

## 15. Implementation boundaries

- `spec`: public Job/config policy types and shape validation only.
- `host_observation`: provider I/O, samples, freshness, process rules, quiet trackers, pure policy
  evaluation.
- `resources`: configured scalar/fence/impact arithmetic plus pure observed-resource arithmetic;
  no Windows/NVML calls.
- `store`: durable planned/reservation/replan/release transactions and persisted decisions; it
  consumes immutable evidence.
- `daemon`: owns providers, routes current evidence into Store reads/decisions, wakes on samples.
- `runner/windows`: born-contained suspended creation, root recording, final guard invocation,
  cleanup proof, and release.
- `api`/CLI/TUI: bounded evidence display only through the public crate.

No file may exceed 3000 lines. New tests and symbols use functional names, never planning-stage
names.

## 16. Verification and mutants

Deterministic tests use injected clocks/providers and assert durable state, not only return values.

Required unit and integration cases:

1. High physical availability plus low commit headroom blocks RAM.
2. A stale or missing physical/commit component blocks before Lease/Invocation creation.
3. Configured capacity and observed headroom each independently block; granted debits participate
   in both gates; no partial Lease row appears.
4. NVML missing, stale, unknown UUID, and memory API failure block VRAM.
5. Standalone observed CPU/GPU upper bounds use fresh evidence and do not require stable quiet.
6. A sidecar Job with no quiet creates no quiet tracker and can overlap compatible cargo work.
7. Quiet stability resets on detector failure, sample gap, provider generation change, and daemon
   restart.
8. No work Lease exists during the long quiet window.
9. A final contaminated sample never resumes the child, proves empty, releases the Lease, preserves
   the Attempt ID, and creates a later Invocation only after backoff/restability.
10. Final stale/missing evidence behaves exactly like contamination.
11. Budget/deferral exhaustion produces `safety_failed/quiet_unattainable`.
12. Cleanup uncertainty retains the Lease and forbids replacement.
13. `dwm.exe` and `LogonUI.exe` never fail foreign-compute/process quiet.
14. Configured cargo/rustc/rust-analyzer/OBS/Nsight/NGFX/RenderDoc patterns reset quiet.
15. An inaccessible NVML compute PID fails closed.
16. GPU UUID/driver in receipt/snapshot comes from the evidence used for grant/release.
17. Doctor coverage changes honestly with provider availability and configured requirements.
18. Mutating the stale-memory check to accept old evidence fails A-04.
19. Mutating the final quiet recheck to accept cached evidence fails A-05.
20. Mutating the implementation to hold the work Lease during stable wait fails.
21. Mutating commit headroom out of the RAM minimum fails.
22. Checked RAM/VRAM overflow and underflow are unusable evidence, never large synthetic capacity.
23. The first CPU/disk delta sample in a generation is warming up and cannot qualify quiet.
24. A Job cannot mix GPU placement, observed, quiet, or case-variant VRAM debit identities.
25. Delaying the reservation/release transaction past sample age blocks; delaying after release
    authorization past token age never resumes the child.
26. Restart preserves planned/admitting Attempt and consumed budget; pending cancel settles it.
27. Cancel racing contaminated cleanup cannot replan a canceled Job or later release user code.
28. Runtime deadline begins at release authorization, not Attempt creation or quiet eligibility.
29. Deferral primaries and postconditions have unique ordering and correct explicit spec mapping;
    the combined Invocation bound rejects before overflow.
30. `gpu_provider=disabled` blocks a provenance-required sidecar `gpu_slots` grant as well as strict
    quiet/VRAM work, and public doctor/receipt fields identify why.
31. A strict RAM/VRAM request greater than half of fresh headroom releases on its first clean final
    sample because release arithmetic excludes only its own Lease debit.
32. Changing live observation generation after authorization cannot pass the held release barrier
    or call `ResumeThread`; no NVML reinit/topology race exists between compare and release.
33. Cleanup uncertainty with retryable `safety_failed` in Job policy still finalizes once, retains
    its Lease, and never creates a replacement Attempt.
34. Exact `gpu_slot_uuid` disappearance/card swap blocks bare `gpu_slots`; another enumerated GPU
    cannot supply provenance.
35. A no-quiet Job's Invocation bound is independent of host pre-release deferrals, while quiet
    Jobs include them.
36. Mutating the admitting wall-clock deadline to pause with quiet budget fails: continuous
    incompatible work must settle `safety_failed/admission_starved` within the host limit.

## 17. Shipped daemon and CLI dogfood

Run against one release daemon and its public CLI/API, with child marker files proving whether user
code was released:

- **A:** cargo test Job (`cpu_heavy`, `cargo_slots`, `ram_mb`) overlaps a sidecar GPU correctness
  Job (`gpu_slots`, no quiet) and serializes only on declared claims/impacts.
- **B:** strict measurement (`gpu_slots`, `measurement`, quiet 30 s) queues while a configured
  incompatible `cpu_heavy` cargo Job runs.
- **C:** external cargo/rustc resets quiet and the child marker remains absent until a full clean
  window completes.
- **D:** two `ram_mb=24000` Jobs with capacity 32768 serialize on the shipped daemon. The separate
  deterministic A-04 provider mutant proves that stale RAM evidence cannot produce a Lease or
  child marker.
- **E:** an isolated release daemon with `gpu_provider=disabled` shows failed doctor coverage and
  never releases strict GPU quiet or VRAM work. Strict quiet reaches its finite safety failure;
  VRAM remains blocked.
- **F:** `dwm.exe`/`LogonUI.exe` do not contaminate foreign-compute quiet.

A-05 also has a shipped-path negative control, not only an in-process assertion. An isolated copy
of the release daemon loads a minimal external NVML ABI fixture placed beside that copy through
the normal Windows DLL search contract. The fixture supplies a stable quiet window and then
changes generation/contaminates the synchronous final sample. The unmodified daemon must leave the
child marker absent. In a detached mutant worktree, removing the final freshness/generation check
must make that same public CLI harness observe the forbidden marker, proving that the gate detects
the named stale-quiet launch failure. The fixture is external provider behavior, not a production
environment-variable bypass. A-04 keeps its deterministic injected-provider mutant plus the
shipped physical/commit dogfood because Windows memory APIs do not have an equivalent safe DLL
fixture.

Debrix then reruns A–F with its real cargo and GPU children on the same per-user daemon as moot.
Stillyard's internal substitute commands do not claim Debrix measurement correctness.

## 18. Delivery order and gates

1. Public/config types, schemas, blocker/reason codes, pure arithmetic/evaluator tests.
2. Injectable Windows memory/process/utilization/NVML providers and doctor coverage.
3. Durable planned Attempt, admission decisions, reservation, replan, and schema epoch.
4. Runner suspended-root split and final pre-release guard.
5. Public receipt/snapshot/CLI/TUI evidence and GPU provenance.
6. Deterministic mutants, full regression suite, release dogfood A–F, then Debrix handoff.

Every coherent delivery runs the checked-in `fmt`, `test`, and `clippy` JobSpecs through the
system default Stillyard daemon as required by `AGENTS.md`; direct local Cargo is inadmissible.
Release output goes to `target/scheduled`, while the running canonical daemon/CLI lives under the
per-user installed `bin` directory. That installed daemon is promoted only after the scheduled
release build and isolated acceptance pass, so it never locks or overwrites its own build output.

## 19. Review questions

Independent reviewers must specifically challenge:

1. whether the conservative RAM/VRAM formula is safe and internally consistent rather than
   accidentally subtracting the wrong debit set;
2. whether any sequence can release user code from stale quiet evidence;
3. whether multiple never-run primary-role Invocations in one Attempt violate the domain model or
   snapshot ordering;
4. whether restart/cancel/cleanup uncertainty can release a Lease or launch a replacement too soon;
5. whether provider ownership avoids Windows/NVML calls under the Store mutex;
6. whether the public/config additions are the smallest complete contract for R-RES-2..5;
7. whether dogfood rows distinguish a passing scheduler from the named stale-memory/stale-quiet
   failure modes.
