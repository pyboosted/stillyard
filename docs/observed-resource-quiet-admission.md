# Observed resource and quiet admission — implementation brief

Status: draft for independent Fable and Grok review  
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

## 6. Host configuration

Add a host observation section:

```rust
pub struct HostObservationConfig {
    pub sample_interval_millis: u64,
    pub memory_max_sample_age_millis: u64,
    pub ram_safety_margin_mb: u64,
    pub vram_safety_margin_mb: u64,
    pub gpu_slot_uuid: Option<String>,
    pub process_rules: ProcessRules,
    pub pre_release_max_deferrals: u32,
    pub pre_release_backoff_millis: u64,
    pub gpu_provider: GpuProviderConfig,
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
- memory max age defaults to 2500 ms and must be at least one sampling interval;
- RAM and VRAM margins are explicit positive values whenever the respective configured capacity
  is nonzero; there is no silently unsafe zero default for an enabled observed resource;
- `gpu_slot_uuid` is required when `gpu_slots > 0`, because a granted GPU claim requires exact
  provenance;
- `pre_release_max_deferrals` is `1..=1024`, default 32;
- pre-release backoff is `100..=60000` ms, default 1000 ms;
- `gpu_provider = disabled` exists to make unavailable coverage explicit and testable; it never
  waives a GPU-dependent gate;
- process patterns are case-insensitive executable-basename globs with literal characters and `*`
  as the only wildcard; separators, NUL, empty patterns, `?`, character classes, and paths reject;
- default ignores are `dwm.exe` and `LogonUI.exe`;
- an exact pattern cannot appear in both lists; ignore takes precedence at evaluation;
- unmatched processes are allowed by `BlockedProcesses`;
- Debrix config supplies `cargo.exe`, `rustc.exe`, `rust-analyzer.exe`, `obs*`, `nsight*`, `ngfx*`,
  and `renderdoc*` in `block`.

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

Store methods receive an immutable bounded `AdmissionContext`; Store never calls Windows or NVML
and never owns provider handles. Public status/receipt paths are routed through the reactor so the
same pure evaluator produces launch decisions and visible blockers.

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

An in-memory provider generation changes and every quiet window resets when any of these occurs:

- daemon/provider initialization or reinitialization;
- NVML device topology, canonical UUID mapping, or driver version changes;
- a suspend/resume discontinuity is detected;
- the sampler misses the maximum permitted cadence gap.

Suspend/resume detection compares wall-clock progress with an unbiased monotonic Windows clock.
A discontinuity larger than two configured sample intervals increments the generation. The final
pre-release sample must have the same generation as the stable window and reservation evidence.

No historical sample is reused after restart. Quiet stability restarts from zero, while the
durable wait budget continues.

## 9. Windows providers

### 9.1 Memory

Use in-process Windows APIs:

- `GlobalMemoryStatusEx` for `AvailablePhysical`;
- `GetPerformanceInfo` for `CommitLimit`, `CommitTotal`, and page size.

All multiplication and byte-to-MiB conversion is checked/saturating and rounds available capacity
down. The sample is usable only when both calls succeed and `CommitLimit >= CommitTotal`.

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

Every operation is saturating. The same two-gate formula applies to one VRAM UUID with NVML free
memory and that UUID's granted debits.

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
granted debit, sample time/age, and provider generation without process command lines or secrets.

## 11. Durable Attempt and release lifecycle

### 11.1 Planned Attempt

Create the next Attempt as `planned` when dependencies, configured capacity, current granted
debits, and retry backoff permit it to enter observed/quiet admission. The Job remains `pending`,
stores that Attempt ID, and
persists:

- quiet-wait start time when a quiet policy first becomes eligible;
- pre-release deferral count;
- current finite wait deadline derived once from `wait_budget_seconds`.

Resource occupancy before initial quiet eligibility does not burn quiet budget. Once quiet waiting
starts, its wall-clock budget remains finite even if the Job later loses a reservation race.
Missing/stale quiet provider evidence after initial static fit burns that budget. Missing/stale RAM
or VRAM evidence without quiet blocks indefinitely.

### 11.2 Stable wait without Lease

The reactor maintains bounded volatile quiet trackers keyed by Job/Attempt. A tracker records only
generation, first qualifying monotonic instant, latest sample, and current failures. It does not
retain process inventories over the full window.

Every qualifying sample must:

- cover every declared detector;
- be within the Job's max age;
- have the same provider generation;
- be separated from the prior sample by no more than two configured intervals.

Any failure or gap resets stability. Restart also resets stability but not the durable wait budget.
No work Lease or work Containment exists during this wait.

### 11.3 Atomic reservation

After a stable window, one Store transaction rechecks Job/Attempt state, dependencies, configured
claims, granted debits, fresh RAM/VRAM/load evidence, quiet generation, budget, and deferral count.
It then atomically:

- changes Attempt `planned -> starting`;
- grants the complete Lease;
- creates a new prepared primary Invocation and creating Containment;
- records the immutable admission evidence and GPU provenance for this reservation;
- changes Job `pending -> active` and points it at the current Invocation/Containment.

One Attempt may own multiple primary-role Invocation records only when earlier ones provably never
ran because of pre-release deferral. `role_index` remains unique and monotonically increases.

### 11.4 Suspended root and final recheck

Split the current `mark_started_with_identity` operation:

1. `record_suspended_root` persists PID, exact creation identity, executable hash, daemon
   generation, and live Containment while Invocation stays `prepared` and Attempt stays `starting`.
2. The runner asks the observation service for a synchronous fresh sample, excluding only that
   suspended PID, and evaluates the same quiet policy and generation.
3. On pass, `authorize_release` atomically changes Invocation to `started`, Attempt to `running`,
   persists the final evidence, and records the first Job start time. Only then may `ResumeThread`
   be called. A failure after authorization remains a conservative `start_failed`; recovery never
   launches a replacement for an authorized Invocation.

### 11.5 Contaminated final sample

Before any durable replan:

- terminate the suspended root through its Stillyard Job Object;
- wait boundedly for both root exit and empty Job Object;
- retain the registered Job Object authority until emptiness is proven.

Then one transaction:

- resolves the never-started Invocation with no root exit classification;
- marks its Containment empty;
- releases the Lease;
- changes Attempt `starting -> planned`;
- changes Job `active -> pending`, clears current Invocation/Containment pointers, and sets finite
  backoff;
- increments the deferral count and records `quiet_contaminated` evidence.

If cleanup cannot be proven, the Containment becomes uncertain, retains the Lease, and the Attempt
settles `safety_failed/pre_release_cleanup_uncertain`; no replacement Invocation is launched.

If budget or deferral count is exhausted, the clean never-run path settles the Attempt as
`safety_failed/quiet_unattainable` and the Job follows its ordinary retry policy. A fresh Job retry
creates a new Attempt and a new quiet budget.

## 12. Durable evidence and public observation

Bump the greenfield SQLite schema epoch. Add bounded durable admission-decision records keyed by
Attempt and reservation index. Each record contains:

- decision state (`waiting`, `reserved`, `replanned`, `released`, `failed`);
- observation time, age, and provider generation;
- configured/observed/granted/margin operands used for RAM and VRAM;
- load and quiet detector outcomes;
- GPU UUID and driver version;
- bounded reason code/detail and final-sample marker.

Add public snapshots for admission decisions, detector evidence, and GPU provenance. Receipt,
JobSnapshot, and AttemptSnapshot expose the relevant bounded decision data. A waiting receipt can
therefore explain stale evidence or the detector preventing quiet; a final snapshot preserves the
grant and release provenance.

`AttemptSnapshot` gains a bounded `reason_code` so `safety_failed/quiet_unattainable` is not reduced
to an unqualified verdict.

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
- sampler freshness and provider generation.

Each entry is `pass`, `warning`, or `fail`, has a stable code, last observation time when available,
and bounded remediation. Optional unavailable hardware is warning. It becomes fail when configured
capacity or any retained Job requires the missing coverage. This affects admission only; doctor
never starts a helper process or reads a consumer's files.

## 14. Recovery and cancellation

- A daemon restart resets volatile quiet stability and provider generation, then resumes planned
  Attempts with their original durable budgets.
- Recovery of `starting` before release follows existing Containment proof. It never assumes the
  child ran and never creates another Invocation until the old boundary is proven empty.
- A cancel during quiet wait settles the planned Attempt canceled without a Lease or Invocation.
- A cancel after reservation wins before release through the existing suspended-root stop path;
  it does not become a quiet replan.
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

Debrix then reruns A–F with its real cargo and GPU children on the same per-user daemon as moot.
Stillyard's internal substitute commands do not claim Debrix measurement correctness.

## 18. Delivery order and gates

1. Public/config types, schemas, blocker/reason codes, pure arithmetic/evaluator tests.
2. Injectable Windows memory/process/utilization/NVML providers and doctor coverage.
3. Durable planned Attempt, admission decisions, reservation, replan, and schema epoch.
4. Runner suspended-root split and final pre-release guard.
5. Public receipt/snapshot/CLI/TUI evidence and GPU provenance.
6. Deterministic mutants, full regression suite, release dogfood A–F, then Debrix handoff.

Every coherent delivery keeps `cargo fmt --check`, `cargo test --all-targets`, and
`cargo clippy --all-targets -- -D warnings` green. The current default daemon is not replaced until
the new release build and isolated acceptance pass.

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
