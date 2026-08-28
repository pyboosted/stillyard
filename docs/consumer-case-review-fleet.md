# Consumer case: a cross-model review fleet on Stillyard

Status: consumer requirements, input to requirements v0.12
Consumer: `fleet` — a thin Rust client of the `stillyard` crate that runs foreign agent CLIs (OpenAI Codex, Claude Code, xAI Grok) as non-interactive reviewer and implementer jobs on developer workstations, WSL2, and remote Linux/Windows build hosts.
Source of truth for the current pain: `~/.claude/skills/{codex-audit,grok-review,fleet-review}`, `~/.codex/skills/{opus-review,fable-review,grok-review}`, and `reviews/round-*/` in this repository.

This document has three parts: the concrete scenario (§1–§2), the requirements it generates with rationale (§3), and an explicit list of things we do **not** ask Stillyard to do (§4). The current mapping into simplified v0.12 is recorded in `consumer-case-v0.12-disposition.md`.

---

## 1. Who the consumer is and what it does today

One orchestrator (an interactive Claude Code or Codex session, driven by the operator) repeatedly runs **rounds** of reviewers over an artifact — a brief, a spec such as `docs/requirements.md`, or a delivered diff in a git worktree. A round is 3–5 concurrent jobs of different model architectures:

| Reviewer | Binary | Typical wall time | Prompt size |
|---|---|---|---|
| sol | `codex.exe exec -m gpt-5.6-sol -c model_reasoning_effort=xhigh --output-schema … -o …` | 20–60 min | 5–80 KB |
| grok | `grok.exe --prompt-file … --tools Read,Glob,Grep --output-format json --no-leader` | 10–25 min | 5–80 KB |
| Opus ×2–3 | `claude.exe -p --model opus --output-format json --json-schema …` | 5–20 min | 10–80 KB |
| Fable (acceptance gate) | `claude.exe -p --model fable --effort xhigh …` | 20–40 min | 10–80 KB |

Each reviewer must return a JSON verdict (`summary` + `findings[]`) to a known file. The orchestrator validates it (schema, the model that actually answered per `modelUsage` / JSONL, non-empty, not `stopReason: Cancelled`), synthesizes, amends the artifact, and launches a follow-up round. The same machinery dispatches an **implementer** (`codex exec` at `high` in an isolated worktree with an explicit commit policy) and then an **acceptance round** over its delivery. Some rounds must also run device tests (GPU timing lanes) that are only valid on a quiet machine, serially.

Today all of this is ~1 600 lines of PowerShell/bash/Python wrappers behind six near-identical skill documents, plus shell-profile shims to select accounts. What hurts, concretely:

1. **Backgrounding and reattachment.** Runs exceed a single tool-call window, so every skill invents its own detached launcher (`Start-Process` + `<out>.launch.json` + PID polling). `reviews/v11-round-1/` shows the result: `fable-v11-r1.json.launch.json`, `…-retry.json`, `…-retry2.json`, per-round `start-opus-round10.ps1`. When the orchestrator's session is compacted or the terminal closes, the run's ownership is lost.
2. **Containment.** Grok spawns a persistent *leader* process that outlives the call and has asynchronously replayed a previous session's writes into the repo. Codex/Claude child trees survive Ctrl-C. Nothing proves a run is gone.
3. **Account selection is state in five places.** `codex-current` / `claude-current` are functions in two PowerShell profiles, npm `.cmd`+`sh` shims, `~/.local/bin` shims, and fish functions in WSL, with comments saying "keep in sync". The routing decision ("which account has quota left") is a hand-moved pointer.
4. **Resource contention is prose.** "Default 2–3 auditors, not 10, the accounts share one rate bucket", "device lanes only serially, parallel GPU lanes fake regressions", "do not modify the reviewed worktree while a pass is active" — all enforced by instructions to an LLM.
5. **Validation is prose.** "Read the `-Out` file, not stdout", "empty file with exit 0 is not a verdict", "verify `modelUsage` resolves to `claude-fable-5`" — every skill restates it; none of it is a machine-checked postcondition.
6. **Windows ↔ WSL drift.** Skill copies have already diverged (`codex-audit/SKILL.md` by 71 lines, `codex-exec.sh` by 28); one script exists only in WSL.

Stillyard's simplified v0.12 removes items 1, 2, 4, and most of 5 by construction. Account selection remains explicit consumer-owned Job data instead of becoming hidden host-daemon configuration. Item 6 disappears when Linux arrives as the independently gated v0.2 platform.

---

## 2. The scenario, step by step

Repository `C:\Development\the-debrix` (Windows) or `/home/pythonic/the-debrix` (WSL2/Linux). Artifact: `specs/stage-37-brief.md`, uncommitted. Worktree slots `D:\dev-ci\wt\slot-{0..3}` pre-created by `fleet`, detached at HEAD with the uncommitted diff applied. Round directory `D:\dev-ci\stage-37-review\r1\` holds `prompts/_common.md`, one vector file per reviewer, and receives verdicts.

**Step 1 — submit the round as one graph.** `fleet review …` builds five JobSpecs and submits them atomically:

```jsonc
// illustrative, not the normative spec format
{
  "spec_version": 1,
  "jobs": [
    {
      "name": "sol",
      "executable": "C:\\Users\\User\\AppData\\Local\\Programs\\OpenAI\\Codex\\bin\\codex.exe",
      "args": ["exec", "--cd", "D:\\dev-ci\\wt\\slot-0", "--skip-git-repo-check",
               "-m", "gpt-5.6-sol", "-c", "model_reasoning_effort=xhigh",
               "--output-schema", "D:\\dev-ci\\stage-37-review\\findings.schema.json",
               "-o", "D:\\dev-ci\\stage-37-review\\r1\\sol.json", "--color", "never", "-"],
      "stdin": { "file": "D:\\dev-ci\\stage-37-review\\r1\\prompts\\sol.full.md" },
      "workdir": "D:\\dev-ci\\wt\\slot-0",
      "env": { "set": { "CODEX_HOME": "C:\\Users\\User\\.codex-account-1",
                          "CARGO_TARGET_DIR": "D:\\dev-ci\\target\\the-debrix" } },
      "timeout": "90m",
      "expected_duration": "35m",
      "resources": { "cpu_units": 2, "ram_mb": 2048, "codex_account_1_slots": 1 },
      "fences": { "exclusive": ["path:D:\\dev-ci\\wt\\slot-0"], "shared": ["path:D:\\dev-ci\\target\\the-debrix"] },
      "labels": { "round": "stage-37/r1", "role": "reviewer", "backend": "codex" },
      "artifacts": ["D:\\dev-ci\\stage-37-review\\r1\\sol.json"],
      "postconditions": [
        { "exec": ["C:\\Users\\User\\.local\\bin\\fleet.exe", "validate",
                   "--backend", "codex", "--expect-model", "gpt-5.6-sol",
                   "D:\\dev-ci\\stage-37-review\\r1\\sol.json"] }
      ],
      "retry": { "max_attempts": 3, "backoff": "20m", "on": ["process_failed", "postcondition_retryable"] }
    },
    { "name": "grok",   "...": "grok.exe --prompt-file …, explicit account env, resources.grok_slots = 1" },
    { "name": "opus-perf", "...": "claude.exe -p --model opus …, explicit CLAUDE_CONFIG_DIR, resources.claude_slots = 1" },
    { "name": "opus-contract", "...": "same" },
    {
      "name": "collect",
      "executable": "C:\\Users\\User\\.local\\bin\\fleet.exe",
      "args": ["collect", "--round", "D:\\dev-ci\\stage-37-review\\r1"],
      "depends_on": [ { "job": "sol", "on": "terminal" }, { "job": "grok", "on": "terminal" },
                      { "job": "opus-perf", "on": "terminal" }, { "job": "opus-contract", "on": "terminal" } ],
      "labels": { "round": "stage-37/r1", "role": "collect" }
    }
  ]
}
```

**Step 2 — the orchestrator goes away.** It gets a Receipt per job and the group id, prints them to the operator, and its session may end. Later — from a new session, another terminal, or over SSH — it runs `stillyard wait --label round=stage-37/r1 --json` and receives one JSON line per job as each settles, or `stillyard logs <job> --follow` to watch a reviewer think.

**Step 3 — a reviewer spikes.** The Opus perf lens has full tools. Inside slot-2 it writes a micro-benchmark and wants to run `cargo test -p engine-vk --release -- --ignored gpu_timing`. It must not do so while another lane holds the GPU, and it must not hold a `cargo_slots` token itself for the 20 minutes it spends reading. `fleet` first snapshots the benchmark into a pre-created per-child scratch worktree and derives a stable idempotency key plus unique result-file path from `STILLYARD_JOB_ID`, `STILLYARD_ATTEMPT`, reviewer/round, and spike identity; it also includes `STILLYARD_INVOCATION_ID` for defense-in-depth provenance. On first use it runs `stillyard submit --spec spike.json --wait --idempotency-key <fleet-key> --result-file <fleet-result>`; after tool-window loss inside that same logical run it runs `stillyard recover --result-file <fleet-result> --wait`. If recovery proves `not_received` while the parent Attempt is still current, the shim restages and resubmits the exact same payload with the same key/result file; `unknown` never authorizes replay. The child declares its own exclusive scratch fence, `cargo_slots: 1, gpu_slots: 1, vram_mb:<uuid>: 4096`, and a quiet policy; it never mutates the parent-held slot. Before accepting the combined operation, Stillyard conservatively checks the proposed child's primary/probe/postcondition claims against the parent/ancestor Leases; the safe child is then serialized behind the running device lane. Disconnecting the disposable wait does not cancel the child; Submission recovery finds the same device run. A later parent Attempt derives a different path/key and therefore creates fresh child work; a pre-user-code deferral runs no wrapper and keeps the same Attempt. Parallel tool calls use independent keys and disposable waits; v0.1 deliberately has no durable WaitEdge graph.

**Step 4 — verdict lands, gets validated.** The reviewer process exits 0 and `sol.json` exists. The postcondition `fleet validate` checks the schema, that the JSONL/`modelUsage` names the expected model, and that the verdict is not an empty or `Cancelled` envelope. Three outcomes: accepted → Attempt succeeded; rate-limited (Codex quota exhausted mid-run) → retryable failure, Job retries after backoff; malformed verdict → failure, no retry, the orchestrator narrows the prompt by hand.

**Step 5 — collect and iterate.** The `collect` job runs once every reviewer is terminal (including failed ones), writes `r1/summary.md`, and its declared artifacts appear in its snapshot. The orchestrator synthesizes and amends the brief. Before any slot is reused, `fleet` issues `cancel --cascade` for the prior same-slot root and resolves the returned authenticated-child closure. It then submits a Stillyard **reset Job** for each affected main/scratch slot: the reset depends `terminal` on every closure member that used that slot, holds the slot's exclusive path fence, replaces the worktree contents without deleting/recreating the fenced slot root, and succeeds only after cleanup. An uncertain Containment retaining that fence naturally keeps reset blocked; closure final alone is not process absence. Each round-2 reviewer depends `success` on its reset Job and then takes the same exclusive fence. Content replacement under that ordering is not R-JOB-2 unsafe replacement; a changed slot-root identity is. No out-of-band process resets a slot. The original root terminal dependency remains useful for explanation but reset success is the actual reuse gate. Retry backoff may release the old Attempt fence without allowing reset past unfinished dependencies, while uncertain Containment preserves exclusion after terminal publication.

**Step 6 — implementation and acceptance.** `fleet dispatch` submits `codex exec … --cd slot-0` with commit policy `require` on a delivery branch, then an acceptance round whose four reviewer jobs `depends_on: { job: "impl", on: "success" }` and a GPU device suite that depends on `impl` too and carries `gpu_slots: 1` + a quiet policy. `cancel --cascade` on `impl` takes the whole tree down, including the children the implementer itself submitted.

**Step 7 — the same on Linux in product v0.2.** The identical `fleet` commands run inside WSL2 (Ubuntu 26.04, kernel 6.6) and on a remote Ubuntu build host reached over SSH. The daemon there is a Unix-socket daemon under the same user; codex there additionally runs with its own kernel sandbox (`--sandbox read-only`), which Stillyard neither knows nor needs to know about.

---

## 3. Requirements

Numbering is provisional (`C-*` = consumer requirement). Historical **covered/gap/change** annotations explain where the requirement came from; the current disposition is the separate v0.12 mapping document.

### 3.1 Process input and environment

**C-STDIN-1 (gap)** A JobSpec MUST declare the Invocation's standard input as exactly one of: `null` (an open, immediately-EOF handle — not an inherited or closed handle), or `file: <absolute path>` whose bytes are streamed to the child. Probes and postconditions get `null` unless overridden.
*Why (step 1):* `codex exec` takes its prompt from stdin (`-`) and has no prompt-file flag; `claude -p` reads stdin when it is not a TTY. Windows command lines are capped at 32 767 characters and our prompt bundles are 40–80 KB. Without staged stdin every Codex job needs a wrapper script, which R-JOB-2 avoids. `null` must be a real EOF handle: `claude -p` with an inherited-but-never-written stdin hangs.

**C-ENV-1 (R-ENV-1)** Explicit environment additions MUST include the ability to set `PATH` (Windows and Linux). The daemon's own PATH is never inherited; the job's PATH is exactly the persisted value.
*Why (steps 1, 3):* reviewers with tools need `git`, `rg`, `cargo`, `python`, and the agent CLI's own helper binaries. Copy-from-client is the wrong mechanism — the client's PATH is the nondeterminism we are removing.

**C-ENV-2 (R-ENV-1)** Account and toolchain selection MUST be explicit, self-contained Job environment data. Host configuration does not contain named environment presets or precedence rules. Reusable launchers/templates may generate the same explicit Job data for multiple submissions.
*Why (§1 item 3):* an LLM account is represented by values such as `CODEX_HOME=%USERPROFILE%\.codex-account-2` or `CLAUDE_CONFIG_DIR=~/.claude2`. Keeping those values in the Job makes the accepted payload and provenance sufficient to explain which account was selected. The clean base already excludes ambient `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `CLAUDE_CODE_OAUTH_TOKEN`, `CLAUDE_CODE_USE_{BEDROCK,VERTEX,FOUNDRY}`, and `XAI_API_KEY`; the submitting consumer owns any explicit additions.

**C-ENV-3 (gap)** The daemon MUST inject into every Invocation's environment: `STILLYARD_JOB_ID`, `STILLYARD_ATTEMPT`, `STILLYARD_INVOCATION_ID`, `STILLYARD_ROLE` (`primary|probe|postcondition`), and `STILLYARD_ENDPOINT` (the pipe/socket the same-user client should use). Injected names are reserved and rejected in user-supplied `set`.
*Why (steps 3, 4):* the validator postcondition writes per-attempt reports; the nested submission in step 3 needs the endpoint without guessing paths; provenance in verdict files should carry the job id.

**C-ENV-4 (R-ENV-5)** The Linux clean environment is the account-derived `HOME`, `USER`, `LOGNAME`, `SHELL`, `TMPDIR`, and `LANG`, plus explicit Job additions. `STILLYARD_ENDPOINT` is injected directly; `XDG_RUNTIME_DIR`, DBUS, `SSH_*`, `DISPLAY`, `WAYLAND_DISPLAY`, and `WSL_*` are absent unless the Job explicitly supplies them.

### 3.2 Submission, grouping, and waiting

**C-SUB-1 (gap)** Submission MUST accept a **batch**: one document containing several JobSpecs with local names, whose `depends_on` edges MAY reference batch-local names. The batch is accepted or rejected atomically and returns one Receipt per job plus a batch id. R-JOB-1's "foreign dependency" rejection applies to ids outside the batch and the store.
*Why (step 1):* a round is a DAG (N reviewers → collect). Submitting jobs one by one and threading real ids into the next spec leaves a window where the collector does not exist and creates partial rounds on a mid-batch failure.

**C-SUB-2 (gap)** A JobSpec MAY carry **labels**: up to 32 `key=value` string pairs (bounded length, no NUL). `list`, `status`, `wait`, `watch`, and `cancel` MUST accept `--label key=value` (repeatable, AND) as a selector. Labels are immutable after acceptance and are part of the spec hash.
*Why (steps 2, 5, 6):* `wait --label round=stage-37/r1`, `cancel --label round=stage-37/r1`, and a TUI filter are the difference between an orchestrator that can reattach after its own session died and one that has to have written the ids down.

**C-SUB-3 (gap)** `wait` MUST accept a set of jobs (explicit ids, a batch id, or a label selector) and, with `--json`, emit one JSON line per job **as each settles**, then exit with the worst exit code of the set (R-CLI-2 codes). `--any` returns after the first settlement.
*Why (step 2):* "collect verdicts as they land, report each to the operator" is the standing procedure; a wait that returns only when all are done hides a 40-minute straggler behind three finished reviewers.

**C-SUB-4 (gap)** A JobSpec MAY declare **artifacts**: absolute paths the job is expected to produce. After Attempt settlement the JobSnapshot MUST report, per artifact, existence, size, last-write time, and a content hash (or `absent`). Artifacts are informational; absence does not by itself fail the Attempt (a postcondition can).
*Why (steps 4, 5):* the orchestrator should read the verdict path and the spike patch path from the snapshot, not reconstruct them from conventions in six skill files.

**C-SUB-5 (R-PKG-5)** The JobSpec file format MUST be a published, versioned JSON Schema shipped with the crate and printable by the CLI (`stillyard schema spec`). Unknown fields are rejected, so the schema is generated from the same public types the daemon validates.
*Why:* `fleet` generates specs; it needs to validate them at build time and in tests without a running daemon.

**C-SUB-6 (gap)** `submit --wait` and `wait` on a single job MUST offer a **passthrough** mode (`--passthrough`): the job's primary stdout and stderr are streamed to the caller's stdout/stderr as they are committed (R-OBS-2 order, no decoration), and the process exit code is the primary root's real exit code when the Attempt settled `succeeded`/`process_failed`; scheduler exit codes apply to non-process outcomes. The JobSnapshot MUST expose the root exit code of every settled Invocation. The fleet/cargo shim supplies a stable per-logical-run idempotency key and unique atomic result file, binds them to current parent Job/Attempt plus operation identity, and uses the separate `recover` command after loss. A later parent Attempt creates fresh child work. Passthrough recovery uses explicit canonical offsets or replay from zero and never claims exactly-once delivery to external handles.
*Why (step 3):* the consumer puts a `cargo` shim on the job's PATH that routes `cargo test/bench/run` through `stillyard submit --wait --passthrough` so an agent does not need to know the scheduler exists. That only works if the shim is indistinguishable from `cargo`: same bytes on the same streams, same exit code. Without it every agent learns "cargo is broken on this host" and works around the scheduler.

### 3.3 Nested submission and job trees

**C-NEST-1 (gap)** A process running inside an Invocation MUST be able to act as a same-user client of the daemon (using `STILLYARD_ENDPOINT`) and submit jobs. Such a job records `parent_job` (the submitting Invocation's Job) in its spec.
*Why (step 3):* the reviewer that wants to run a device test must not hold `gpu_slots` for its whole life, and must not run the lane concurrently with another lane. Letting it submit the lane as a child job is the only design in which the scheduler, not prose, serializes device work. This is also how an implementer runs its own build/test verification under `cargo_slots`.

**C-NEST-2 (R-JOB-5, R-NEST-2)** `cancel --cascade`, force stop, timeout, and uncertain Containment of a parent Job MUST also cancel every unfinished job whose authenticated parent chain reaches it. Children are otherwise ordinary jobs: they do not inherit the parent's Leases and are admitted independently.
*Why (step 6):* a killed implementer must not leave its self-submitted `cargo test` child running against a slot the next round is about to reset.

**C-NEST-3 (change to R-SCHED / documentation)** The requirements MUST state the deadlock rule for nested submission: a parent that holds a scalar and waits on a child that needs the same scalar can deadlock if capacity is 1. Product v0.1 deliberately avoids a general wait graph: managed waits are restricted to authenticated descendants and conservatively reject any target whose next primary/probe/postcondition claim conflicts with a Lease held by the waiter or its ancestors (`blocked_by_ancestor`).
*Why:* this is the first trap a reviewer will fall into (`cargo_slots: 1` on both). A blocker is enough; preemption is not requested.

### 3.4 Postconditions and retry

**C-POST-1 (R-JOB-4, R-RUN-6)** An executable postcondition's exit code MUST be classified by the spec into three sets: `accepted`, `retryable`, and `failed` (default: `0` accepted, everything else failed). A `retryable` classification settles the Attempt as `postcondition_retryable`, which MAY appear in the retry policy's verdict subset. `postcondition_failed` stays nonretryable.
*Why (step 4):* the verdict validator can distinguish "the model was rate-limited / the run was cancelled upstream" (retry after 20 minutes on the same prompt is correct) from "the model returned prose instead of JSON" (retrying the same prompt is waste; the orchestrator must narrow it). Today both are a hand-run `retry2`.

**C-POST-2 (R-JOB-7)** Bounded diagnostic tails of probe and postcondition output MUST be readable through the public snapshot / `status --json`, not only through the TUI.
*Why (step 4):* the validator's one-line reason ("modelUsage = claude-opus-5, expected claude-fable-5") is what the orchestrator reports to the operator.

**C-POST-3 (R-JOB-4, R-RES-7)** Finite retry backoff of minutes to hours and `expected_duration` per Job are sufficient; `fleet` knows typical durations per backend/effort.

### 3.5 Resources and fences

**C-RES-1 (R-RES-1, R-RES-6)** Custom scalar capacities are exactly how rate buckets become schedulable: host config declares `codex_account_1_slots = 2`, `codex_account_2_slots = 2`, `grok_slots = 1`, `claude_account_2_slots = 3`; every reviewer Job claims one. A Job may claim several custom scalars and fences in the same atomic Lease.

**C-RES-2 (R-RES-8)** Path-fence identity for a **missing leaf** MUST be the pair (identity of the longest existing ancestor, case-folded canonical remainder), not the ancestor identity alone.
*Why (steps 1, 5):* with ancestor identity alone, `slot-0` and `slot-1` under a not-yet-created `D:\dev-ci\wt\` both resolve to `D:\dev-ci\` and, as exclusive fences, falsely conflict — four reviewers would serialize on a phantom.

**C-RES-3 (covered)** Shared fence on the warm `CARGO_TARGET_DIR`, exclusive fence on the worktree slot, `cargo_slots` token for builds, `gpu_slots` + `vram_mb:<uuid>` + quiet policy for device lanes — all expressible in simplified v0.12. Listed so the acceptance matrix has a realistic multi-fence Job to test against.

**C-RES-4 (consumer rule, no scheduler change)** Before reusing a slot in a later round, `fleet` MUST cascade-cancel the prior same-slot root, resolve its complete authenticated-child closure, and submit a fenced reset Job depending `terminal` on every closure member that used that main/scratch slot. The reset may replace all contents but MUST preserve the stable identity of the fenced slot root. The later Job depends `success` on reset and takes the same fence; no out-of-band reset is permitted. Terminal closure alone is insufficient because a final Job may coexist with uncertain Containment, whose retained Lease correctly blocks reset until proof or explicit clearance. Canceling/settling only the Batch is also insufficient because ordinary parent completion and a disconnected disposable wait do not cancel detached children. Child mutations use fleet-provided scratch slots. Attempt fences release during retry backoff, so dependencies order pre-final work while the reset Job's fence supplies the final proof-sensitive handoff.

### 3.6 Linux as the product v0.2 platform

**C-LINUX-1 (superseded product decision)** Product v0.2 conformance MUST include Linux x86_64 under the stricter kernel/systemd prerequisites selected by `requirements.md`, covering both a native Ubuntu host and Ubuntu under WSL2 with systemd enabled. Product v0.1 remains Windows-only. No root, no capabilities, no container runtime.
*Why (step 7, §1 item 6):* half of our reviewer runs are in WSL — it is where codex has a real kernel sandbox — and the perf hosts are Linux. Two platforms with one spec is the whole point; a Windows-only v0.1 recreates the Windows/WSL drift one level down.

**C-LINUX-2 (gap)** The Windows primitives MUST have specified Linux counterparts, each with its own proof rule:

| Concept | Windows v0.1 | Linux v0.2 |
|---|---|---|
| Local IPC (R-PKG-4, R-LINUX-1) | owner-only named pipe | owner-only Unix-domain socket in the fixed per-UID runtime location; no IP socket |
| Peer identity (R-ENV-4, R-LINUX-1) | owner SID + PID/start/executable check | peer credentials + PID/start/executable check; another uid and root reject |
| Singleton/store | cross-session mutex + one SQLite writer | `flock` plus socket-bind exclusivity + one SQLite writer; stale socket removal only after the lock is won |
| Containment (R-RUN-2, R-LINUX-2) | Job Object, kill-on-close, no breakaway | a dedicated delegated cgroup v2 leaf with `cgroup.kill`; no process-group/subreaper fallback |
| Root identity (R-RUN-2, R-LINUX-2) | boot ID, PID, creation time | boot ID, PID, `/proc/<pid>/stat` starttime, pidfd |
| Cleanup proof | zero processes in Job Object + no matching root identity | `cgroup.events populated=0` + no matching pidfd/root identity |
| Secrets (R-ENV-3) | platform owner-protection facility | platform owner-protection facility; absence blocks only secret-referencing Jobs |
| GPU provider (R-RES-2, R-LINUX-3) | in-process NVML | in-process NVML; absent/stale provider blocks VRAM claims |
| Metrics (R-RES-4, R-LINUX-3) | in-process Windows APIs/NVML | in-process procfs/cgroup/NVML; no helper processes |
| Detach (R-PKG-4, R-LINUX-2) | daemon outside console/SSH lifetime | persistent systemd user manager with linger; no double-fork fallback for conforming v0.2 |

**C-LINUX-3 (R-ENV-5)** On Linux, "executable" means anything `execve` accepts, including `#!` scripts; the daemon still performs no shell parsing. The Windows batch/script restriction remains Windows-only.
*Why:* `~/.local/bin/claude` and `codex` on the Linux side are shebang launchers; refusing them would force a shell-mode wrapper for every Linux job.

**C-LINUX-4 (gap, documentation + doctor)** WSL2 stops the VM shortly after the last interactive session ends, which would stop the daemon and every job. The requirements MUST state that Stillyard does not keep the VM alive, and `doctor` on WSL2 MUST report whether `.wslconfig` has `vmIdleTimeout` disabled (or an equivalent keepalive), because US-2 ("closing that SSH session does not kill … submitted work") is otherwise false on WSL2 through no fault of the daemon.

**C-LINUX-5 (R-LINUX-4, A-20)** Every platform-neutral scenario MUST run on both platforms; native containment, crash, PID reuse, detachment, socket family/mode, and no-listener variants run independently. There is one conforming cgroup-v2 tier, not a weaker fallback.

### 3.7 Provenance

**C-OBS-1 (R-OBS-4)** Attempt provenance MUST record the effective **non-secret environment** names and values with secrets redacted.
*Why:* "which Codex account produced this verdict" is a question the operator asks when a limit is exhausted or when two accounts diverge in plan/model access. The executable hash alone does not answer it — both accounts run the same `codex.exe`.

**C-OBS-2 (R-JOB-2, R-JOB-7, R-RUN-2, R-OBS-4)** Executable identity/hash per Invocation is provenance; agent CLIs may self-update while queued or between Attempts. Replacing an ordinary executable at the same canonical path is expected and launches the newly verified image, while disappearance or a Windows file-to-directory/reparse/type change fails before release.

---

## 4. Explicitly not requested (stays in `fleet`)

- Knowledge of Codex/Claude/Grok, their flags, models, efforts, or output envelopes.
- Verdict schema validation and model-identity checks (these are the postcondition executable).
- Git worktree creation, reset, slot assignment, spike-diff capture (git-specific; `fleet` pre-creates slots and fences them).
- Prompt assembly, injection-guard preambles, review procedure, synthesis.
- Sandboxing of the file system: containment of the *process tree* is Stillyard's; isolation of the *repository* is by construction (disposable worktrees), and per-CLI kernel sandboxes where they exist are the CLI's own flags.
- LLM rate-limit probing. Buckets are modeled as custom scalars; quota exhaustion mid-run is a retryable postcondition, not a provider.

Nothing above names Debrix, a project path, or a credential format, so the §20 boundary holds; examples should keep using generic `cargo`, GPU-measurement, and "external agent CLI" wording.

---

## 5. Priority for v0.12

Blocking for the first Windows consumer run (a round cannot be submitted or reattached without them): **C-STDIN-1, C-ENV-1, C-ENV-2, C-ENV-3, C-SUB-1, C-SUB-2, C-SUB-3, C-SUB-5**. **C-LINUX-1..3** gate the first Linux consumer run in product v0.2.

Needed before the first implementer/acceptance cycle: **C-NEST-1..3, C-POST-1, C-POST-2, C-SUB-4, C-SUB-6, C-RES-2, C-LINUX-4..5, C-OBS-1**.

Everything marked *covered* is listed so it lands in the acceptance matrix with a realistic multi-fence, nested, two-platform job graph rather than a synthetic one.
