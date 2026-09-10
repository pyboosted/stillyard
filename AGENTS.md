# AGENTS.md — Stillyard contributor entry point

Read this file before changing or validating Stillyard.

## Current host lifecycle restriction

The user prohibits stopping or restarting the working `Ubuntu-SSD` distribution
and shutting down the shared WSL VM. Do not run `wsl --shutdown`, terminate
`Ubuntu-SSD`, restart WSL host services, or perform host sleep/logout/reboot tests.
Lifecycle fault work must target a separately identified disposable test distro,
under a native default Stillyard Job. Existing other distros are not disposable
without the user's confirmation. Preserve working-distro continuity evidence.
Whole-VM acceptance remains explicitly deferred; a test-distro restart does not
prove whole-VM recovery. This restriction supersedes older prepared window specs.

## Worktree and snapshot resource cleanup

After completing work with a worktree or source snapshot, remove its disposable
build caches (`target/`) and temporary artifacts without waiting for a user
reminder. Before removal, verify that no running or queued Job, process, or
planned acceptance step still needs them. Retain canonical evidence, source
changes, installed binaries, and any artifacts still required for rollback.
Record substantial cleanup in the status/evidence ledger, including paths and
space reclaimed. Keep only the caches needed for current work; do not accumulate
a full target directory for every completed snapshot. Perform cache removal as
filesystem maintenance; Cargo invocations remain subject to the Job invariant
below.

## Build and test invariant

On this host, every Stillyard `cargo` build, check, test, Clippy, and rustfmt invocation MUST run as a Job on the system default Stillyard daemon. Do not invoke `cargo` directly from an agent shell, editor task, or ad-hoc script.

Use the checked-in launcher and JobSpecs:

```powershell
& .\scripts\run-stillyard-job.ps1 fmt
& .\scripts\run-stillyard-job.ps1 fmt-write  # apply rustfmt after Rust edits
& .\scripts\run-stillyard-job.ps1 check
& .\scripts\run-stillyard-job.ps1 test
& .\scripts\run-stillyard-job.ps1 msrv-check  # Rust 1.85 acceptance check
& .\scripts\run-stillyard-job.ps1 msrv-test   # Rust 1.85 acceptance tests
& .\scripts\run-stillyard-job.ps1 clippy
& .\scripts\run-stillyard-job.ps1 schema-update  # intentional public schema changes only
& .\scripts\run-stillyard-job.ps1 build-release
```

The canonical CLI and default daemon executable is:

```text
C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe
```

The daemon must run from that installed path, never from a Cargo target directory. Generated JobSpecs use `<selected checkout>\target\scheduled` as `CARGO_TARGET_DIR`, so a build cannot overwrite or lock the running daemon. Use an isolated daemon only inside tests that explicitly verify isolated-instance behavior; it is not a substitute for the system default daemon that schedules the test itself.

All new local validation commands and handoff evidence must name the Stillyard Job or resulting Job ID. Direct-Cargo results are inadmissible.

## Stored jobs

Canonical definitions live under `.stillyard/jobs/`. They deliberately carry an explicit clean toolchain environment, `cargo_slots: 1`, the `cpu_heavy` impact, and `project=stillyard` labels. Update those definitions when the host toolchain or canonical repository path changes; do not reintroduce daemon-side environment profiles.

All `*.json.in` files, including `msrv-check.json.in` and `msrv-test.json.in`, are portable templates, not directly-submittable JobSpecs. The launcher expands their `${...}` placeholders into a temporary JobSpec using the selected checkout, user profile, Rust toolchain, and the Visual Studio/Windows SDK environment discovered through `vswhere` plus `VsDevCmd`. Do not replace those placeholders with reference-host paths.

The optional `-RepositoryRoot <absolute Windows path>` selects a separate source snapshot without overwriting another checkout. `-EvidenceDirectory <absolute Windows path>` retains the submitted JobSpec and durable receipt. From WSL, use Windows PowerShell to invoke this Windows launcher. This is a native Windows validation path; it does not launch Linux Cargo. The MR-0 WSL bootstrap remains gated on its own containment proof.

The system daemon configuration must provide at least one `cargo_slots` token. A queued Job is expected when another Cargo workload owns that token; bypassing the scheduler is not an acceptable workaround.

## Installed WSL validation (MR-3)

The installed default WSL manager is now paired with that same Windows machine
coordinator. As required by MR-3.3, Linux validation moves from bootstrap to this
manager; Windows remains the authority for the shared machine `cargo_slots`.
The Linux CLI/daemon is `${XDG_DATA_HOME:-$HOME/.local/share}/stillyard/bin/stillyard`,
outside Cargo targets. Run Linux gates through the checked-in launcher:

```bash
python3 scripts/run-wsl-job.py check --evidence-directory /absolute/evidence/path
python3 scripts/run-wsl-job.py test --evidence-directory /absolute/evidence/path
```

It also supports `fmt`, `fmt-write`, `clippy`, `msrv-check`, `msrv-test`,
`schema-update`, and `build-release`. Templates under `.stillyard/jobs/linux/`
retain explicit toolchain/environment, `cargo_slots: 1`, and `cpu_heavy`.
`--repository-root` selects an isolated source snapshot; `--source-manifest`
checks its exact bytes. Targets remain `<snapshot>/target/scheduled-linux`.
Every invocation must still be a system Stillyard Job: direct Linux Cargo and
an unpaired/isolated scheduler are inadmissible. Record the WSL Job ID and actual
Windows Grant ID. The launcher verifies both installed default daemons report
the same healthy machine and authority; attached execution requires a real Grant
and a fresh Invocation Ticket. No local fallback is permitted on disconnect.
