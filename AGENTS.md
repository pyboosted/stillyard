# AGENTS.md — Stillyard contributor entry point

Read this file before changing or validating Stillyard.

## Build and test invariant

On this host, every Stillyard `cargo` build, check, test, Clippy, and rustfmt invocation MUST run as a Job on the system default Stillyard daemon. Do not invoke `cargo` directly from an agent shell, editor task, or ad-hoc script.

Use the checked-in launcher and JobSpecs:

```powershell
& .\scripts\run-stillyard-job.ps1 fmt
& .\scripts\run-stillyard-job.ps1 check
& .\scripts\run-stillyard-job.ps1 test
& .\scripts\run-stillyard-job.ps1 clippy
& .\scripts\run-stillyard-job.ps1 build-release
```

The canonical CLI and default daemon executable is:

```text
C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe
```

The daemon must run from that installed path, never from a Cargo target directory. The JobSpecs use `C:\Development\stillyard\target\scheduled` as `CARGO_TARGET_DIR`, so a build cannot overwrite or lock the running daemon. Use an isolated daemon only inside tests that explicitly verify isolated-instance behavior; it is not a substitute for the system default daemon that schedules the test itself.

All new local validation commands and handoff evidence must name the Stillyard Job or resulting Job ID. Direct-Cargo results are inadmissible.

## Stored jobs

Canonical definitions live under `.stillyard/jobs/`. They deliberately carry an explicit clean toolchain environment, `cargo_slots: 1`, the `cpu_heavy` impact, and `project=stillyard` labels. Update those definitions when the host toolchain or canonical repository path changes; do not reintroduce daemon-side environment profiles.

The system daemon configuration must provide at least one `cargo_slots` token. A queued Job is expected when another Cargo workload owns that token; bypassing the scheduler is not an acceptable workaround.
