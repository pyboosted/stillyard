# Contributing

Stillyard is implementing the frozen [v0.12 baseline](docs/requirements.md). Before proposing a change, identify the requirement and acceptance row it satisfies.

Run the local gates before opening a pull request:

Use the checked-in system-daemon launchers from `AGENTS.md`: `fmt`, `check`, `test`, and `clippy`.
Direct Cargo validation is not admissible on the Stillyard development host.

Changes to the public contract require an explicit requirements amendment with an executable acceptance scenario or negative-control mutant. Implementation details that preserve the contract should stay in code and tests rather than expanding the requirements.

Machine resource scheduling follows [the phased plan](docs/machine-resource-scheduling-plan.md),
[protocol contract](docs/machine-resource-protocol.md), and
[status/evidence ledger](docs/machine-resource-implementation-status.md).
Design acceptance is separate from installed-platform and live-consumer acceptance.
For a Windows snapshot selected with `-RepositoryRoot`, retain the generated specs and
receipts with `scripts/run-stillyard-job.ps1 <gate> -EvidenceDirectory <directory>`.
For installed WSL validation use `python3 scripts/run-wsl-job.py <gate>
--evidence-directory <directory>`, with `--repository-root` and
`--source-manifest` for acceptance snapshots. This submits to the installed WSL
manager paired with the default Windows coordinator; both share machine tokens.
All nine native gate names are supported, including Rust 1.85 checks and tests.
Use the retained receipt to recover a lost client; do not create a new submission
to work around a queue or an unavailable bridge.

By contributing, you agree that your contribution may be licensed under either Apache-2.0 or MIT, at the user's option.
