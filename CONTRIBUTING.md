# Contributing

Stillyard is implementing the frozen [v0.12 baseline](docs/requirements.md). Before proposing a change, identify the requirement and acceptance row it satisfies.

Run the local gates before opening a pull request:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Changes to the public contract require an explicit requirements amendment with an executable acceptance scenario or negative-control mutant. Implementation details that preserve the contract should stay in code and tests rather than expanding the requirements.

By contributing, you agree that your contribution may be licensed under either Apache-2.0 or MIT, at the user's option.

