# Phase 8a isolated daemon instances — implementation handoff

Date: 2026-08-28  
Repository: `C:\Development\stillyard`  
Branch: `main`  
Working-tree state before this document: clean  
Local HEAD: `2b3e2bf6ab75ae293497e89c642579efdae6a4b0`  
Published `origin/main`: `494e627756cb891783934581bea17e018225929f`

## Objective

Finish and publish isolated Stillyard daemon instances so Stillyard's own tests and consumers such
as `moot` can run a pinned daemon with a temporary store and unique endpoint without contacting,
replacing, or locking the owner's default daemon/store.

The requested surface is:

```text
stillyard daemon --store <absolute-dir> --endpoint <local-pipe>
STILLYARD_STORE=<absolute-dir> STILLYARD_ENDPOINT=<local-pipe> stillyard daemon
stillyard --endpoint <local-pipe> status|wait|cancel|doctor|watch|...
Client::builder().endpoint(...).daemon_executable(...).auto_start(false)
```

## Completed work

Two coherent local commits exist and are not pushed yet:

- `a10d63e docs: design isolated daemon instances`
- `2b3e2bf feat: isolate daemon instances`

The contract is in [phase-8a-isolated-daemon-instances.md](phase-8a-isolated-daemon-instances.md).
The implementation currently provides:

- CLI/env store and endpoint selection with CLI precedence;
- connect-only custom endpoints (no custom/default auto-start leakage);
- default auto-start with explicit default coordinates and ambient override removal;
- lifetime store lock and owner-only endpoint mutex lease;
- `FILE_FLAG_FIRST_PIPE_INSTANCE` as a second endpoint collision guard;
- endpoint-scoped managed coordinates and server-derived Windows Job membership;
- effective endpoint injection into scheduled children;
- pinned arbitrary-path daemon executable authentication;
- `DaemonSnapshot.endpoint` and protocol version 10;
- a live Windows test with two pinned daemon copies, two stores/endpoints, nested CLI routing,
  foreign IDs, wrong executable, both collision axes, cross-instance managed environment, and
  kill/restart with store UUID continuity.

Validation on `2b3e2bf` was green:

```text
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test
git diff --check
```

The last full test run reported 106 library tests passing (3 ignored helpers), 6 binary tests
passing, and the live isolated-daemon test passing. The worktree was clean before review.

## External review state

Review artifacts live outside the repository at:

```text
C:\Development\review-artifacts\stillyard-instance-isolation\
```

Files:

- `review-brief.md` — frozen contract plus implementation diff;
- `opus.json` — complete, two Opus lenses;
- `fable.json` — complete, Fable xhigh, session
  `e8cc9440-0c33-46fb-bc05-c3c84076f636`;
- `grok.json` — still pending at handoff time.

The Grok launcher was still running at handoff time:

```text
launcher PID 27760
grok PID     11472
state        C:\Development\review-artifacts\stillyard-instance-isolation\grok.json.launch.json
```

Do not kill it merely because it is slow. On resumption, first check whether `grok.json` appeared
and read it. If the process is still alive, let it finish. Its invocation is exact Grok 4.6, high
reasoning, read-only, subscription OAuth.

Opus sessions, if a focused same-session follow-up is needed:

- Windows IPC/concurrency: `73334a71-f55a-4fda-903d-164f19846ebc`
- authority/consumer harness: `6b2f371b-2ce7-4de8-898b-7d59fec312d3`

## Confirmed changes still required

Keep this bounded. These are local Phase 8a corrections, not reasons to reopen the architecture.

### 1. Require a complete explicit instance tuple

Current `daemon::run` independently falls back each coordinate. Consequently,
`stillyard --endpoint X daemon` can lock the owner's default store, and
`stillyard daemon --store X` can claim the owner's default endpoint.

Before resolving values, compute whether store and endpoint were selected through either the CLI or
their environment variables. Reject XOR selection. Both explicit, or neither explicit, are valid.
Default auto-start already passes both explicitly and removes ambient variables.

Add tests for CLI/env combinations and update the Phase 8a contract to state that an isolated daemon
must select both coordinates together.

### 2. Claim the first pipe instance before opening the store or starting the scheduler

Current order is mutex lease -> store lock/open -> scheduler start -> first `CreateNamedPipeW` in
`serve`. If the pipe-name backstop fails, queued work may have begun before startup fails.

Refactor only the first pipe creation:

1. acquire the endpoint mutex lease;
2. create one owner-only pipe instance with `FILE_FLAG_FIRST_PIPE_INSTANCE`;
3. acquire/open the store;
4. start the scheduler;
5. pass the pre-created handle into `serve` as its first listening instance.

Use an RAII handle so every early return closes the pipe. Transfer ownership exactly once to the
first connection worker. Subsequent listening instances may use the existing loop. A small bounded
retry for first-instance creation after crash/restart is reasonable, but must occur before the store
is opened and scheduler exists.

Do not implement the suggested permanently reserved, never-served pipe handle: a client could attach
to that instance and hang. The pre-created handle must become the first real listener.

### 3. Make invalid endpoint input non-retryable and restrict Windows names to ASCII

`validate_endpoint` currently returns `Error::Unavailable`, so consumer readiness loops mistake a
permanently invalid endpoint for a daemon that is still starting. Return `Error::InvalidSpec` (or an
equivalent non-retryable public error) for malformed endpoint input.

Also reject non-ASCII Windows pipe names. The endpoint mutex key uses ASCII case folding while NPFS
name comparison is not safely represented by that normalization. ASCII endpoint names make both
guards agree and avoid homoglyph/case ambiguity.

### 4. Add executable controls for auto-start and managed-scope mutants

Add tests that leave `auto_start` at its default and select an absent explicit endpoint, both via
`ClientBuilder::endpoint` and `STILLYARD_ENDPOINT`. They must immediately return the explicit/custom
endpoint error and must not spawn a daemon. Prefer a small pure selection helper plus unit tests, or
a canary executable whose invocation would create a marker.

Make default auto-start structurally pass `default_endpoint()` rather than `client.endpoint`, even
though the current guard already makes them equal. This prevents a later guard regression from
pairing the default store with a custom endpoint.

Extend `isolated_client_helper`: after proving A-scoped coordinates are suppressed when explicitly
connecting to B, connect to inherited endpoint A with the same forged coordinates and assert that
`submission_context` is rejected. This kills the "always suppress" client mutant and proves
same-endpoint coordinates are reauthenticated.

Match expected error variants/messages for foreign ID and wrong-image cases instead of accepting any
error.

### 5. Preserve the Linux strict-clippy gate

Fable identified that `resolve_store_root` and `resolve_endpoint` are only consumed by the Windows
daemon but are compiled on non-Windows. Add `#[cfg_attr(not(windows), allow(dead_code))]` or gate
them appropriately, then run the Linux-equivalent strict build if available. Linux runtime remains
v0.2, but the existing CI gate must compile now.

### 6. Avoid rejected-store filesystem side effects where practical

`resolve_store_root` creates the selected directory before volume/reparse validation. Validate the
nearest existing ancestor first, then create, validate the full path (including every reparse
component), and canonicalize. Do not simply canonicalize before creation because the selected path
may not exist.

The existing `require_fixed_local_ntfs` already walks existing path components and rejects reparse
points, so Opus's claimed reparse bypass is not present; only the pre-rejection creation side effect
remains.

## Reviewed findings that should not be implemented as stated

### `Global\` mutex privilege claim is false

Both Opus and Fable expressed uncertainty that creating a global mutex requires
`SeCreateGlobalPrivilege`. Microsoft documents that this privilege check applies to global
file-mapping and symbolic-link objects, not mutexes. Keep the global owner-SID-scoped mutex unless a
real standard-user test falsifies this.

Primary references:

- <https://learn.microsoft.com/en-us/windows/win32/termserv/kernel-object-namespaces>
- <https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-createmutexw>

### Canonical `\\?\` store paths are not automatically a defect

The Phase 8a instance tuple deliberately names a canonical store root, and the live test asserts the
canonical path. Do not strip the verbatim prefix merely to satisfy the old display spelling unless a
real consumer contract requires it. If changed, separate internal canonical identity from display
format and test both; do not weaken store identity.

### Do not expand into a new supervisor or database design

The public crate remains a client API; consumer harnesses own the pinned daemon child process. No
store migration is needed: the product is greenfield, so incompatible schema work may drop/recreate
the local database rather than introducing store v2/v3/v4 machinery.

## Recommended implementation order

1. Wait for/read Grok; verify only concrete findings against source.
2. Patch complete-tuple validation and tests.
3. Move first pipe creation ahead of store/scheduler with RAII ownership.
4. Patch endpoint validation/ASCII, auto-start selection, managed mutant tests, and non-Windows
   dead-code gate.
5. Apply the nearest-existing-ancestor store validation improvement if it remains small.
6. Run formatting, strict clippy, all tests, the isolated live test, and `git diff --check`.
7. Commit the review fixes coherently.
8. Send focused follow-ups only for any accepted High/Critical finding. Medium/Low fixes may be
   closed by local evidence; do not start another broad convergence loop.
9. Change the Phase 8a document status from review candidate to reviewed/frozen and record review
   dispositions.
10. Commit the frozen baseline, push `main`, verify `origin/main == HEAD`, public GitHub visibility,
    and a clean worktree.

## Final validation commands

```powershell
Set-Location -LiteralPath C:\Development\stillyard
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test --test isolated_daemon -- --nocapture
git diff --check
git status --short
git log -5 --oneline
git rev-parse HEAD
git rev-parse origin/main
gh repo view --json nameWithOwner,visibility,url
```

If Cargo reports contradictory stale metadata after parallel review/test activity, the previously
successful recovery was `cargo clean -p stillyard`, followed by the normal gates. Do not delete the
repository or user stores.

## Completion condition

Phase 8a is complete only when the bounded review fixes are committed, all gates are green, accepted
High/Critical findings have focused closure evidence, the design status is frozen, the public remote
contains the final commits, and the worktree is clean. Until then, the active implementation goal is
unfinished rather than blocked.
