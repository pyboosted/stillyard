# Post-integration CI fixture portability repairs

The first recorded main CI run after integration, 34700100279 at `c40e7af`,
failed the Ubuntu test
`store::attached::tests::attached_admission_persists_candidates_and_holds_lease_until_release_ack`:
"attached installation belongs to another owner". The fixture hardcoded UID 1000;
that did not match the GitHub runner's owner. Both OS formatting and Clippy steps
passed. Ubuntu reported 267 passed, 1 failed, 19 ignored; matrix fail-fast then
canceled Windows testing. Original metadata and failed-job logs are retained.

Commit `2441fed16aeee87b7a911b9340e9aadb98a68310` derives Linux fixture ownership
from the actual temporary Store directory. The change is entirely inside the
`#[cfg(test)]` module. Production installation validation still rejects owners
different from `geteuid()`. No runtime ownership validation was weakened and no
installed daemon was rebuilt or restarted. The original z11f complete source-file
map predates this test-only change; production code remains identical.

Local formatting ran as default attached Stillyard Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a09616-c31d-7e10-aaac-159a867680f4`.
It succeeded; the later canonical snapshot shows Windows Grant
`01a05f1f-858c-7880-8c15-d55875da9e6b~01a09616-c367-7e80-9577-928500c1adf2`
released. Launcher inputs, durable receipt, canonical status and logs are retained.
No local compilation cache was created for this repair. Remote GitHub Actions
supplies the actual different-owner regression environment.

The existing independent Codex handoff reviewer separately inspected the patch
and production validator. Verdict: approved; test fixture only, with no runtime
change or ownership-check weakening. This was a platform agent review, not an
Opus verdict or a local CLI Job. The original handoff's full MR-3 lifecycle
limitations remain in force.

## Windows functional-test deadlines

Run 34700394953 at `2441fed` passed Ubuntu, but Windows failed two existing
postcondition tests (297 passed, 2 failed, 4 ignored). Diagnostic-only commit
`6b915e7` retained complete Job snapshots in assertions. Its run 34700649223
again passed Ubuntu and confirmed both Windows failures were `TimedOut` at the
fixture's 10-second Attempt deadline. In the retry test the primary succeeded
but the postcondition timed out; in the tree test the primary timed out before
postconditions could run. Raw snapshots and run metadata are retained.

Commit `89c4e7b` gives only these two functional tests 60 seconds for PowerShell
startup and execution. Production deadlines, the default fixture budget and
dedicated timeout tests remain unchanged. The tree's descendant now waits
indefinitely inside its Job Object instead of naturally exiting after 30 seconds,
so a longer test budget cannot falsely satisfy the cleanup assertion. Existing
retry classification, same-Job second Attempt, immutable result, descendant
absence and empty-containment assertions remain intact. The independent Codex
reviewer approved this patch and found no new escape/leak path: the existing
Job Object terminates descendants during cleanup/timeout and on last handle close.

Both Rust edits were formatted as default attached Stillyard Jobs, with retained
launcher receipts and later canonical released-Grant snapshots:

- Diagnostics: L`01a0961c-cdc3-7ea3-bfc8-cce14c5d51f4`.
- Test budgets: L`01a09621-c7a9-7730-800b-40a3c3a09a42`.

L abbreviates Store `01a089b1-9a6a-7711-a600-39e2b74e495d~`.
The source-continuity record verifies that both changed source files retain
identical production code before their `#[cfg(test)]` module boundary.

The next run, 34700910522 at `89c4e7b`, passed Ubuntu and the Windows retry test.
The tree primary still timed out at 60 seconds without publishing the expected
PID file (Windows: 298 passed, 1 failed, 4 ignored). This disproved a sufficient
fix by increasing the deadline alone. The old PowerShell tree-launch fixture's
specific startup failure remains undiagnosed; its timeout and empty stderr are
retained, not presented as successful containment acceptance.

Commit `e1d4f09` replaces that primary tree setup with an ignored helper mode of
the test executable. This helper is explicitly launched by the nonignored test;
no acceptance test was disabled. The child publishes its own PID and waits
indefinitely. The primary waits for complete expected PID content and confirms
the child is still alive, then exits 25. The actual PowerShell postcondition and
all result/cleanup assertions remain unchanged. This removes reliance on nested
PowerShell `Start-Process` behavior while preserving a real native descendant
inside the Job Object. The independent reviewer approved the helper with that
complete-content readiness check and identified no new containment escape.

Scheduled formatting Jobs for the helper and its readiness refinements:
L`01a09627-ec32-71f0-80fe-9d74ce3b09ba`,
L`01a09628-9258-7672-b805-d8fa90c51eeb`, and
L`01a09628-c0d3-73b2-88c3-fa73aa7b1ed8`.
Their launcher and canonical evidence is retained under the matching ci-tree
directories. These jobs do not substitute for Windows execution validation.

## Windows CI native compiler environment

Run 34701288360 at `e1d4f09` passed Ubuntu and all Windows unit tests: 299 library
tests (5 helper-only ignored entries) and 35 CLI tests. Both repaired functional
tests passed. The subsequent Windows isolated-daemon suite reported 20 passed,
1 failed and 6 helper-only ignored entries: the NVML generation fixture could
not find `cl.exe` when compiling its C DLL. This is a missing CI toolchain
environment, not a failed NVML-generation assertion. The full failed log and
run metadata are retained.

Commit `e27bf17` initializes the existing MSVC installation in Windows CI through
`vswhere` and `VsDevCmd`, exporting only PATH, INCLUDE, LIB and LIBPATH for later
steps. No third-party action, production source change, test exclusion or local
direct compiler invocation was introduced. The native NVML fixture test remains
enabled and must execute its actual assertions in the next run.
The independent Codex reviewer found no blocker in the environment setup;
the actual Windows CI run validates its command parsing and fixture build.

## Accepted result

[Run 34701637074](https://github.com/pyboosted/stillyard/actions/runs/34701637074)
at `e27bf173b84b757d6a9ad82582285d3b80554299` completed successfully on both
Windows and Ubuntu. Formatting, Clippy (all targets/features, warnings denied)
and the full all-features tests passed on both OSes. Windows passed the repaired
postcondition tests and the actual NVML generation integration test. Metadata,
complete logs and extracted test-result lines are retained in accepted-run.json,
accepted-run.log and accepted-test-results.txt.

All production source remains identical to the installed accepted build. This
CI result validates the test/CI follow-up; it neither rebuilds the installed pair
nor upgrades deferred VM/distro/logout/suspend/reboot cases to PASS.
