# Observed resource and quiet admission verification

Verified on 2026-08-29 against the installed per-user Windows daemon and CLI.

## Shipped artifact

- Installed executable: `C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe`
- SHA-256: `650A8615A392069B22B27E4CF194F3BF8C3E8A60A9DC55E165EB4A3F50DDDDFD`
- Final daemon PID after provider recovery: `52160`
- Store UUID: `01a04a7c-8e88-7583-a5b0-8e2c7bc1798b`
- Final daemon generation: `01a04a80-fb36-7cc2-aba4-11a40340e699`
- GPU UUID: `GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a`
- Driver: `610.88`

The final `doctor` run reported no NVML, GPU-placement, RAM, or process-rule coverage failure.
The only transient warnings were CPU and disk sampler warm-up.

## Build and test gates

All Rust commands were submitted through the installed default Stillyard daemon with
`scripts/run-stillyard-job.ps1`.

| Gate | Job | Result |
|---|---|---|
| formatting | `...~01a04ca0-7d1d-72a1-adfa-b10b5393f3df` | passed |
| full test suite | `...~01a04ca0-a74b-7ba0-8dcf-c7bf3f0c4cf6` | passed: 166 library tests, 11 CLI/TUI tests, isolated and public API tests; 5 environment-specific tests ignored |
| check | `...~01a04ca0-87ca-7c02-a799-9d1c1904c004` | passed |
| clippy | `...~01a04ca0-fbfb-7841-b093-6839fa7aa849` | passed |
| release build | `...~01a04ca1-1bff-7e31-a79e-ab8d2611647e` | passed |

The TUI detail view is covered by `detail_identifies_the_job_working_directory`; it renders the
job working directory as `CWD: ...` and omits absent/default fields.

## Adversarial tests and independent review

- A-04 stale-memory mutant failed as required in Job
  `...~01a04a7d-8dc6-7d40-b320-0655971dde38`.
- A-05 stale-quiet mutant failed only the release-barrier integration assertion in Job
  `01a0499c...~01a04a65-3619-78a3-aaca-bf1f2b7bb84c`; the mutant was then removed.
- Final Fable xhigh closure review: clean, no product defects.
- Final Grok 4.6 closure review: `findings: []`.

## Shipped-path dogfood

### A — cargo and sidecar correctness overlap

- Cargo lane Job: `...~01a04a7f-2bb5-7612-b893-acee015d2710`, succeeded,
  `started=1787956243443`, `finished=1787956253638`.
- Sidecar GPU Job: `...~01a04a7f-2bd1-7d93-9911-28d5e2f9a340`, succeeded,
  `started=1787956243472`, `finished=1787956253674`.
- The executions overlapped for about ten seconds. The sidecar omitted quiet and did not serialize
  against the `cpu_heavy` cargo Job.
- Its receipt contains GPU UUID `gpu-a1144c26-a15c-cba1-3b7a-870c755ef08a` and driver `610.88`.

### B — strict measurement waits on impact

- Strict Job: `...~01a04a81-1dae-7dd2-8576-ac89de13dbc2`.
- While `cpu_heavy` Job `...~01a04a81-1b98-7471-bb98-80acf4d42b76` was active, the strict Job
  remained pending with `impact_busy: measurement incompatible with active cpu_heavy`.
- It had no Invocation and `target/dogfood/b-released.txt` was not created. The probe was canceled
  after the blocker was recorded.

### C — external blocked process resets quiet

- Strict Job: `...~01a04a7f-e7e0-7931-987a-84147aac0eaa`.
- An external process with PID `94476` and basename `cargo.exe` produced
  `quiet_contaminated: blocked_process pid=94476 basename=cargo.exe`.
- The Job had no Invocation and no release marker. It was canceled after verification.

### D — observed RAM and granted debits

- First 24000 MiB Job: `...~01a04a7f-76da-7912-a2b0-364d089e210a`, succeeded,
  `finished=1787956268855`.
- Second 24000 MiB Job: `...~01a04a7f-7700-78a1-972e-ff3867cd299c` initially remained pending
  with `resource_busy: requested 24000, available 8768, configured 32768` and then succeeded,
  `started=1787956268892`.
- Therefore the second Job started only after the first released its debit. A-04 separately proves
  stale RAM evidence cannot release a child.

### E — provider down fails closed

- With host `gpu_provider=disabled`, `doctor` reported failed NVML and GPU-placement coverage.
- GPU+VRAM Job `...~01a04a80-bbcf-7c03-aaf0-5b29e0216f09` remained pending with
  `observation_missing: nvml: NVML provider is disabled by host policy`.
- It had no Invocation and no release marker. The probe was canceled, the configuration was restored
  to NVML, and a fresh daemon passed the corresponding doctor coverage checks.

### F — shell processes in the ignore list do not contaminate

- `dwm.exe` PID `62468` was present.
- Blocked-process-only quiet Job `...~01a04a80-15a5-7441-bffb-51276976e3e6` succeeded after its
  one-second stable window.
- The final detector sample recorded `observed=0`, `satisfied=true`; the receipt contains the exact
  GPU UUID and driver.

## Verdict

The shipped daemon satisfies the Debrix increment rows A–F. Strict policies fail closed when their
evidence is absent or contaminated, quiet admission happens before Invocation creation, sidecar Jobs
without quiet remain independent, observed RAM debits serialize oversized concurrent requests, and
GPU grants carry the provenance required to retire Debrix journals.
