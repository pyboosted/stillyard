# Windows/WSL operation

The installed development pair uses one Windows machine coordinator. Windows
and WSL Jobs compete for the same `cargo_slots`, impacts and machine budgets.
Each manager owns its own Jobs and receipts; a Job ID includes that Store UUID.
The [acceptance ledger](machine-resource-implementation-status.md) identifies
the installed binaries, accepted source snapshots and remaining delivery work.

## Inspect and submit

On the reference workstation, the Windows executable is
`C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe`.
The WSL executable is `$HOME/.local/share/stillyard/bin/stillyard`.
Both are outside build targets. Use the relevant executable with `daemon-status`,
`doctor --json`, `list`, `status JOB_ID` and `logs JOB_ID --stream stderr --json`.
`machine events` on Windows provides ordered Grant changes across the pair.

Read the endpoint from `daemon-status` and pass it explicitly to `ensure`:

```bash
cli="$HOME/.local/share/stillyard/bin/stillyard"
endpoint="$HOME/.local/share/stillyard/stillyard-v6.sock"
"$cli" --endpoint "$endpoint" ensure --spec /absolute/job.json \
  --idempotency-key YOUR_STABLE_UUID --result-file /absolute/receipt.json --wait
```

Use a distinct stable key for each intended submission. Repeat the same key,
specification and result file to recover a lost reply. A pending Job can be
waiting for Windows work; queueing is expected when the only Cargo token is held.
The JSON outcome and canonical Job status distinguish scheduler failure from
client/transport failure.

## Build and consumer workflows

All Cargo work on this workstation is scheduled, including formatting and
Clippy. The [contributor instructions](../AGENTS.md) list both launchers.

```bash
python3 scripts/run-wsl-job.py test \
  --repository-root /absolute/source-snapshot \
  --source-manifest /absolute/source.json \
  --evidence-directory /absolute/evidence
```

The native equivalent is `scripts/run-stillyard-job.ps1 test` with
`-RepositoryRoot` and `-EvidenceDirectory`. Match source manifests before
comparing Windows and WSL results. Linux targets use `target/scheduled-linux`;
Windows targets use `target/scheduled`.

`scripts/machine-resource-consumer.py` runs inside a Stillyard Invocation:

- `review` invokes the configured subscription CLI with a concrete brief;
  `validate-review` checks its actual model route and typed result as a
  postcondition. The profile and CLI must already be configured.
- `managed-build` ensures a child from an explicit JobSpec and stable operation
  key, then waits through the managed API. Agent consumers invoke this adapter.
- `measure` hashes the identified source payload and records CPU/wall duration.
  `scripts/run-installed-wsl-measurement.py` submits it with predefined criteria
  and cross-machine measurement exclusions.

The [three accepted rounds](evidence/mr3-consumer-rounds-20260910z3/rounds.json)
link real native/WSL builds, CLI reviews, managed children and measurements.
Their retained JobSpecs and receipts make the accepted commands inspectable.

## Service lifetime and maintenance

The WSL user unit delegates cgroup v2 CPU, memory and PID controllers. A
persistent supervisor keeps the delegated tree alive and starts the installed
daemon in a separate subgroup. On daemon failure it restarts that daemon; the
new daemon reconciles its independent executor journal before releasing Grants.
The supervisor does not clean user Invocation boundaries.

Use the maintenance helpers for replacement: `upgrade-wsl-daemon.py` verifies a
successful release Job and exact candidate hash; `upgrade-wsl-service.py`
verifies the helper's system Job. Both require no active Leases, no unresolved
containment, a sealed executor journal and an empty executor tree before stopping
the unit. Their receipts retain the prior image/helper and Store identity.
An arbitrary `systemctl restart` during active work is not an accepted upgrade
procedure: stopping the whole unit can remove empty cgroups before their seals
are durable. A control process that cannot be terminated leaves maintenance
waiting with delegation retained.

Windows replacement uses `install-windows-daemon.py` and a validated native
release Job. The WSL pairing pins the Windows bridge hash. After replacing that
image, `rotate-wsl-bridge-pin.py` performs the explicit matching old/new pin
rotation under the same idle barriers. A recorded interrupted rotation resumes
with its original intent; creating new pairing identities is not recovery.
Host configuration changes take effect on daemon restart through maintenance.

The reference WSL user has linger enabled. A separate Windows-owned foreground
`wsl.exe` session runs `wsl-service.py keepalive` and supplies the pinned interop
socket. This is separate from the WSL systemd unit: linger alone does not keep
the WSL VM alive. Persistent scheduled-task registration and logout/cold-start
acceptance are still open in the ledger. Closing one terminal is insufficient
to establish those guarantees.

## Failure diagnosis and capability boundaries

A missing bridge or stale peer blocks new attached starts. Already started
work can continue under local containment; its machine Grant remains held.
Reconnection reconciles retained history and lost acknowledgements. The live
bridge-loss acceptance verifies Windows waits both during Linux execution and
after local cleanup until the coordinator receives the release.

An uncertain containment can have no remaining processes and still lack a
durable seal. Inspect the incident and executor evidence; an absent PID or a new
daemon generation does not prove cleanup. The live cleanup-failure acceptance
verifies that restoring inspectable cleanup allows automatic Grant release.

Missing/corrupt pairing history and replaced Stores require explicit recovery;
ordinary startup does not regenerate identities or declare resources free.
Whole-VM shutdown, distro termination, suspend/resume, host reboot and Windows
logout still need the remaining controlled acceptance and an external Windows
observer. Any test affecting unrelated work requires a separate agreed window.

WSL provides CPU, memory, disk and process observations, plus cgroup containment.
Guest GPU observation is explicitly unsupported. Machine admission quantities
are reservations; the executor tree's cgroup limits and WSL VM limits are
separate hard limits. Physical Windows and guest memory observations are not
added together as independent free capacity.

Installed alpha.20 diagnostics include a `runtime_timer_expirations` doctor
check. Its summary contains process-local counts for reactor, attached-driver,
subscriber, transport and error-backoff timers. Compare snapshots from the same
daemon generation over five minutes; these counters do not count IPC signals as
timers and do not grant admission or prove cleanup. The installed z9 fault control exercised each Linux timer path; the ledger
separates those controls from five-minute idle measurements and helper coverage.

When an installed manager is alive but its own scheduling path is broken, a
recovery release build may use the accepted protected default Windows bootstrap
Job. `upgrade-wsl-daemon.py --bootstrap-build` checks the actual coordinator's
retained bootstrap work and sealed-empty proof against that candidate.
`--allow-sealed-release-pending` permits replacement only for final work whose
actual journal seals, consumed Tickets, resolved Invocations and empty cgroups
all match. It leaves Lease/Grant history intact for normal protocol release.
The default updater still refuses outstanding Leases. These flags do not
authorize SQL resets, forced resource release or updating over running work.
