# Windows/WSL operation

The installed development pair uses one Windows machine coordinator. Windows
and WSL Jobs compete for the same `cargo_slots`, impacts and machine budgets.
Each manager owns its own Jobs and receipts; a Job ID includes that Store UUID.
The [acceptance ledger](machine-resource-implementation-status.md) identifies
the installed binaries, accepted source snapshots and remaining delivery work.
The [daily-use handoff](windows-wsl-acceptance.md) separates accepted behavior
from deferred lifecycle checks. Standalone Linux and containers remain MR-4;
the commands here target an already installed, paired WSL2 manager.

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

Keep the Linux Store and durable receipt on local ext4, such as your WSL home
directory. `/mnt/c` is not supported for durable Linux receipts. For a first
Linux Job, the following creates a private ext4 directory, discovers the actual
installed endpoint and records a stable submission intent. Run the generated
`submit.sh` again to recover the same Job:

```bash
python3 scripts/prepare-wsl-example.py --directory "$HOME/stillyard-hello"
bash "$HOME/stillyard-hello/submit.sh"
```

The example executes `/usr/bin/python3` through the installed default manager.
For a project command, prepare a new JobSpec with its absolute executable,
working directory, explicit environment and resource claims. Rust builds must
still use the scheduled launcher or a managed child Job, as described below.

On the reference Ubuntu 26.04 image, direct fd-based execution of the multicall
coreutils `printf` fails with `coreutils: unknown program '3'`, even with the
requested argv[0]. An actual system Job reproduced the difference between normal
path execution and fd execution. For such a command, submit an explicit
interpreter wrapper, for example `/usr/bin/python3` with arguments
`["-c", "import os; os.execv('/usr/bin/printf', ['printf', 'hello\\n'])"]`.
The wrapper executes under the same Invocation containment; its selected primary
image is Python. The later target is not separately pinned as the primary image.
Do not describe this workaround as transparent support for every multicall
executable. Direct execution compatibility remains an open follow-up.

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
On this workstation, the user prohibits stopping Ubuntu-SSD or the shared VM
and host sleep/logout/reboot tests; see [AGENTS.md](../AGENTS.md). Historical
prepared shutdown specifications do not authorize those operations.

The executor currently requires its exact recorded cgroup and Linux boot when
sealing cleanup. If a distro/VM stop destroys an unsealed Invocation boundary,
its Grant can remain retained after restart. Keep the incident evidence and
wait for an explicitly supported recovery; do not delete the Store, journal or
pairing anchor, or rotate identities to make the resource appear free.

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

For the five-minute idle interval, close installed CLI viewers on both sides and
avoid opening clients or submitting Jobs until collection finishes. The observer
runs outside Jobs by design, verifies empty queues and client inventories, then
retains an additional 35-second boundary-settlement observation:

```bash
python3 scripts/observe-installed-idle.py --windows-keepalive-pid PID \
  --evidence-directory /absolute/new-idle-evidence
python3 scripts/validate-installed-idle.py /absolute/new-idle-evidence \
  --installation-plan /absolute/accepted-pair/plan.json
```

The second command produces an explicit condition-by-condition acceptance file.
It requires the accepted pair's sibling `after.json`, exact installed image hashes
and generations, full helper inventory, source-audited wait paths, native lifetime
cycle totals and timer budgets. Observer completion alone is not aggregate idle
acceptance. Memory is sampled at interval endpoints using the specified native
private-byte/Linux RssAnon metric; an interval peak is not claimed.
