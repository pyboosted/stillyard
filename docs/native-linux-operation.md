# Standalone Linux preview

The `linux` branch contains a native Linux manager, local coordinator and cgroup
executor. It uses one local Lease/Grant and needs no Windows coordinator.
[Acceptance evidence](native-linux-acceptance.md) distinguishes the accepted
no-helper native CI installation from earlier supervised checkpoints.
This remains a development preview; persistent-host reboot recovery and upgrades
are not yet accepted. Containers use a separate adapter planned in MR-4.

## Reference profile

The disposable reference machine is native Ubuntu 24.04 x86-64, systemd 255,
ext4 Store, cgroup v2 with delegated CPU/memory/PID controllers, Python 3.12 and
bubblewrap. The installed user manager has linger enabled. Ubuntu's vendor
bwrap AppArmor profile must permit the actual namespace exec-stop prerequisite;
disabling the host-wide user namespace restriction is unnecessary.

The installer rejects WSL, container hosts, an existing installation, a changed
candidate digest, unsupported storage and missing containment prerequisites.
It currently requires systemd 254 or newer and Python 3.11 or newer. Version
numbers alone are insufficient: the executable prerequisite probe must pass.

## First installation

The native workflow produces a `native-linux-install-bundle` artifact after
successful acceptance. It contains a tarball and SHA-256, the identified binary,
manifest, prerequisites, service installer and this guide. Extract it and run
`python3 install.py --evidence-directory /absolute/path/to/evidence`; add `--apply`
to install. Package verification is a digest check, not a release signature.
The current bundle is a development candidate from its recorded native CI build.

Use a separately selected native host and a candidate with retained build
evidence. Existing Windows/WSL installations use their own operator guide.
The following only prepares a reviewable installation plan:

```bash
python3 scripts/install-native-linux.py \
  --candidate /absolute/path/to/stillyard \
  --candidate-sha256 <recorded-sha256> \
  --build-origin <retained-build-Job-or-CI-URL> \
  --evidence-directory /absolute/path/to/evidence \
  --ram-mb 4096 --cargo-slots 2
```

Add `--apply` for the first installation. The default native profile
creates two user units: `stillyard.service` runs the installed daemon;
`stillyard-delegation.service` retains the executor cgroup without a persistent
process. A short setup command initializes the explicit Store once. Later daemon
starts validate retained history and never repeat that initialization. The
older supervised preview is no longer offered by the first-install tool.
The former `--no-helper` flag remains accepted for command compatibility.

The binary is installed at `${XDG_DATA_HOME:-$HOME/.local/share}/stillyard/bin/stillyard`,
outside Cargo targets. Its Store, native anchor, authority and executor journal
are durable private state. Preserve them together; deleting a journal or cloning
selected SQLite rows is not an installation or recovery procedure.

## Submit work

Use the installed CLI's `daemon-status` and `doctor --json` to inspect the
selected Store, authority, containment and detector coverage. For this repository,
the portable launcher schedules Cargo as a real native default Job:

```bash
python3 scripts/run-linux-job.py check --evidence-directory /absolute/path/to/evidence
python3 scripts/run-linux-job.py test --evidence-directory /absolute/path/to/evidence
```

The launcher also supports fmt, fmt-write, Clippy, Rust 1.85 checks/tests and
release build. All later Cargo on an installed host should follow this route.
Targets use the selected checkout's `target/scheduled-linux`; preserve one useful
cache and remove completed snapshot caches after checking Job/process references.

Other programs use the same JobSpec 4 and explicit environment. Supply a stable
idempotency key and a durable result file to `ensure`; retain the returned Job ID
and use that receipt after a client disconnect. Select the observed endpoint
explicitly. On an SSH host, run the CLI on that host: its local coordinator owns
its resources, regardless of which laptop initiated the SSH session.

## Capability boundaries

| Capability | Native reference status |
|---|---|
| Local slots, complete claims, two concurrent Jobs | Accepted under installed native Jobs |
| Descendants, cancellation, timeout, retry and all Invocation roles | Accepted on the no-helper reference installation |
| Managed child authentication and repeated ensure | Accepted |
| Cargo check/test consumer and resolver through private `/run` | Accepted |
| Daemon crash, retained epoch, cleanup seal and original submission | Accepted on the no-helper reference installation |
| CPU/RAM admission and strict CPU/disk release evidence | Accepted with actual native samples |
| Namespace-wide process rules | Incomplete when root-owned executable identities cannot be inspected; strict policy stays closed |
| GPU/NVML | No GPU acceptance on the reference VM; unavailable evidence is not zero load |
| Five-minute standalone idle / no-helper | Accepted: one process, 4.383 MiB endpoint RssAnon, zero measured timer expirations |
| Drained service/delegation loss, same-boot recovery and receipt replay | Accepted on the disposable native VM; every old Invocation must be sealed |
| Active SQL/authority rollback | Refused independently by retained history; exact restoration and interrupted recovery accepted |
| Missing unsealed kernel root | Refused with outstanding rights retained; operator recovery is pending |
| SSH logout, host reboot, user-manager restart | Changed-boot/session acceptance still pending |
| Upgrade/downgrade and existing installation replacement | Not implemented by the first-install tool |
| Dev-container domains | Separate pending MR-4 adapter and matrices |

Resource claims govern admission. The executor subtree also has an aggregate
memory/PID boundary; this is not a promise of per-Job CPU or memory partitioning.
Missing, corrupt or inconsistent history blocks execution and retains uncertainty.
Do not restart the delegation unit as a substitute for resolving an incident.
