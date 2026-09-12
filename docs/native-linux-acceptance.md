# Standalone Linux acceptance — native checkpoint passed, rollout in progress

The native first-install bundle at source `57096bf` passed installation from its
extracted archive, process/resource consumers, crash recovery, drained kernel-tree
restoration and replay, active SQL/authority rollback with interrupted recovery,
native Cargo check/test, A-19 idle, and terminal missing-unsealed-boundary refusal.
[Accepted run 34714025080](https://github.com/pyboosted/stillyard/actions/runs/34714025080)
and [canonical evidence and package identity](evidence/mr4-native-ci-20260912/run-34714025080/).
Persistent-host reboot/session recovery, upgrades and further fault coverage
remain open. The records below retain earlier failures and supervised checkpoints.

## Earlier supervised installation

Source `a0c58a7`, native Ubuntu 24.04 Azure VM, kernel 6.17, systemd 255,
UID 1001, ext4 and cgroup v2. This is an ephemeral native CI installation,
independent of the workstation Windows/WSL pair. It is not a persistent-user-host,
logout/reboot, idle-budget or full MR-4 container acceptance report.

[Native run 34705545536](https://github.com/pyboosted/stillyard/actions/runs/34705545536)
installed the daemon outside target and started it under the delegated user
service. The vendor AppArmor bwrap profile was loaded; host-wide user namespace
restrictions remained enabled. All eleven process Jobs below passed their
asserted outcomes, including expected timeout/cancel. Local native Grant IDs
were the corresponding Store-qualified Lease IDs and were released.

Store `01a09677-8b5c-7a21-b09a-2e578f35764a`, daemon generation
`01a09677-8b8d-7381-bed4-0495a525bc6b`, PID 4748. Installed image SHA-256
`8b27e3dd9cf883ce2f9eb26591cf63ab06b8989d27b112a20ecf468e60a59c36`.
[Canonical installation, specs, receipts, logs and status](evidence/mr4-native-ci-20260912/run-34705545536/).

| Scenario | Native Job |
|---|---|
| canary | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-9629-7120-a866-ccc2b03aa583` |
| descendant | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-985b-74d3-904e-f7e07fc7d7b1` |
| parallel-a | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-9c27-7703-8133-14459c5d1827` |
| parallel-b | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-9c38-7760-9747-d78071288fa1` |
| parallel-c | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-9c59-7812-aaf7-c8dfb5265fdd` |
| probe-and-postcondition | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-c02c-78a2-9361-1aee68e1a465` |
| retry | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-c73a-7d03-98f0-b58ea1f83f93` |
| managed-parent | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-cc65-7982-9ba6-ddb05337de4d` |
| managed-child | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-e1ca-7621-8fbc-e60a832e1ef9` |
| timeout | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-eec8-7b12-b72e-9ae1ecbd5997` |
| cancel | `01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-f351-7bb3-9e41-85268f695967` |

The detached descendant had a durable executor seal and a removed cgroup after
root exit; its heartbeat stopped. Two Jobs overlapped under two shared slots,
and the third waited. Probe, primary and postcondition roles executed. Retry
created two Attempts. Managed child context authenticated its parent; repeated
ensure reused the child. Cancellation targeted an already started Job.

The subsequent native Cargo check Job
`01a09677-8b5c-7a21-b09a-2e578f35764a~01a09677-f6ff-7db3-b40c-440de3866907`
failed with DNS resolution of static.crates.io. Its real output revealed that
private /run hid the systemd-resolved file behind /etc/resolv.conf. A follow-up
preserves only that regular resolver file as a read-only mount. A DNS process
control, scheduled native Cargo rerun and daemon-only crash recovery are next.

Workstation WSL Rust 1.85 check/test passed Jobs
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a09677-4f84-7e02-a289-d28c32b3d60d`
and `01a089b1-9a6a-7711-a600-39e2b74e495d~01a09677-4fa1-7b83-9780-71b5da74291c`.
They are compatibility regressions, not substitutes for the pending native Cargo
consumer result. The subsequent resolver change compiled in default WSL check
Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0967c-75ec-7af2-9176-2852707d7915`.
The native crash controller refused WSL in control Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a0967e-1d8a-7970-9fff-8ae96f5f8de6`.

Remaining: native build/test consumers after DNS repair, crash/unknown-history
faults, sustained/idle measurements, a selected persistent Linux host and its
session lifetime, plus the separate container adapter/matrices in full MR-4.

Native follow-up [34706013508](https://github.com/pyboosted/stillyard/actions/runs/34706013508)
(source `e018a66`) passed the resolver control and all native process scenarios.
The daemon crash controller stopped at final Job state before asynchronous
containment cleanup: its early status retained an armed Lease, while the later
canonical status recorded released. Store and authority epoch were retained.
The controller now waits for a durable seal, released allocations and healthy
authority, and pins the complete process identity before its fault. This run
is retained as a failed controller result, not successful crash acceptance.
[Raw evidence](evidence/mr4-native-ci-20260912/run-34706013508/).

Native run [34706337183](https://github.com/pyboosted/stillyard/actions/runs/34706337183)
(source `c08372a`) passed installation, all process controls, daemon-only crash
recovery, and actual installed native Cargo check and test consumers.
Store `01a09685-ec48-7a91-a688-905f40eb4b13`; check Job
`01a09686-96c6-7ac1-ae86-aa01858d0c46`, test Job
`01a09687-1005-7903-aa51-53f344c3b1d5`, crash Job
`01a09686-7d7c-7603-a9b2-1fdc064be7f9` (all qualified by that Store).
Crash recovery retained the supervisor PID, Store and authority epoch, killed
the descendant, sealed and removed its cgroup, released the allocation, and
replayed the original ensure as the same interrupted Job without another launch.
[Canonical native evidence](evidence/mr4-native-ci-20260912/run-34706337183/).

Next controls cover final CPU/RAM/disk admission samples, refusal of strict
process rules with incomplete unprivileged namespace coverage, quiescent journal
corruption with exact restoration, and five-minute idle measurements. The current
native profile has a persistent Python delegation supervisor. A-19's no-helper
condition is therefore **not passed**; quantitative measurement will not hide this
architectural gap or amend the normative limit. A systemd lifetime alternative
is under review.

## Accepted no-helper native checkpoint

[Native run 34707203384](https://github.com/pyboosted/stillyard/actions/runs/34707203384),
source `b5a34dc`, passed the entire installed no-helper suite: all process and
resource controls, exact daemon crash recovery, corrupt executor-history startup
refusal and restoration canary, installed Cargo check/test, and five-minute idle.
A single daemon remained during 300.072 seconds: endpoint-sampled RssAnon
3.914 MiB, no observed CPU tick increase and zero instrumented timer expirations.
The no-helper condition and quantitative A-19 bounds passed on this native VM.
[Canonical evidence](evidence/mr4-native-ci-20260912/run-34707203384/).

CPU/RAM admission retained real satisfied operands; strict CPU/disk release
retained final samples. Incomplete process visibility yielded Job `failed`,
Attempt `safety_failed/quiet_unattainable`, and no primary release. The two
delegation/root inodes survived actual daemon restart. A deliberately invalid
executor checksum prevented startup; restoring its exact original bytes
preserved Store/domain/epoch and allowed a new canary with a sealed boundary.

Remaining native work includes persistent-host/session and quiescent reboot
restoration, active-work history-loss/SQL-rollback and pre-release fault controls,
release packaging and upgrade procedures. Missing executor tree still blocks
startup even after a prior drain; this is not yet an everyday reboot-tolerant
installation claim. Full MR-4 dev-container work remains separate.

After the user's clarification, Windows gates for source `b0786d9` run directly
on the workstation Windows host from a verified disposable NTFS snapshot through
its installed default Stillyard. Native run 34707513945 was canceled as redundant
for that Windows-test-only source change. Source manifests and Job evidence are
retained; the snapshot target will be removed once these gates finish.

Local Windows regression is now passed for source `b0786d9`: default installed
test/check/Clippy Jobs, all released allocations, exact NTFS source projection
and [canonical evidence](evidence/mr4-native-windows-local-20260912/). Its completed
build cache was removed (2.906 GiB logical). Ordinary CI34707513955 passed too;
Windows remote CI was unnecessary for accessing this workstation's native host.

## Drained runtime-tree restoration

Source `e141d37`, native run34712346630, passed the complete no-helper suite,
including drained service/delegation stop, removal of the old executor root,
automatic startup with a new kernel inode and retained Store/domain/epoch/seals.
Original Job `01a096f4-d41a-7c52-8bee-87bb4dad00b3~01a096f5-c1ba-7893-8830-42aece9b4beb`
replayed without another launch; fresh Job
`01a096f4-d41a-7c52-8bee-87bb4dad00b3~01a096f5-e563-7943-83e2-e93a2039e7c3`
launched once and sealed/released. Native Cargo check/test passed afterward.
A-19 measured 300.104s, one daemon, 4.105MiB endpoint RssAnon and zero observed
CPU-tick delta/timer expirations. [Canonical run](evidence/mr4-native-ci-20260912/run-34712346630/).

Review strengthened all 14 missing/corrupt-history controls: a valid delegated
parent now exists before each probe, so missing-parent errors cannot mask a
skipped history check. Immutable anchors/config and restoration receipt IDs are
also compared. These stronger drained controls passed run34712730725 (`324cf97`).
Its subsequent active-work test failed a harness assumption: the namespace dies
with the daemon, so heartbeat is not required afterward. The retained unsealed
Invocation and outstanding SQL/authority rights still forbid restoration.
[Failure and retained original histories](evidence/mr4-native-ci-20260912/run-34712730725/).
The corrected controller and default no-helper installation are queued for the
next native run. These controls retain the same boot and do not close actual
host reboot/session, destroyed-boundary, upgrades or dev-container acceptance.

## Accepted installation bundle and final restoration matrix

Native run34713696102 (`a1d10bf`) first passed the complete extracted-bundle and
restoration matrix. [Retained results](evidence/mr4-native-ci-20260912/run-34713696102/).
Final run34714025080 (`57096bf`) repeated it with pinned wrapper provenance and
mandatory successful canonical collection. Both passed actual terminal kernel
loss; the latter is the selected distributable bundle.

Native Store `01a09714-3580-7c52-baaf-26e21c27da67`, domain
`01a09714-3598-7361-9a78-dc4f13dfa0e3`, journal
`01a09714-359b-7550-9e9e-9537b8f3e1cc`. Installed image SHA-256
`82b258e8548e514d9de060b2f5ea2ea91e865334e92735ba93cfd07cec943a12`.
The following native Job suffixes all use that Store prefix:

| Scenario | Native Job suffix | Result |
|---|---|---|
| Drained restore / original receipt replay | `01a09715-23cb-7380-b464-f9e752be8352` | Same Job, unchanged launch count |
| Fresh Job after root recreation | `01a09715-4744-7471-a8d1-bb1a1a8704a5` | Succeeded, sealed, released |
| Active SQL / authority rollback | `01a09715-4bba-7310-bf03-033ad0a7371a` | Three exact refusals, original bytes restored, interrupted recovery/seal/release/replay |
| Installed Cargo check | `01a09715-6f1b-70d2-968e-76e3c88b2984` | Succeeded, released |
| Installed Cargo test | `01a09715-e69c-7ba1-9e86-36d3cd390797` | Succeeded, released |
| Terminal missing unsealed root | `01a0971c-c806-7c70-bd63-9131a681c3b2` | Restore refused; no seal or release; original outstanding rights retained |

A-19: 300.103 seconds, one daemon, 4.383MiB endpoint RssAnon, 0.00333% of one
logical core, zero instrumented timer expirations. Memory is endpoint sampling,
not an interval peak. The terminal fault occurs after this interval and canonical
collection; it intentionally leaves only the disposable VM's daemon stopped.
This refusal is not recovery from lost boundaries. Actual boot/session and
operator lost-boundary recovery, upgrades and container matrices remain pending.

The selected archive is `stillyard-native-linux-x86_64-57096bfc02be.tar.gz`,
27,906,546 bytes, SHA-256
`dbc664e5ea6c0a223953af84b85f67af12518b1cd9fb196720c8c7bc2500efd6`.
[Package manifest/local location](evidence/mr4-native-ci-20260912/run-34714025080/package.json).
Default WSL Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0971e-b6b8-7490-8b5b-fdcf6cd40cc4`
verified the downloaded archive hash, every manifest file and exact native source/
build origin. This is artifact verification, not WSL being counted as native-host
acceptance. Windows/WSL daemons stayed on their original installed images/PIDs;
the final Rust inputs match the locally accepted Windows source projection.
