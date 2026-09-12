# Standalone Linux acceptance — in progress

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
