# Native Linux implementation and acceptance

Started 2026-09-12 from `49b9b95`, branch `linux`, at the user's request.
This is the standalone part of MR-4. The full phase also requires container
adapters and the two/ten-container Linux and WSL matrices from the original
plan. Deferred MR-3 lifecycle cases remain deferred; the working Ubuntu-SSD
distribution and shared WSL VM must remain running.

## Existing implementation and gaps

Linux already has owner-authenticated Unix IPC, cgroup v2 containment, a trusted
exec-stop barrier, pidfd/process identity, a reset-independent executor journal,
managed-client authentication and CPU/RAM/disk/process observation. The installed
execution path requires a WSL pairing anchor, external coordinator Tickets and
release acknowledgements. An ordinary unpaired Linux Store deliberately has no
execution capability. Native Windows already uses local atomic admission and
reset-independent NativeStartPermission records with the same admission core.

The native Linux path must preserve those local transactions and use real Linux
containment. Creating a synthetic Windows pairing or silently switching an
attached Store to local authority is not an acceptable implementation.

## Planned implementation

1. Add explicit stopped native Linux installation, a private durable anchor and
   fixed owner Store binding. Installation records the executor cgroup and
   journal, initializes the fresh local authority explicitly, and refuses an
   existing attachment, outstanding work or ambiguous prior installation.
   Missing/corrupt/mismatched anchors and reset history remain closed to starts.
2. Select an installed Linux runtime independently of WSL attachment. Reuse
   the journal, cgroups, process attestation, log settlement and reconciliation.
   Native starts use the existing local Lease/Grant transaction and durable
   NativeStartPermission; attached starts retain their actual coordinator Ticket
   and release protocol. Record native Linux boundary provenance truthfully.
3. Provide a persistent Linux user-service installation and a portable scheduled
   launcher. Require actual cgroup v2 delegation, supported local durable storage
   and owner IPC. First native reference profile and minimum kernel/systemd
   capabilities must be justified by a real host preflight. No process-group
   cleanup fallback or implicit authority creation during daemon startup.
4. Exercise real native Jobs, postconditions, probes, retries, cancellation,
   descendants, managed children and durable submission replay. Exercise daemon
   failure, retained executor history, unknown history and stale/missing boundary
   controls. Validate CPU/RAM/disk/process observation, capabilities, service
   lifetime and the standalone idle budget independently of WSL evidence.
5. Run matched Windows/WSL regression gates under the installed default scheduler
   and native-host gates under its installed default scheduler. Record source,
   image, host/boot/Store/domain identities, Job/Attempt/Grant IDs and exact
   canonical results. Installed native acceptance requires a native host, not
   merely Linux compilation or isolated WSL fixtures.

## Work and evidence status

The independent Codex architecture pass identified the required ordering:
validate the installation and open its executor journal before native authority
recovery can retire permissions; journal intent precedes authority/SQL release;
guarded primaries must record their suspended root; native and attached journal
intents are mutually exclusive. Cleanup must use the same seal on normal and
recovery paths. The namespace wrapper's WSL-only `/init` mask must be selected by
runtime profile. Installation completion is published only after authority,
journal and SQL bindings agree. These are implementation obligations, not an
acceptance verdict. Opus authentication was checked and reported expired; no
Opus model invocation or verdict is claimed.

The kernel's [cgroup v2 contract](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
defines inherited membership, recursive populated evidence and hierarchical
limits. Native host preflight must verify the actual delegation and namespace
policy rather than relying on kernel version alone.

| Item | Status | Evidence / next condition |
|---|---|---|
| Source baseline | recorded | main `49b9b95`; unchanged installed alpha.20 pair |
| Separate native host | requested | Awaiting host selection; implementation can proceed |
| Native installation and recovery contract | implemented, unit-tested | Explicit anchor and full SQL/authority/executor inventory gate |
| Native launch and kernel acceptance | implemented, native execution pending | WSL regression Jobs passed; native kernel acceptance still required |
| Native service, packaging and live consumers | staged | One-shot installer and shared lifetime helper; native host acceptance pending |
| Windows/WSL regressions | WSL local gates passed, Windows pending | [Core checkpoint](evidence/mr4-native-core-20260912/); all local Cargo through system Jobs |
| Container runtime/profile and matrix | pending | Later part of full MR-4; no container support claim |

The [preflight control](evidence/mr4-native-preflight-20260912/) and
[core checkpoint](evidence/mr4-native-core-20260912/) distinguish local scheduled
regressions from pending native installation. Keep one active cache per platform
and clean completed caches after checking Job/process references.

Version decision for this development checkpoint: JobSpec 4, IPC 25 and existing
public schemas are unchanged. The native setup command uses a private optional
SQL table and a version-1 installation anchor. The version-1 executor journal
omits the new optional native intent when absent; an exact serialization test
preserves old WSL bytes/checksums. No installed upgrade or compatibility acceptance
is inferred from this decision.

## Native installation runner

The `native-linux.yml` workflow uses a disposable Ubuntu 24.04 native VM for the
first installation probe. The pre-install CI build is explicitly a bootstrap;
subsequent Cargo check/test runs use the installed native default Stillyard Jobs.
This does not replace a persistent user host, logout/reboot, idle-budget or full
consumer/container acceptance. The standalone service requires `DelegateSubgroup`,
present in the [systemd 254 resource-control contract](https://raw.githubusercontent.com/systemd/systemd/v254/man/systemd.resource-control.xml).
Actual delegation and namespace preflight remain mandatory.

A follow-up independent review found non-atomic setup receipt publication and
installer success preceding daemon startup. The helper now publishes the complete
receipt atomically; the installer waits boundedly for the exact installed native
Store/domain/process to become healthy. Default WSL Job
`01a089b1-9a6a-7711-a600-39e2b74e495d~01a0966b-75f1-72e2-8454-e5215d681bf5`
passed five setup controls, syntax checks for all new launchers, and the candidate
CLI negative control: native installation rejects WSL before creating its Store.
[Canonical follow-up evidence](evidence/mr4-native-core-20260912/native-service-controls-v2/).
