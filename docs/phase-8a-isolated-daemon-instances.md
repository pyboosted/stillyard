# Phase 8a — Isolated daemon instances

Status: implemented review candidate (2026-08-28)

This bounded foundation makes a Stillyard daemon instance explicitly addressable without changing
scheduling semantics. It exists so Stillyard's own live tests and external consumers such as `moot`
can run a pinned daemon and temporary store without contacting or replacing the owner's default
daemon.

## Instance identity and defaults

A daemon instance is the runtime tuple:

```text
(canonical store root, canonical local endpoint, expected daemon executable)
```

The store UUID remains the durable authority carried by every public durable ID and cursor. The
endpoint selects a live instance; the expected executable authenticates the selected pipe server.
Neither endpoint names nor filesystem paths are bearer authority.

With no override, behavior is unchanged:

- the store is the existing per-user local-data directory;
- the Windows endpoint is `\\.\pipe\stillyard-v6-<sha256(owner SID)[0..16]>`; and
- an unmanaged client may auto-start the sibling `stillyard.exe` only for that complete default
  selection.

`stillyard daemon --store <absolute-dir> --endpoint <local-pipe>` starts a foreground explicit
instance. `STILLYARD_STORE` and `STILLYARD_ENDPOINT` provide the same daemon inputs, with command-line
values taking precedence. Explicit stores must be absolute, canonical fixed local NTFS paths with no
reparse component. Explicit Windows endpoints must be nonempty local `\\.\pipe\NAME` paths, never a
remote UNC pipe. A client uses `ClientBuilder::endpoint` or the CLI's global `--endpoint`; an explicit
builder/CLI value takes precedence over `STILLYARD_ENDPOINT`.

`STILLYARD_STORE` is daemon-only. Clients select an instance by endpoint and learn its store path and
UUID from `DaemonSnapshot`; they never inspect a selected store directly. Default auto-start passes
the default store and endpoint explicitly and removes ambient instance overrides. Any endpoint
selected explicitly or through `STILLYARD_ENDPOINT` is connect-only and never auto-starts a daemon.

## Exclusive ownership

The existing `daemon.lock` remains the lifetime singleton for one canonical store root. A second
lifetime endpoint lease, keyed by owner SID plus canonical endpoint, prevents two Stillyard daemon
processes from serving different stores as instances of the same Windows named pipe. The daemon
acquires the endpoint lease before the store lock and holds both until process exit. Contention is a
startup error; it is never retried as a pipe accept failure.

Consequently:

- same store plus another endpoint fails on `daemon.lock`;
- same endpoint plus another store fails on the endpoint lease;
- different store and different endpoint coexist; and
- process death releases both claims without a stale cleanup protocol.

The pipe retains its owner-only DACL and `PIPE_REJECT_REMOTE_CLIENTS`. A unique endpoint changes only
the address, not the peer-authentication boundary.

## Client and managed-environment rules

`ClientBuilder::daemon_executable(path)` remains the expected-image selector used for server PID/image
authentication. The path may be a pinned arbitrary location such as
`target/pinned-stillyard-<revision>/stillyard.exe`; R-ENV-4 does not mean "installed at the default
path." The bundled CLI expects its own executable unless an embedding client selects another path.
Protocol mismatch and wrong image still reject.

Managed coordinates are scoped to the endpoint that injected them:

- if `STILLYARD_JOB_ID`, `STILLYARD_ATTEMPT`, and `STILLYARD_INVOCATION_ID` are present, their
  `STILLYARD_ENDPOINT` must also be present;
- when the selected endpoint equals that inherited endpoint, the client presents the coordinates and
  the daemon reauthenticates them from exact pipe-peer membership in its current Job Objects;
- when an explicit selected endpoint differs, inherited coordinates belong to another instance and
  are not presented; the selected daemon independently derives whether the peer is managed by it;
- incomplete coordinates, or a same-endpoint mismatch between claimed coordinates and OS membership,
  fail closed.

This permits a test process scheduled by the owner's default daemon to act as an unmanaged client of
its explicitly selected temporary daemon. It does not let a process opt out of managed identity on
the endpoint that contains it: server-derived membership remains authoritative even when no claim is
sent.

Every primary and postcondition receives the effective daemon endpoint in `STILLYARD_ENDPOINT`, not
the per-user default. Nested clients therefore return to the instance that launched them. The clean
child environment does not expose `STILLYARD_STORE`.

## Public and CLI surface

The minimal surface is:

```text
stillyard daemon [--store DIR] [--endpoint PIPE]
stillyard [--endpoint PIPE] submit|recover|status|list|events|wait|logs|watch|daemon-status|cancel|doctor ...
```

`--endpoint` is one global CLI option rather than a separate spelling on every command. Commands that
do not connect to a daemon ignore it. `DaemonSnapshot` reports the effective endpoint alongside the
existing store UUID and canonical store/config paths.

The public crate does not become a daemon supervisor. Consumer harnesses spawn the pinned foreground
binary with `std::process::Command`, retain its exact child handle, connect using
`ClientBuilder::endpoint(...).daemon_executable(...).auto_start(false)`, and terminate/wait that exact
process during cleanup. Abrupt termination is an intentional recovery test primitive; a temporary
store may be discarded only after the daemon process is gone.

## Acceptance and mutants

1. **Default compatibility.** With no flags or instance environment, default store, pipe, and
   unmanaged auto-start are byte-for-byte unchanged. An `ambient-store-changes-autostart` mutant
   fails.
2. **Two live instances.** Two pinned copies with distinct temporary stores and endpoints run
   concurrently, report their own canonical store/endpoint and different store UUIDs, and accept only
   their own IDs/cursors. Neither contacts an already-running default daemon.
3. **Two-dimensional singleton.** Same-store/different-endpoint and
   same-endpoint/different-store second daemons fail promptly while the first lives, then can start
   after the owner exits. `store-lock-only` and `pipe-name-joins-foreign-server-family` mutants fail.
4. **No custom auto-start.** Explicit builder/CLI endpoints and `STILLYARD_ENDPOINT` return
   unavailable when absent; they never start the default or a guessed custom daemon. An
   `explicit-endpoint-autostarts-default` mutant fails.
5. **Pinned image.** A client that pins the copied executable connects to that daemon from its
   arbitrary path. Pinning a different executable rejects before protocol traffic. A
   `custom-endpoint-skips-image-check` mutant fails.
6. **Effective child endpoint.** A real scheduled child observes the selected instance endpoint and
   a nested client returns to it. Injecting `default_endpoint()` fails the test.
7. **Cross-instance managed scope.** A process carrying valid managed coordinates for instance A can
   explicitly use endpoint B as an unmanaged peer of B. Selecting A still presents and reauthenticates
   the coordinates; incomplete or forged same-endpoint coordinates reject. Mutants that always claim
   ambient parentage or always suppress it fail.
8. **Cleanup isolation.** Killing and waiting the temporary daemon closes its contained tree and
   releases only its endpoint/store claims. The default daemon and default store identity remain
   unchanged. No test reads or deletes the default store.

Targeted review attacks endpoint collision, auto-start leakage, executable authentication,
cross-instance managed authority, effective child injection, and whether the harness can accidentally
touch the owner's default instance.

## Implementation evidence

The shipped-path Windows integration test copies the built binary to a pinned arbitrary directory,
starts two foreground daemons with distinct temporary stores/endpoints, connects through the public
crate, and runs the pinned CLI itself as a scheduled child. The child uses only its injected endpoint
to read its daemon status, proving nested routing through the selected instance. The same test checks
foreign durable IDs, wrong expected image, both singleton collision axes, endpoint/store claim release
after exact process death, and a helper process carrying complete managed coordinates for another
instance. Unit tests cover endpoint validation, incomplete managed environment, endpoint lease
lifetime, CLI parsing, and exact child-environment injection.
