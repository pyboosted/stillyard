# Native Linux core checkpoint, 2026-09-12

These are incremental development Jobs on the installed default WSL manager,
coordinated by the unchanged Windows daemon. They are **not** native-host or
installed-candidate acceptance. Canonical status, logs, receipt, input spec and
available installed-pair identity snapshots are retained per Job.

The latest test ran 274 library tests, 35 CLI/TUI tests, two Linux daemon tests
and two public API tests successfully. The 19 library ignored entries require
explicit kernel/fault fixtures; no execution claim is made for them. Six new
unit tests cover native installation, journal compatibility/permission binding
and SQL rollback inventory. Service controls passed four one-shot installation
tests and both existing WSL supervisor signal/restart tests.

Initial check failed on the new mutable closure and missing Unknown identity
match; both were repaired. Initial Clippy failed only on the test mutation-list
type; the final check and Clippy passed. All grants below are released.

Earlier incremental source snapshots were not retained. The checkpoint source
manifest describes the final source, not an invented exact-source mapping for
those earlier Jobs. The last test precedes the test-only Clippy type simplification;
final Clippy compiles that simplification. The new native launcher is staged and
still requires a native installed-host run. The service installer itself has not
been executed on a native host.

| Job | Result | Windows Grant |
|---|---|---|
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a09658-a439-7c00-97f0-c67e8fd399a9` | succeeded — `wsl-fmt-write-74284463c7104d20b701c4fe100421ba` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a09658-a45c-7831-8d88-5a69e896c10c` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a09664-4e77-7d12-9132-735a348da014` | succeeded — `wsl-check-89b7d3c9020047b296748d12b3ecc24b` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a09664-4e95-7f50-9ac0-167b2ee31bc7` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0965f-afa8-7e52-93c7-72cd926fa779` | failed — `wsl-clippy-e604667cd06b4dcc901537e8d2b8b716` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0965f-afc7-72f3-af77-1c1018097287` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a09664-4e92-7bd0-8943-dac3a89062bb` | succeeded — `wsl-clippy-d6230fc8eef3444cb31655948572a118` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a09664-6cc8-7690-a957-57a11b7eefdc` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0965f-8ce0-7481-b40b-eefffd49a3b3` | succeeded — `wsl-fmt-write-b02e10f70a2646dfbb36870d5a92372d` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0965f-8d01-79b2-abd8-ed054d072b69` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0965c-f58c-73b2-9e60-cbd335f934db` | succeeded — `wsl-test-be062a0e7def404b83bc0b8f6f527562` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0965c-f5ac-7ea1-8215-347c25016197` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a09659-0085-78e0-8731-5f3d5d6be18f` | failed — `wsl-check-9adc36798aa7490cae12ee40608623ff` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a09659-00c2-7f71-a506-89592d5918ce` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a09663-e508-7c71-8927-eb892dfef8ac` | succeeded — `wsl-fmt-write-0db05fc1a82e4e2ca3fc14a0baffd88c` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a09663-e526-7562-83b5-25ad6e29cf37` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0965f-b448-7822-9c3c-253bc61dbbaa` | succeeded — `wsl-test-5c3ab3fff7554bc7bb3fbc09b2272957` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0965f-d173-7322-9fef-68abc69b751f` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a0965c-72b5-7291-94f6-6d59306be95a` | succeeded — `wsl-fmt-write-06e5319ce98e4960a0dbf7bf910a2554` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a0965c-72d4-7863-b34d-437a567d16c8` (released) |
| `01a089b1-9a6a-7711-a600-39e2b74e495d~01a09664-528c-7261-b43d-e6048766311d` | succeeded — `native-service-controls` | `01a05f1f-858c-7880-8c15-d55875da9e6b~01a09664-8d2d-7ba3-ac9c-663bbfd22280` (released) |

Independent Codex review found and verified two issues before this checkpoint:
SQL rollback could orphan an executor obligation without a retained debit, and
native pre-release rejection could lose timeout/deferral classification. The
implementation now validates every unsealed executor against its granted SQL
Lease and any published permission against the exact allocation/authority
inventory; it preserves stop/deferral reasons. No Opus verdict is claimed because
subscription authentication was expired. Native crash and consumer acceptance
remain required despite these unit tests.

One active `target/scheduled-linux` cache is retained for continuing MR-4 work.
No installed executable, working distro, service or WSL VM was stopped/replaced.
