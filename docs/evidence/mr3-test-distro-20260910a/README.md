# Disposable test distro: import did not complete

The user prohibited stopping Ubuntu-SSD or the shared WSL VM and asked to test
another distribution. A new disposable `Stillyard-MR3-Test` was selected instead
of treating the existing stopped Ubuntu-22.04 as disposable. The latter was not
started or modified. No terminate, shutdown, host-service restart or host reboot
was executed.

Ubuntu's official 26.04.1 WSL image was downloaded from the URL in image.json.
SHA256SUMS.gpg verified against the host's Ubuntu archive keyring; the downloaded
image matched SHA-256
`48d56724b5c8e60f24893e83e73bbb58c60b3ca22fba3da977075420acd54104`.
The 399 MiB image is retained outside Git at
`C:\Development\stillyard-mr3-test-distro-20260910a` for recovery/reuse.

Native default import Job
W`01a08bab-3dfa-7473-a15d-cc2b68bac749` failed after its 180-second subprocess
deadline. WSL created registration `{1cf1e672-7ffd-4a75-a2b5-c6dc6b127450}`
with State=3 and a 12 MiB ext4.vhdx at the selected new directory. No successful
import ownership receipt was produced. The exact default distribution remained
Ubuntu-SSD. The initial client syntax error lacked an explicit endpoint and was
corrected using the same intent before submission; it did not run an import.

Afterward, an ordinary `wsl --list --verbose` request also stalled. Its two
native CLI processes were identified from PID/creation/command records; cleanup
stopped the still-live owned child (the wrapper also exited). Foreign list
clients and all working-distro keepalives were untouched. Native process command
lines are hashed in the canonical summary; the full local observation is retained
outside Git to avoid publishing unrelated command contents.

One native default cleanup Job
W`01a08bb0-b833-7483-a3a5-fc52cb0458ba` attempted only
`wsl --unregister Stillyard-MR3-Test` after verifying its new GUID, exact path,
non-default identity, unchanged partial disk size and absent successful-import
receipt. It also timed out, after 30 seconds. Its CLI process ended, but this is
not proof that WSL canceled its service-side operation. Do not delete the VHD or
edit its registration behind WSL. No retry/replacement distro was created.
The exact cause of the WSL management stall is not established.

## Working pair continuity

protected-before.json and protected-after.json show unchanged Linux Store,
daemon generation, process identity and kernel boot. The attached scheduler
continued reporting healthy authority. Actual ordinary canaries with
`cargo_slots: 1` succeeded afterward:

- Windows W`01a08bb2-0b59-7f50-9edb-17c568f5c057`.
- Ubuntu-SSD L`01a08bb2-ee1c-7c01-9a0b-ea0b3605b752`, with Windows-issued Grant
  `01a08bb2-ee6a-79c2-861c-f294c14b1462`.

The initial Linux canary client rejected a DrvFS durable receipt path. The retry
used the same intent on local ext4 and succeeded. Both observations are retained.
These short real commands prove ordinary scheduling still works; they are not
replacement build gates, concurrency acceptance or test-distro lifecycle PASS.
jobs.json links all four actual Jobs and their final canonical logs/status.

## Prepared inputs and recovery boundary

install-fixture.py, pair-fixture.py and lifecycle/ are **unexecuted drafts**.
They select the exact accepted z11f release (no new Cargo build/cache), stage a
fresh stopped Store in the test distro, and register only its executor beneath
the existing WSL VM domain. The fixture budget is 1 GiB/one Cargo slot. No pairing
registration or private anchor for the test distro has been created.
The draft controller supports only preflight and targeted test-distro terminate,
pins the owned registry identity, and compares protected Ubuntu-SSD identities.
It needs native validation/review and a completed import before use.

Next recovery must inspect the existing import registration and pending cleanup
outcome before any retry. Preserve all history. Do not run the historical j
shutdown/Ubuntu-SSD terminate specs to unblock this fixture. Whole-VM, suspend,
logout/reboot coverage is deferred under the user's explicit host restriction.

Command scope references:
[Microsoft WSL commands](https://learn.microsoft.com/en-us/windows/wsl/basic-commands#terminate),
[Ubuntu official WSL image directory](https://releases.ubuntu.com/26.04/).
