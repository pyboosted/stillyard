# Cross-OS reset review disposition

Actual Opus Job `01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a57-477d-7e40-a1da-0a9d29cc7019` succeeded. No product safety finding; test improvements below are root-only after snapshot z4.

Computed observations and actual error strings now accompany reset/anchor rejection evidence. Reset errors are checked against the expected fencing or missing/corrupt-anchor reason. UID is measured by the Linux subject. Python path literals use JSON escaping. Native cleanup propagates the actual seal's possibly_released value. The live subject also rejects missing/corrupt executor anchors.

Drop uses best-effort abort publication and a finite 260-second join bound inside the outer 600-second system Job; protected bootstrap remains the cleanup owner. Every guest mailbox phase checks abort. Bootstrap result/error is retained for a premature subject exit.

Coverage boundaries remain explicit: this combines real native coordinator RPC/reset with production Linux Store/PreparedLaunch/Journal and real processes; it does not run the guest Driver/full guest daemon, production Ticket transit/freshness or configured runtime cgroup path. Separate installed scenarios cover those components. Test mailbox assumes the reference /mnt/<drive> mapping. Fixture secrets grant access only to the isolated test coordinator, inherit the owner's Windows temporary-directory ACL, and are excluded from evidence export.
