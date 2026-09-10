# Verified follow-up disposition

Job 01a089b1-9a6a-7711-a600-39e2b74e495d~01a08a19-0be9-7673-9a0e-d9c2ea7a6572
completed actual subscription Opus review with a typed findings verdict.

- Core durable intent / SQL commit / atomic anchor publication / archive ordering
  and original SQL barrier across stop were independently confirmed.
- Root now re-reads and validates the full old/new anchor after stopping and locking
  (commit_rotation). The daemon never rotates the immutable pairing anchor itself,
  but this also detects external changes instead of overwriting them.
- Root guards nullable scheduling status, tolerates startup timeout, verifies final
  Store path/UUID and changed daemon generation.
- Journal checksum and all-sealed/empty barrier are repeated after stop.
- XDG_DATA_HOME is respected, root and lock ownership/type/mode checked, missing
  pairing rows diagnosed. SQLite uses mode=rw and cannot create a missing Store.
- Upgrade singleton remains held through restart/readiness; daemon/socket locks
  are released first. An incomplete operation reports its exact explicit resume
  requirement. An unqueryable active service must be stopped before resume.
- Intent is deliberately not automatically abandoned on a Windows rollback.
  Resume requires the originally selected replacement image; a distinct rollback
  procedure is outside this forward upgrade. Never delete an unknown intent.

The real SQLite split-commit test preserves Job/identity history and rejects
unknown SQL pins or changed pairing; it passed before the additional guards,
and the final helper controls are being recollected. Live rotation still pending.
