#!/usr/bin/env python3
"""Explicit, resumable image-pin rotation after a validated Windows upgrade.

Preserves pairing identities, secret, executor journal and all Job history.
Only the expected old/new image digests may be reconciled. Missing/corrupt
history is never initialized. Run outside managed Jobs; the WSL service is
stopped under an admission barrier and both singleton locks are then held.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import select
import sqlite3
import stat
import subprocess
import time
import uuid


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def commit_rotation(db, anchor_path, intent, new_configuration, publish):
    """Caller holds stopped Store/endpoint locks and an IMMEDIATE transaction.

    A crash after SQL commit leaves the matching durable intent for explicit
    resume. Every other anchor or SQL value remains an unrecoverable mismatch.
    """
    if not db.in_transaction:
        raise RuntimeError("rotation requires the admission transaction")
    current = json.loads(anchor_path.read_bytes())
    old_configuration = new_configuration | {"bridge_sha256": intent["old_image"]}
    if (current["configuration"] not in (old_configuration, new_configuration)
            or fingerprint(current["configuration"]) != current["sha256"]
            or fingerprint(new_configuration) != intent["new_anchor"]
            or fingerprint(old_configuration) != intent["old_anchor"]):
        raise RuntimeError("anchor changed outside the recorded rotation")
    updated = db.execute("update attached_meta set value=? where key='installation_sha256' and value in (?,?)",
                         [intent["new_anchor"], intent["old_anchor"], intent["new_anchor"]]).rowcount
    if updated != 1:
        raise RuntimeError("SQLite anchor changed outside the recorded rotation")
    db.execute("COMMIT")
    publish(anchor_path, {"configuration": new_configuration, "sha256": intent["new_anchor"]})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--expected-prior-sha256", required=True)
    p.add_argument("--new-sha256", required=True)
    p.add_argument("--evidence-directory", type=Path, required=True)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--resume", action="store_true", help="resume only this tool's matching durable intent")
    args = p.parse_args()
    if os.environ.get("STILLYARD_JOB_ID"):
        p.error("image-pin maintenance cannot run inside a managed Job")
    if any(len(h) != 64 or any(c not in "0123456789abcdef" for c in h)
           for h in [args.expected_prior_sha256, args.new_sha256]):
        p.error("explicit old and new SHA-256 are required")
    if args.expected_prior_sha256 == args.new_sha256:
        p.error("image-pin rotation requires distinct reviewed images")
    os.umask(0o077)
    spec = importlib.util.spec_from_file_location("updater", Path(__file__).with_name("upgrade-wsl-daemon.py"))
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    root = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "stillyard"
    metadata = root.lstat()
    if not root.is_absolute() or not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
        raise RuntimeError("installed root must be an absolute owner-only directory")
    attachment = root / "attachment"
    anchor_path = attachment / "anchor.json"
    intent_path = attachment / "bridge-upgrade.json"
    cli = root / "bin/stillyard"
    locks = []
    def lock(path):
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o022:
            os.close(fd)
            raise RuntimeError("unsafe maintenance lock")
        # Older Store initialization creates daemon.lock as 0644 inside the
        # verified 0700 root. Tighten this owned inode; never replace a lock.
        if metadata.st_mode & 0o077:
            os.fchmod(fd, 0o600)
            os.fsync(fd)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(fd)
    def publish(path, value):
        temporary = path.with_name(".rotate-" + uuid.uuid4().hex)
        with temporary.open("x") as out:
            json.dump(value, out, ensure_ascii=False, separators=(",", ":"))
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        helper.sync_directory(path.parent)
    def query(executable, *command):
        return json.loads(subprocess.check_output([str(executable), *command], timeout=15))
    lock(root / "upgrade.lock")
    anchor = json.loads(anchor_path.read_bytes())
    configuration = anchor["configuration"]
    if fingerprint(configuration) != anchor["sha256"]:
        raise RuntimeError("installed anchor checksum differs; no automatic repair")
    prior = args.expected_prior_sha256
    new = args.new_sha256
    if configuration["bridge_sha256"] not in ([prior, new] if args.resume else [prior]):
        raise RuntimeError("anchor image is outside the explicit rotation")
    windows_cli = Path(configuration["bridge_executable"])
    if helper.digest(windows_cli) != new:
        raise RuntimeError("installed Windows image does not match the reviewed replacement")
    windows = query(windows_cli, "--endpoint", configuration["coordinator_endpoint"], "daemon-status")
    machine = windows["machine_scheduling"]
    if machine["blocker"] is not None or machine["domains"]["machine_id"] != configuration["machine_id"]:
        raise RuntimeError("replacement coordinator is fenced or belongs to another machine")
    old_configuration = configuration | {"bridge_sha256": prior}
    new_configuration = configuration | {"bridge_sha256": new}
    intent = {"version": 1, "old_image": prior, "new_image": new,
              "old_anchor": fingerprint(old_configuration), "new_anchor": fingerprint(new_configuration),
              "store_uuid": configuration["pairing"]["manager_store_uuid"],
              "domain_id": configuration["pairing"]["installation"]["domain_id"],
              "journal": configuration["journal"]}
    if intent_path.exists():
        if not args.resume or json.loads(intent_path.read_bytes()) != intent:
            raise RuntimeError("retained rotation intent requires matching explicit --resume")
    elif args.resume:
        raise RuntimeError("there is no durable rotation intent to resume")
    evidence = args.evidence_directory.resolve() / ("bridge-pin-" + uuid.uuid4().hex)
    evidence.mkdir(parents=True)
    publish(evidence / "plan.json", intent | {"apply": args.apply, "resume": args.resume,
                                              "windows": windows})
    print(evidence, flush=True)
    if not args.apply:
        return
    current = subprocess.run([str(cli), "--endpoint", str(root / "stillyard-v6.sock"), "daemon-status"],
                             capture_output=True, timeout=10)
    pidfd = None
    if current.returncode == 0:
        daemon = json.loads(current.stdout)
        if daemon["store_path"] != str(root) or daemon["store_uuid"] != intent["store_uuid"] or daemon["version"] != windows["version"]:
            raise RuntimeError("installed pair has incompatible Store or version identity")
        pidfd = os.pidfd_open(daemon["pid"])
        proc = Path("/proc") / str(daemon["pid"])
        if (proc / "exe").resolve() != cli or int((proc / "stat").read_text().rsplit(")", 1)[1].split()[19]) != daemon["process_identity"]["start_ticks"]:
            raise RuntimeError("daemon process changed")
    elif not args.resume:
        raise RuntimeError("initial rotation requires a queryable installed daemon")
    absent_unit = False
    if pidfd is None:
        unit = subprocess.check_output(["systemctl", "--user", "show", "stillyard.service", "--property=ActiveState", "--value"], text=True).strip()
        if unit not in ("inactive", "failed"):
            raise RuntimeError("unqueryable service is not stopped; run systemctl --user stop stillyard.service, then repeat this explicit --resume")
        # Block automatic start throughout interrupted-maintenance inspection.
        lock(root / "daemon.lock")
        lock(root / "stillyard-v6.sock.lock")
        absent_unit = True
    db = sqlite3.connect((root / "stillyard.sqlite3").as_uri() + "?mode=rw", uri=True, timeout=5, isolation_level=None)
    stopped = False
    consistent = False
    try:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("select value from attached_meta where key='installation_sha256'").fetchone()
        if row is None:
            raise RuntimeError("missing SQLite installation history; no automatic repair")
        pinned = row[0]
        if pinned not in ([intent["old_anchor"], intent["new_anchor"]] if args.resume else [intent["old_anchor"]]):
            raise RuntimeError("SQLite history is outside this rotation")
        row = db.execute("select store_uuid from attached_local_mode where singleton=1").fetchone()
        if row is None:
            raise RuntimeError("missing SQLite pairing history; no automatic repair")
        mode = row[0]
        if mode != intent["store_uuid"]:
            raise RuntimeError("paired SQLite identity differs")
        if db.execute("select count(*) from leases where state='granted'").fetchone()[0] or db.execute("select count(*) from containments where state!='empty'").fetchone()[0]:
            raise RuntimeError("outstanding local work prevents image-pin rotation")
        barrier = helper.executor_barrier(root, allow_absent=absent_unit)
        publish(evidence / "barrier.json", barrier)
        if not intent_path.exists():
            publish(intent_path, intent)
        stopped = True
        subprocess.run(["systemctl", "--user", "stop", "stillyard.service"], check=True, timeout=20)
        if pidfd is not None:
            poll = select.poll(); poll.register(pidfd, select.POLLIN)
            if not poll.poll(5000):
                raise RuntimeError("pinned daemon did not stop")
            os.close(pidfd)
        if not absent_unit:
            lock(root / "daemon.lock")
            lock(root / "stillyard-v6.sock.lock")
        after_stop = helper.executor_barrier(root, allow_absent=True)
        if after_stop["journal_sha256"] != barrier["journal_sha256"]:
            raise RuntimeError("executor journal changed during stop; rotation remains resumable")
        publish(evidence / "barrier-after-stop.json", after_stop)
        # Interrupted transaction/rename is resumable only with this exact
        # public intent; never infer a new pin from whatever executable exists.
        commit_rotation(db, anchor_path, intent, new_configuration, publish)
        consistent = True
        publish(evidence / "replacement.json", intent | {"state": "anchor_and_store_committed"})
        # Archive intent as a durable owner audit, outside the secret anchor.
        os.replace(intent_path, attachment / ("bridge-upgrade-completed-" + uuid.uuid4().hex + ".json"))
        helper.sync_directory(attachment)
    finally:
        if db.in_transaction:
            db.execute("ROLLBACK")
        db.close()
        # Retain the maintenance singleton through service readiness/evidence;
        # release only daemon/endpoint locks before asking systemd to start.
        for fd in reversed(locks[1:]):
            os.close(fd)
        if stopped and consistent:
            subprocess.run(["systemctl", "--user", "start", "stillyard.service"], check=True, timeout=20)
        elif stopped:
            print("Rotation is incomplete; verify service stop and rerun the same explicit digests with --apply --resume.", flush=True)
    until = time.monotonic() + 45
    while True:
        try:
            after = query(cli, "--endpoint", str(root / "stillyard-v6.sock"), "daemon-status")
            if after.get("machine_scheduling") and after["machine_scheduling"]["blocker"] is None:
                break
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        if time.monotonic() >= until:
            raise RuntimeError("rotation committed but attached service did not reconnect")
        time.sleep(.5)
    if (after["store_uuid"] != intent["store_uuid"] or after["store_path"] != str(root)
            or (current.returncode == 0 and after["daemon_generation"] == daemon["daemon_generation"])):
        raise RuntimeError("replacement daemon does not preserve the rotation identity contract")
    publish(evidence / "after.json", after)
    os.close(locks[0])
    print(json.dumps({"store_uuid": after["store_uuid"], "new_bridge_sha256": new,
                      "daemon_generation": after["daemon_generation"]}), flush=True)


if __name__ == "__main__":
    main()
