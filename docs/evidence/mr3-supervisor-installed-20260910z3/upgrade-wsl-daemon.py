#!/usr/bin/env python3
"""Upgrade the installed default WSL binary under a SQLite admission barrier.

Prepare is read-only except evidence. Apply preserves the Store, pairing anchor,
executor journal and queued Jobs. It refuses outstanding Leases or containment.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import sqlite3
import stat
import subprocess
import time
import uuid


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def executor_barrier(root, *, allow_absent=False):
    """Independent retained obligations and recursive kernel emptiness, not SQL alone."""
    path = root / "attachment/executor/state.json"
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
        raise RuntimeError("unsafe executor journal")
    raw = path.read_bytes()
    envelope = json.loads(raw)
    encoded = json.dumps(envelope["state"], ensure_ascii=False, separators=(",", ":")).encode()
    if hashlib.sha256(encoded).hexdigest() != envelope["sha256"]:
        raise RuntimeError("executor journal checksum differs")
    records = envelope["state"]["records"]
    unsealed = [key for key, record in records.items() if record["seal"] is None]
    # Read only this public path from the private anchor; never copy its credentials.
    anchor = json.loads((root / "attachment/anchor.json").read_bytes())
    executors = Path(anchor["configuration"]["executor_cgroup"])
    if not executors.is_relative_to("/sys/fs/cgroup") or executors.is_symlink():
        raise RuntimeError("invalid executor cgroup path")
    if allow_absent and not executors.exists():
        # Used only by explicit interrupted-maintenance recovery with a stopped
        # unit. Every retained executor record must already have its own seal.
        events, children = {"populated": "0", "maintenance_absent": "true"}, []
    else:
        events = dict(line.split() for line in (executors / "cgroup.events").read_text().splitlines())
        children = [p.name for p in executors.iterdir() if p.is_dir()]
    if unsealed or events.get("populated") != "0" or children:
        raise RuntimeError("executor obligations or kernel boundaries prevent upgrade; daemon left running")
    return {"journal_sha256": hashlib.sha256(raw).hexdigest(), "records": len(records),
            "unsealed": 0, "executors": str(executors), "events": events, "children": children}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--candidate-sha256", required=True)
    p.add_argument("--build-job-id", required=True)
    p.add_argument("--evidence-directory", type=Path, required=True)
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    if os.environ.get("STILLYARD_JOB_ID"):
        p.error("upgrade must run outside a Job scheduled by the daemon being replaced")
    os.umask(0o077)
    root = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "stillyard"
    installed = root / "bin/stillyard"
    candidate = args.candidate.resolve()
    def query(*command):
        return json.loads(subprocess.check_output([str(installed), *command], timeout=15))
    before = query("daemon-status")
    if before["store_path"] != str(root) or before["machine_scheduling"]["mode"] != "attached":
        p.error("upgrade requires the installed default attached manager")
    build = query("status", args.build_job_id)
    if build["state"] != "final" or build["outcome"] != "succeeded":
        p.error("release build Job has not succeeded")
    target = Path(build["spec"]["environment"]["set"]["CARGO_TARGET_DIR"]) / "release/stillyard"
    if (candidate != target.resolve() or build["spec"]["args"] != ["build", "--locked", "--release"]
            or {"key": "gate", "value": "wsl-build-release"} not in build["spec"]["labels"]
            or candidate == installed or digest(candidate) != args.candidate_sha256):
        p.error("candidate does not match the selected installed-system release Job")
    version = subprocess.check_output([str(candidate), "--version"], text=True, timeout=5).strip()
    if version != "stillyard " + before["version"]:
        p.error("this updater supports same-version fixes; version upgrades need a reviewed migration")
    evidence = args.evidence_directory.resolve() / ("upgrade-" + uuid.uuid4().hex)
    evidence.mkdir(parents=True)
    def save(name, value):
        with (evidence / name).open("x") as stream:
            json.dump(value, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        sync_directory(evidence)
    save("plan.json", {"before": before, "candidate": str(candidate),
                       "candidate_sha256": args.candidate_sha256, "build_job_id": args.build_job_id,
                       "prior_sha256": digest(installed), "apply": args.apply})
    print(evidence, flush=True)
    if not args.apply:
        return
    lock = os.open(root / "upgrade.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    pidfd = os.pidfd_open(before["pid"])
    process = Path("/proc") / str(before["pid"])
    ticks = int((process / "stat").read_text().rsplit(")", 1)[1].split()[19])
    if ticks != before["process_identity"]["start_ticks"] or (process / "exe").resolve() != installed:
        raise RuntimeError("daemon process identity changed")
    staged = installed.with_name(".next-" + uuid.uuid4().hex)
    with candidate.open("rb") as source, staged.open("xb") as output:
        shutil.copyfileobj(source, output)
        output.flush()
        os.fsync(output.fileno())
    staged.chmod(0o700)
    if digest(staged) != args.candidate_sha256:
        raise RuntimeError("staged binary checksum differs")
    sync_directory(installed.parent)
    db = sqlite3.connect((root / "stillyard.sqlite3").as_uri() + "?mode=rw", uri=True, timeout=5, isolation_level=None)
    stop_requested = False
    try:
        db.execute("BEGIN IMMEDIATE")
        leases = db.execute("select count(*) from leases where state='granted'").fetchone()[0]
        containment = db.execute("select count(*) from containments where state!='empty'").fetchone()[0]
        if leases or containment:
            raise RuntimeError("active or uncertain work prevents upgrade; daemon left running")
        jobs = db.execute("select count(*) from jobs").fetchone()[0]
        independent = executor_barrier(root)
        save("barrier.json", {"job_count": jobs, "granted_leases": leases,
                              "blocking_containment": containment, "executor": independent})
        stop_requested = True
        subprocess.run(["systemctl", "--user", "stop", "stillyard.service"], check=True, timeout=20)
        poll = select.poll()
        poll.register(pidfd, select.POLLIN)
        if not poll.poll(5000):
            raise RuntimeError("pinned old daemon did not exit")
        backup = installed.with_name("stillyard.previous-" + uuid.uuid4().hex)
        # Preserve the old inode without ever removing the canonical path.
        os.link(installed, backup, follow_symlinks=False)
        sync_directory(installed.parent)
        os.replace(staged, installed)
        sync_directory(installed.parent)
        save("replacement.json", {"backup": str(backup), "installed_sha256": digest(installed)})
    finally:
        if db.in_transaction:
            db.execute("ROLLBACK")
        db.close()
        os.close(pidfd)
        # Reopen service even if replacement or evidence publication failed.
        # The atomic path contains either the complete old or complete new image.
        try:
            if stop_requested:
                start = subprocess.run(["systemctl", "--user", "start", "stillyard.service"],
                                       capture_output=True, timeout=20)
                save("service-start.json", {"exit_code": start.returncode,
                                            "stderr": start.stderr.decode()})
        finally:
            if staged.exists():
                staged.unlink()
    deadline = time.monotonic() + 45
    while True:
        try:
            after = query("daemon-status")
            if after["machine_scheduling"] and after["machine_scheduling"]["blocker"] is None:
                break
        except subprocess.CalledProcessError:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError("replacement did not reconnect; retained history and backup require diagnosis")
        time.sleep(.5)
    if after["store_uuid"] != before["store_uuid"] or after["daemon_generation"] == before["daemon_generation"]:
        raise RuntimeError("replacement Store identity or generation differs from the upgrade contract")
    save("after.json", after)
    os.close(lock)
    print(json.dumps({"pid": after["pid"], "generation": after["daemon_generation"],
                      "store_uuid": after["store_uuid"], "sha256": digest(installed)}), flush=True)


if __name__ == "__main__":
    main()
