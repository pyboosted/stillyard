#!/usr/bin/env python3
"""Upgrade the installed default WSL binary under a SQLite admission barrier.

Prepare is read-only except evidence. Apply preserves the Store, pairing anchor,
executor journal and queued Jobs. Sealed final work can be explicitly retained
across a binary repair; only the daemon protocol may release its resources.
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

from wsl_maintenance import sql_barrier


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def bootstrap_build(build, holds, job_id, candidate):
    """Bind a recovery image to durable default-coordinator work and cleanup."""
    matching = [h for h in holds if h.get('bootstrap')
                and h['bootstrap']['parent']['job_id'] == job_id]
    if len(matching) != 1:
        raise RuntimeError('recovery build requires exactly one actual bootstrap Hold')
    hold = matching[0]
    bootstrap, proof = hold['bootstrap'], hold['cleanup_proof']
    work = bootstrap['work']
    spec = build['spec']
    if (build['state'] != 'final' or build['outcome'] != 'succeeded'
            or {'key': 'gate', 'value': 'wsl-bootstrap-build-release'} not in spec['labels']
            or spec['args'][:3] != ['bootstrap', 'run', '--spec']
            or spec['resources']['cargo_slots'] != 1
            or work['args'] != ['build', '--locked', '--release']
            or Path(work['executable']) != Path.home() / '.cargo/bin/cargo'
            or work['distribution'] != os.environ.get('WSL_DISTRO_NAME')
            or work['user'] != Path.home().name
            or candidate != (Path(work['environment']['CARGO_TARGET_DIR']) / 'release/stillyard').resolve()
            or hold['released'] is not True or proof is None
            or proof['phase'] != 'sealed_empty' or proof['root_exit_code'] != 0
            or proof['termination'] != 'exited'
            or proof['operation_id'] != work['operation_id']
            or proof['request_sha256'] != bootstrap['request_sha256']
            or bootstrap['parent']['invocation_id'].split('~')[-1] != work['operation_id']):
        raise RuntimeError('recovery build does not match durable bootstrap work and seal')
    return hold


def executor_barrier(root, *, allow_absent=False, db=None, store_uuid=None,
                     allow_sealed_release_pending=False):
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
    if (not executors.is_relative_to("/sys/fs/cgroup") or executors.is_symlink()
            or '..' in executors.parts or executors.resolve() != executors):
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
    sql = sql_barrier(db, store_uuid, records,
                      allow_sealed_release_pending=allow_sealed_release_pending) if db is not None else None
    return {"sql_clearance": sql, "journal_sha256": hashlib.sha256(raw).hexdigest(), "records": len(records),
            "unsealed": 0, "executors": str(executors), "events": events, "children": children}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--candidate-sha256", required=True)
    p.add_argument("--build-job-id", required=True)
    p.add_argument("--evidence-directory", type=Path, required=True)
    p.add_argument("--allow-sealed-release-pending", action="store_true",
                   help="Retain fully sealed final work Leases across a binary repair")
    p.add_argument("--bootstrap-build", action="store_true",
                   help="Verify recovery build against the default Windows bootstrap Hold")
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
    build_cli = (Path('/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe')
                 if args.bootstrap_build else installed)
    build = json.loads(subprocess.check_output([str(build_cli), 'status', args.build_job_id], timeout=15))
    if build["state"] != "final" or build["outcome"] != "succeeded":
        p.error("release build Job has not succeeded")
    provenance = None
    if args.bootstrap_build:
        holds = json.loads(subprocess.check_output([str(build_cli), 'authority', 'status'], timeout=15))['holds']
        provenance = bootstrap_build(build, holds, args.build_job_id, candidate)
    else:
        target = Path(build["spec"]["environment"]["set"]["CARGO_TARGET_DIR"]) / "release/stillyard"
        if (candidate != target.resolve() or build["spec"]["args"] != ["build", "--locked", "--release"]
                or {"key": "gate", "value": "wsl-build-release"} not in build["spec"]["labels"]):
            p.error("candidate does not match the selected installed-system release Job")
    if candidate == installed or digest(candidate) != args.candidate_sha256:
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
                       "prior_sha256": digest(installed), "apply": args.apply,
                       "allow_sealed_release_pending": args.allow_sealed_release_pending,
                       "bootstrap_build": provenance})
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
        jobs = db.execute("select count(*) from jobs").fetchone()[0]
        independent = executor_barrier(root, db=db, store_uuid=before["store_uuid"],
                                      allow_sealed_release_pending=args.allow_sealed_release_pending)
        save("barrier.json", {"job_count": jobs, "executor": independent})
        stop_requested = True
        subprocess.run(["systemctl", "--user", "stop", "stillyard.service"], check=True, timeout=20)
        poll = select.poll()
        poll.register(pidfd, select.POLLIN)
        if not poll.poll(5000):
            raise RuntimeError("pinned old daemon did not exit")
        stopped = executor_barrier(root, allow_absent=True, db=db, store_uuid=before['store_uuid'],
                                   allow_sealed_release_pending=args.allow_sealed_release_pending)
        if stopped['journal_sha256'] != independent['journal_sha256']:
            raise RuntimeError('executor journal changed during the verified empty stop')
        save('stopped-barrier.json', stopped)
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
    retained = independent['sql_clearance']['sealed_release_pending']
    if retained:
        deadline = time.monotonic() + 45
        with sqlite3.connect((root / 'stillyard.sqlite3').as_uri() + '?mode=ro', uri=True) as db:
            while True:
                states = {item['lease_id']: db.execute("""select l.state,p.released,
                    json_extract(g.grant_json,'$.state'),g.seal_json is not null
                    from leases l join attached_local_plans p on p.lease_id=l.id
                    join attached_grants g using(allocation_key) where l.id=?""",
                    (item['lease_id'],)).fetchone() for item in retained}
                if all(state == ('released', 1, 'released', 1) for state in states.values()):
                    save('retained-released.json', states)
                    break
                if time.monotonic() >= deadline:
                    save('retained-release-timeout.json', states)
                    raise RuntimeError('replacement connected but original retained releases have not reconciled')
                time.sleep(.2)
    os.close(lock)
    print(json.dumps({"pid": after["pid"], "generation": after["daemon_generation"],
                      "store_uuid": after["store_uuid"], "sha256": digest(installed)}), flush=True)


if __name__ == "__main__":
    main()
