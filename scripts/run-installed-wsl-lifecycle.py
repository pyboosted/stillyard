#!/usr/bin/env python3
"""Exercise real installed WSL primary/postcondition/probe cleanup, retaining evidence.

No Cargo and no isolated daemon. Each scenario is a default attached Job with a
real Windows coordinator Grant. Read-only journal excerpts omit the pairing anchor.
"""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import uuid


def write(path, document):
    path.write_text(json.dumps(document, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["postcondition", "probe", "timeout", "cancel"])
    parser.add_argument("--evidence-directory", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    root = Path.home() / ".local/share/stillyard"
    cli = str(root / "bin/stillyard")
    directory = args.evidence_directory.resolve() / (args.scenario + "-" + uuid.uuid4().hex)
    directory.mkdir(parents=True)
    def query(*arguments):
        return json.loads(subprocess.check_output([cli, *arguments], timeout=15))
    daemon = query("daemon-status")
    assert daemon["store_path"] == str(root)
    assert daemon["machine_scheduling"]["mode"] == "attached"
    assert daemon["machine_scheduling"]["blocker"] is None
    write(directory / "daemon-before.json", daemon)
    marker = directory / "primary-marker"
    code = ("import pathlib,subprocess; "
            "subprocess.Popen(['/usr/bin/python3','-c','import time;time.sleep(120)']); "
            f"pathlib.Path({str(marker)!r}).write_text('primary'); "
            "print('primary with live descendant',flush=True)")
    spec = {"spec_version": 4, "executable": "/usr/bin/python3", "args": ["-c", code],
            "working_directory": str(directory), "timeout_seconds": 30,
            "resources": {"cargo_slots": 1, "impacts": ["cpu_heavy"]},
            "labels": [{"key": "project", "value": "stillyard"},
                       {"key": "gate", "value": "installed-wsl-" + args.scenario}]}
    if args.scenario == "postcondition":
        spec["postconditions"] = [{"executable": "/usr/bin/python3", "args": ["-c",
            f"import pathlib,time; assert pathlib.Path({str(marker)!r}).read_text()=='primary'; "
            "print('validated real primary output',flush=True);time.sleep(1)"]},
            {"executable": "/usr/bin/python3", "args": ["-c", "print('second postcondition')"]}]
    elif args.scenario == "probe":
        spec["conditions"] = [{"predicate": {"kind": "probe", "probe": {
            "executable": "/usr/bin/python3", "args": ["-c", "print('independent probe')"],
            "working_directory": str(directory), "resources": {"cargo_slots": 1},
            "timeout_seconds": 10, "interval_seconds": 1, "accepted_exit_codes": [0]}},
            "deadline": {"kind": "relative", "seconds": 60}}]
    else:
        spec["args"][1] += "; import time; time.sleep(120)"
        spec["timeout_seconds"] = 2 if args.scenario == "timeout" else 60
    write(directory / "spec.json", spec)
    key = str(uuid.uuid4())
    write(directory / "intent.json", {"idempotency_key": key, "endpoint": daemon["endpoint"]})
    command = [cli, "--endpoint", daemon["endpoint"], "ensure", "--spec", str(directory / "spec.json"),
               "--idempotency-key", key, "--result-file", str(directory / "receipt.json"),
               "--wait", "--deadline-seconds", "90"]
    with (directory / "client.stdout").open("wb") as out, (directory / "client.stderr").open("wb") as err:
        process = subprocess.Popen(command, stdout=out, stderr=err)
        print(directory, flush=True)
        if args.scenario == "cancel":
            deadline = time.monotonic() + 40
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(.1)
            assert marker.exists(), "primary never started; receipt retained for recovery"
            receipt = json.loads((directory / "receipt.json").read_text())
            job = receipt["receipt"]["accepted"]["job_id"]
            write(directory / "cancel.json", query("cancel", job))
        process.wait(timeout=100)
    receipt = json.loads((directory / "receipt.json").read_text())
    job = receipt["receipt"]["accepted"]["job_id"]
    entity = job.split("~", 1)[1]
    status = query("status", job)
    write(directory / "status.json", status)
    for stream in ("stdout", "stderr"):
        write(directory / (stream + ".json"), query("logs", job, "--stream", stream, "--json"))
    with sqlite3.connect(f"file:{root / 'stillyard.sqlite3'}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        invocations = [dict(r) for r in db.execute(
            "select i.id,i.role,i.started_ms,i.finished_ms from invocations i "
            "join attempts a on a.id=i.attempt_id where a.job_id=? order by i.rowid", [entity])]
        deadline = time.monotonic() + 15
        while True:
            plans = [dict(r) for r in db.execute(
                "select lease_id,allocation_key,armed,committed,release_pending,released "
                "from attached_local_plans where job_id=?", [entity])]
            if plans and all(p["released"] == 1 for p in plans):
                break
            if time.monotonic() > deadline:
                break
            time.sleep(.1)
        for i in invocations:
            i["id"] = daemon["store_uuid"] + "~" + i["id"]
        cleanup = [dict(r) for i in invocations for r in db.execute(
            "select * from attached_local_cleanup where invocation_id=?", [i["id"]])]
    records = json.loads((root / "attachment/executor/state.json").read_text())["state"]["records"]
    records = {i["id"]: records[i["id"]] for i in invocations if i["id"] in records}
    write(directory / "durable-evidence.json", {"invocations": invocations, "plans": plans,
                                               "cleanup": cleanup, "executor_records": records})
    print(json.dumps({"job_id": job, "outcome": status["outcome"], "directory": str(directory)}), flush=True)
    expected = {"timeout": "timed_out", "cancel": "canceled"}.get(args.scenario, "succeeded")
    assert status["outcome"] == expected, status.get("reason_code")
    count = {"postcondition": 3, "probe": 2}.get(args.scenario, 1)
    assert len(invocations) == len(cleanup) == len(records) == count
    assert all(r["seal"] and not Path(r["boundary"]["path"]).exists() for r in records.values())
    assert all(p["released"] == 1 for p in plans)
    grants = {r["release_intent"]["grant_id"] for r in records.values()}
    assert len(grants) == (2 if args.scenario == "probe" else 1)
    write(directory / "verdict.json", {"passed": True, "job_id": job, "scenario": args.scenario,
                                       "grant_ids": sorted(grants)})


if __name__ == "__main__":
    main()
