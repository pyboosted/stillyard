#!/usr/bin/env python3
"""Crash only the supervised installed WSL daemon during a live descendant Job.

The service lifetime owner must survive. No generation change is accepted as
cleanup: verify the same retained cgroup and final independent journal seal.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("STILLYARD_JOB_ID"):
        parser.error("crash controller must live outside the tested manager")
    os.umask(0o077)
    root = Path.home() / ".local/share/stillyard"
    cli = str(root / "bin/stillyard")
    directory = args.evidence_directory.resolve() / ("daemon-crash-" + uuid.uuid4().hex)
    directory.mkdir(parents=True)
    def save(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + "\n")
    def query(*command):
        return json.loads(subprocess.check_output([cli, "--endpoint", str(root / "stillyard-v6.sock"), *command],
                                                  stderr=subprocess.DEVNULL, timeout=10))
    def main_pid():
        return int(subprocess.check_output(["systemctl", "--user", "show", "stillyard.service", "--property=MainPID", "--value"], text=True))
    before = query("daemon-status")
    supervisor = main_pid()
    if supervisor <= 0 or supervisor == before["pid"]:
        raise RuntimeError("install the persistent service supervisor before crashing the daemon")
    save("before.json", before)
    marker = directory / "started.json"
    code = ("import pathlib,subprocess,time,json,os; "
            "child=subprocess.Popen(['/usr/bin/python3','-c','import time;time.sleep(120)']); "
            f"pathlib.Path({str(marker)!r}).write_text(json.dumps({{'pid':os.getpid(),'child':child.pid}})); "
            "print('primary and live descendant started',flush=True);time.sleep(120)")
    spec = {"spec_version": 4, "executable": "/usr/bin/python3", "args": ["-c", code],
            "working_directory": str(directory), "resources": {"cargo_slots": 1}, "timeout_seconds": 180,
            "labels": [{"key": "project", "value": "stillyard"}, {"key": "gate", "value": "installed-daemon-crash"}]}
    save("spec.json", spec)
    key = str(uuid.uuid4())
    save("intent.json", {"idempotency_key": key, "supervisor_pid": supervisor})
    with (directory / "client.stdout").open("wb") as out, (directory / "client.stderr").open("wb") as err:
        subprocess.run([cli, "--endpoint", before["endpoint"], "ensure", "--spec", str(directory / "spec.json"),
                        "--idempotency-key", key, "--result-file", str(directory / "receipt.json"),
                        "--deadline-seconds", "30"], stdout=out, stderr=err, check=True)
    job = json.loads((directory / "receipt.json").read_text())["receipt"]["accepted"]["job_id"]
    print(directory, job, flush=True)
    end = time.monotonic() + 30
    while not marker.exists():
        if time.monotonic() >= end:
            raise RuntimeError("live Job did not reach user code; receipt retained")
        time.sleep(.05)
    running = query("status", job)
    save("running.json", running)
    invocation = running["attempts"][0]["invocations"][0]["invocation_id"]
    journal_path = root / "attachment/executor/state.json"
    record = json.loads(journal_path.read_text())["state"]["records"][invocation]
    boundary = Path(record["boundary"]["path"])
    pids = (boundary / "cgroup.procs").read_text().split()
    if len(pids) < 2 or record["seal"] is not None:
        raise RuntimeError("test requires a live root and descendant in the recorded boundary")
    save("executor-before.json", {"record": record, "pids": pids})
    daemon = Path("/proc") / str(before["pid"])
    pidfd = os.pidfd_open(before["pid"])
    if ((daemon / "exe").resolve() != root / "bin/stillyard"
            or int((daemon / "stat").read_text().rsplit(")", 1)[1].split()[19]) != before["process_identity"]["start_ticks"]):
        raise RuntimeError("daemon identity changed before crash")
    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
    os.close(pidfd)
    time.sleep(.1)
    save("immediate-after-crash.json", {"supervisor_pid": main_pid(), "boundary_present": boundary.exists(),
                                       "events": (boundary / "cgroup.events").read_text() if boundary.exists() else None})
    if main_pid() != supervisor or not boundary.exists():
        raise RuntimeError("service lost delegation or discarded an unsealed kernel boundary")
    end = time.monotonic() + 45
    while True:
        try:
            after = query("daemon-status")
            if after["daemon_generation"] != before["daemon_generation"] and after.get("machine_scheduling") and after["machine_scheduling"]["blocker"] is None:
                status = query("status", job)
                if status["state"] == "final" and status["allocations"] and all(a["state"] == "released" for a in status["allocations"]):
                    break
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        if time.monotonic() >= end:
            raise RuntimeError("daemon crash recovery did not reconcile within the acceptance bound")
        time.sleep(.1)
    final = json.loads(journal_path.read_text())["state"]["records"][invocation]
    save("after.json", after)
    save("status.json", status)
    save("executor-after.json", final)
    if (after["store_uuid"] != before["store_uuid"] or main_pid() != supervisor
            or final["boundary"] != record["boundary"] or final["seal"] is None or boundary.exists()
            or status["outcome"] != "interrupted"):
        raise RuntimeError("crash outcome, identity continuity or independent cleanup proof differs")
    save("verdict.json", {"passed": True, "job_id": job, "supervisor_pid": supervisor,
                          "old_daemon_pid": before["pid"], "new_daemon_pid": after["pid"]})
    print(json.dumps({"passed": True, "job_id": job}), flush=True)


if __name__ == "__main__":
    main()
