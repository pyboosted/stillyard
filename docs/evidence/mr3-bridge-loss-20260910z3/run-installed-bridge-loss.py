#!/usr/bin/env python3
"""Actual WSL Cargo/Windows Cargo bridge-loss and delayed-release acceptance.

Run outside Jobs. Cargo is submitted only by the checked-in system launchers.
Only the verified installed daemon's own /init bridge proxy is signalled; the
interop alias is restored in finally. Requires machine cargo_slots=1.
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
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--linux-source", type=Path, required=True)
    p.add_argument("--windows-source", type=Path, required=True)
    p.add_argument("--source-manifest", type=Path, required=True)
    p.add_argument("--evidence-directory", type=Path, required=True)
    p.add_argument("--native-evidence-directory", type=Path, required=True)
    args = p.parse_args()
    if os.environ.get("STILLYARD_JOB_ID"):
        p.error("fault controller must live outside the tested manager")
    os.umask(0o077)
    directory = args.evidence_directory.resolve() / ("bridge-loss-" + uuid.uuid4().hex)
    directory.mkdir(parents=True)
    native = args.native_evidence_directory.resolve() / directory.name
    native.mkdir(parents=True)
    root = Path.home() / ".local/share/stillyard"
    linux_cli = str(root / "bin/stillyard")
    windows_cli = "/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe"
    def save(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + "\n")
    def query(cli, *command):
        return json.loads(subprocess.check_output([cli, *command], timeout=15))
    def winpath(path):
        return subprocess.check_output(["wslpath", "-w", str(path.resolve())], text=True).strip()
    def wait_status(cli, job, predicate, seconds):
        end = time.monotonic() + seconds
        while True:
            status = query(cli, "status", job)
            if predicate(status):
                return status
            if time.monotonic() >= end:
                raise RuntimeError("fault scenario status deadline: " + job)
            time.sleep(.1)
    before = query(linux_cli, "daemon-status")
    windows = query(windows_cli, "daemon-status")
    if windows["capacities"]["cargo_slots"] != 1 or before["machine_scheduling"]["blocker"] is not None:
        raise RuntimeError("bridge loss control requires healthy attached pair and one machine Cargo slot")
    save("pair-before.json", {"windows": windows, "linux": before})
    print(directory, flush=True)
    launcher = Path(__file__).with_name("run-wsl-job.py")
    with (directory / "linux-launcher.log").open("wb") as out:
        subprocess.run(["/usr/bin/python3", str(launcher), "test", "--repository-root", str(args.linux_source),
                        "--source-manifest", str(args.source_manifest), "--evidence-directory", str(directory / "linux-job"),
                        "--no-wait"], stdout=out, stderr=subprocess.STDOUT, check=True)
    receipt = next((directory / "linux-job").rglob("receipt.json"))
    linux_job = json.loads(receipt.read_text())["receipt"]["accepted"]["job_id"]
    running = wait_status(linux_cli, linux_job,
                          lambda s: any(i["state"] == "started" for a in s["attempts"] for i in a["invocations"]), 60)
    save("linux-running.json", running)
    daemon_proc = Path("/proc") / str(before["pid"])
    expected_tail = ["--endpoint", windows["endpoint"], "machine", "bridge"]
    bridges = {}
    for task in (daemon_proc / "task").iterdir():
        for child in (task / "children").read_text().split():
            proc = Path("/proc") / child
            try:
                command = (proc / "cmdline").read_bytes().decode().rstrip("\0").split("\0")
                if command[-4:] == expected_tail and str((proc / "exe").resolve()) == "/init" and command[1:3] == [windows_cli] * 2:
                    bridges[int(child)] = (proc / "stat").read_text().rsplit(")", 1)[1].split()[19]
            except FileNotFoundError:
                pass
    if len(bridges) != 1:
        raise RuntimeError("cannot identify exactly one installed daemon bridge proxy")
    bridge, ticks = next(iter(bridges.items()))
    pidfd = os.pidfd_open(bridge)
    proc = Path("/proc") / str(bridge)
    if (proc.stat().st_uid != os.geteuid()
            or (proc / "stat").read_text().rsplit(")", 1)[1].split()[19] != ticks
            or (proc / "cmdline").read_bytes().decode().rstrip("\0").split("\0")[-4:] != expected_tail):
        raise RuntimeError("bridge identity changed before fault injection")
    alias = root / "interop.sock"
    target = alias.readlink()
    parked = alias.with_name(".interop-fault-" + uuid.uuid4().hex)
    alias.rename(parked)
    try:
        signal.pidfd_send_signal(pidfd, signal.SIGKILL)
        save("fault.json", {"bridge_pid": bridge, "daemon_pid": before["pid"],
                            "alias_target": str(target), "fault_unix_ns": time.time_ns(), "linux_job": linux_job})
        with (native / "launcher.log").open("wb") as out:
            process = subprocess.Popen(["/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
                "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                winpath(args.windows_source / "scripts/run-stillyard-job.ps1"), "test",
                "-RepositoryRoot", winpath(args.windows_source), "-EvidenceDirectory", winpath(native)],
                stdout=out, stderr=subprocess.STDOUT)
        end = time.monotonic() + 20
        while True:
            receipts = list(native.glob("*.receipt.json"))
            if receipts:
                document = json.loads(receipts[0].read_text(encoding="utf-8-sig"))
                if document.get("receipt"):
                    windows_job = document["receipt"]["accepted"]["job_id"]
                    break
            if time.monotonic() >= end:
                raise RuntimeError("Windows acceptance receipt unavailable; retained intent must be recovered")
            time.sleep(.1)
        queued = query(windows_cli, "status", windows_job)
        save("windows-blocked.json", queued)
        if queued["state"] != "pending":
            raise RuntimeError("Windows work started while disconnected Linux work owns the only Cargo token")
        completed = wait_status(linux_cli, linux_job, lambda s: s["state"] == "final", 180)
        save("linux-final-disconnected.json", completed)
        if completed["outcome"] != "succeeded":
            raise RuntimeError("actual Linux Cargo workload failed during bridge loss")
        queued = query(windows_cli, "status", windows_job)
        save("windows-blocked-after-linux-cleanup.json", queued)
        if queued["state"] != "pending":
            raise RuntimeError("Windows work started without the Linux Release acknowledgement")
        journal = json.loads((root / "attachment/executor/state.json").read_text())["state"]["records"]
        ids = [i["invocation_id"] for a in completed["attempts"] for i in a["invocations"]]
        records = {i: journal[i] for i in ids}
        save("executor-records.json", records)
        if not all(r["seal"] and not Path(r["boundary"]["path"]).exists() for r in records.values()):
            raise RuntimeError("Linux completion has no actual executor cleanup seal")
    finally:
        os.close(pidfd)
        if alias.is_symlink() or alias.exists():
            raise RuntimeError("interop binding changed externally; preserved parked alias requires inspection")
        parked.rename(alias)
        save("transport-restored.json", {"unix_ns": time.time_ns(), "alias_target": str(target)})
    final = wait_status(windows_cli, windows_job, lambda s: s["state"] == "final", 240)
    save("windows-final.json", final)
    recovered = wait_status(linux_cli, linux_job, lambda s: bool(s["allocations"]) and all(a["state"] == "released" for a in s["allocations"]), 30)
    save("linux-reconciled.json", recovered)
    if final["outcome"] != "succeeded":
        raise RuntimeError("Windows Cargo workload failed after transport restoration")
    save("verdict.json", {"passed": True, "linux_job": linux_job, "windows_job": windows_job,
                          "native_client_exit": process.wait(timeout=10)})
    print(json.dumps({"passed": True, "linux_job": linux_job, "windows_job": windows_job}), flush=True)


if __name__ == "__main__":
    main()
