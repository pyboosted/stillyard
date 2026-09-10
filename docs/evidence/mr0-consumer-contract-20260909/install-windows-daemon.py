#!/usr/bin/env python3
"""Install a validated native candidate without dropping another Job's work.

Run with native Windows Python outside the daemon being upgraded. Prepare is the
default; --apply takes a SQLite admission barrier, requires no outstanding local
Lease/containment, verifies the exact daemon process, and preserves the database.
The new runtime must start closed or retain its existing authority obligations.
"""

import argparse
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import time
import uuid


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity_path(path):
    return os.path.normcase(str(Path(path).resolve())).removeprefix("\\\\?\\")


def release_key(version):
    match = re.fullmatch(r"stillyard (\d+)\.(\d+)\.(\d+)(?:-alpha\.(\d+))?", version)
    if not match:
        raise ValueError("installer requires a recognized release version")
    major, minor, patch, alpha = match.groups()
    return (int(major), int(minor), int(patch), alpha is None, int(alpha or 0))


def check_candidate_version(version, prior_version, help_text):
    candidate = release_key(version)
    prior = release_key("stillyard " + prior_version)
    if candidate < release_key("stillyard 0.1.0-alpha.15") or candidate < prior:
        raise ValueError("downgrade or candidate without the durable authority guard is forbidden")
    if not all(re.search(r"(?m)^\s+" + name + r"\s", help_text) for name in ("authority", "bootstrap")):
        raise ValueError("candidate does not expose the required authority/bootstrap operations")


def save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-sha256", required=True)
    parser.add_argument("--build-job-id", required=True)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    parser.add_argument("--installed", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--wait-seconds", type=float, default=60)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--initialize-new-authority", action="store_true")
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("use native Windows Python; POSIX locks cannot guard the Windows SQLite store")
    if bool(args.installed) != bool(args.endpoint):
        parser.error("an isolated installed path and endpoint must be supplied together")
    installed = args.installed or Path(os.environ["LOCALAPPDATA"]) / "stillyard/Stillyard/bin/stillyard.exe"
    installed, candidate = installed.resolve(), args.candidate.resolve()
    if installed == candidate or digest(candidate) != args.candidate_sha256:
        parser.error("candidate must be a separate file with the reviewed SHA-256")
    evidence = args.evidence_directory.resolve() / ("install-" + uuid.uuid4().hex)
    evidence.mkdir(parents=True)
    prefix = [str(installed)] + (["--endpoint", args.endpoint] if args.endpoint else [])

    def cli(*arguments):
        result = subprocess.run([*prefix, *arguments], capture_output=True, timeout=15)
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace"))
        return json.loads(result.stdout)

    context = cli("context", "--json", "--deadline-seconds", "5")
    if context["parent"] is not None:
        raise RuntimeError("installer cannot stop the daemon that schedules its own Job")
    before = cli("daemon-status", "--deadline-seconds", "5")
    store = Path(before["store_path"])
    version = subprocess.check_output([str(candidate), "--version"], timeout=5).decode().strip()
    help_text = subprocess.check_output([str(candidate), "--help"], timeout=5).decode()
    check_candidate_version(version, before["version"], help_text)
    plan = {"build_job_id": args.build_job_id, "candidate": str(candidate),
            "candidate_sha256": args.candidate_sha256, "candidate_version": version,
            "installed": str(installed), "prior_sha256": digest(installed), "before": before,
            "initialize_new_authority": args.initialize_new_authority}
    save(evidence / "plan.json", plan)
    print(evidence / "plan.json", flush=True)
    if not args.apply:
        return

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel.QueryFullProcessImageNameW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD)]
    kernel.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    process = kernel.OpenProcess(0x1000 | 0x0001 | 0x100000, False, before["pid"])
    if not process:
        raise ctypes.WinError(ctypes.get_last_error())
    database = None
    staged = None
    try:
        times = [wintypes.FILETIME() for _ in range(4)]
        if not kernel.GetProcessTimes(process, *(ctypes.byref(value) for value in times)):
            raise ctypes.WinError(ctypes.get_last_error())
        created = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
        image = ctypes.create_unicode_buffer(32768)
        length = wintypes.DWORD(len(image))
        if not kernel.QueryFullProcessImageNameW(process, 0, image, ctypes.byref(length)):
            raise ctypes.WinError(ctypes.get_last_error())
        if (created != before["process_identity"]["creation_filetime_100ns"]
                or identity_path(image.value) != identity_path(installed)):
            raise RuntimeError("daemon process identity changed; no process was stopped")
        staged = installed.with_name("stillyard.next-" + uuid.uuid4().hex + ".exe")
        with candidate.open("rb") as source, staged.open("xb") as destination:
            shutil.copyfileobj(source, destination)
            destination.flush()
            os.fsync(destination.fileno())
        if digest(staged) != args.candidate_sha256:
            raise RuntimeError("candidate changed while staging")
        database = sqlite3.connect(store / "stillyard.sqlite3", timeout=1, isolation_level=None)
        limit = time.monotonic() + args.wait_seconds
        while True:
            try:
                database.execute("BEGIN IMMEDIATE")
                leases = database.execute("SELECT COUNT(*) FROM leases WHERE state = 'granted'").fetchone()[0]
                containments = database.execute("SELECT COUNT(*) FROM containments WHERE state NOT IN ('empty', 'cleared')").fetchone()[0]
                cleared = database.execute("SELECT resolution, resolution_audit_json FROM containments WHERE state = 'cleared'").fetchall()
                for resolution, audit_json in cleared:
                    try:
                        audit = json.loads(audit_json)
                        valid = resolution in ("proven_empty", "reboot", "forced_risk_acceptance") and audit["resolution"] == resolution
                    except (ValueError, TypeError, KeyError):
                        valid = False
                    if not valid:
                        containments += 1
                if leases == 0 and containments == 0:
                    break
                database.execute("ROLLBACK")
            except sqlite3.OperationalError as error:
                if database.in_transaction:
                    database.execute("ROLLBACK")
                if "locked" not in str(error):
                    raise
            if time.monotonic() >= limit:
                raise RuntimeError("active or uncertain work prevents installation; daemon and queue left running")
            time.sleep(0.1)
        # The write transaction now prevents a queued submission from crossing
        # admission between the empty check and process stop. No Jobs are reset.
        if digest(installed) != plan["prior_sha256"] or kernel.WaitForSingleObject(process, 0) != 258:
            raise RuntimeError("installed daemon changed before the admission barrier")
        rows_before = database.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        save(evidence / "admission-barrier.json", {"jobs": rows_before, "granted_leases": 0,
                                                  "blocking_containments": 0, "historical_cleared": len(cleared),
                                                  "pid": before["pid"], "creation_filetime_100ns": created})
        if not kernel.TerminateProcess(process, 0):
            raise ctypes.WinError(ctypes.get_last_error())
        if kernel.WaitForSingleObject(process, 10000) != 0:
            raise RuntimeError("old daemon did not exit within the maintenance bound")
        backup = installed.with_name("stillyard.previous-" + uuid.uuid4().hex + ".exe")
        os.rename(installed, backup)  # Existing monitor clients may retain its image.
        try:
            os.rename(staged, installed)
        except BaseException:
            os.rename(backup, installed)
            raise
        save(evidence / "replacement.json", {"backup": str(backup), "installed_sha256": digest(installed)})
        database.execute("ROLLBACK")  # Release the barrier only after publication.
        database.close()
        database = None
    finally:
        if database is not None:
            if database.in_transaction:
                database.execute("ROLLBACK")
            database.close()
        kernel.CloseHandle(process)
        if staged is not None:
            staged.unlink(missing_ok=True)

    # WMI creates the service process independently of the installing shell's Job
    # Object. The executable and store remain the reviewed installed coordinates.
    if args.endpoint:
        # Isolated test subjects stay inside the outer system Job, including on
        # harness failure. Only the real installed default uses independent WMI.
        with (evidence / "daemon.stdout").open("wb") as out, (evidence / "daemon.stderr").open("wb") as err:
            subprocess.Popen([str(installed), "daemon", "--store", str(store), "--endpoint", before["endpoint"]],
                             cwd=store, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                             creationflags=subprocess.CREATE_NO_WINDOW)
    else:
        start_default_via_wmi(installed, store, before["endpoint"], evidence)
    limit = time.monotonic() + 20
    while True:
        try:
            after = cli("daemon-status", "--deadline-seconds", "2")
            break
        except RuntimeError:
            if time.monotonic() >= limit:
                raise
            time.sleep(0.1)
    save(evidence / "after.json", after)
    if after["store_uuid"] != before["store_uuid"]:
        raise RuntimeError("installation changed store identity; do not initialize authority")
    authority = cli("authority", "status")
    save(evidence / "authority-before-initialize.json", authority)
    if authority["blocker"] == "authority_uninitialized" and args.initialize_new_authority:
        authority = cli("authority", "initialize", "--confirm-no-outstanding-work")
    save(evidence / "authority-after.json", authority)
    print(json.dumps({"installed": str(installed), "pid": after["pid"],
                      "version": after["version"], "authority_blocker": authority["blocker"]}), flush=True)


def start_default_via_wmi(installed, store, endpoint, evidence):
    starter = evidence / "start.ps1"
    starter.write_text("param([string] $DaemonCommand, [string] $DaemonDirectory)\n"
                       "$ErrorActionPreference = 'Stop'\n"
                       "Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments "
                       "@{CommandLine=$DaemonCommand; CurrentDirectory=$DaemonDirectory} | ConvertTo-Json\n")
    command = subprocess.list2cmdline([str(installed), "daemon", "--background-child", "--store", str(store), "--endpoint", endpoint])
    start = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(starter), command, str(store)], capture_output=True, timeout=20)
    (evidence / "start.stdout").write_bytes(start.stdout)
    (evidence / "start.stderr").write_bytes(start.stderr)


if __name__ == "__main__":
    main()
