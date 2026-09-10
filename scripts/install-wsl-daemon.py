#!/usr/bin/env python3
"""Stage a validated first WSL installation and its stopped Store.

No Cargo and no queue reset. This initial-install tool refuses an existing binary
or service. Explicit pairing and Windows keepalive registration follow the saved
setup receipt; staging alone is not installed-runtime acceptance.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def quote(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%').replace('$', '$$') + '"'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-sha256", required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--build-job-id", required=True)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    parser.add_argument("--windows-cli", type=Path, default=Path("/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if os.environ.get("STILLYARD_JOB_ID"):
        parser.error("installation must run outside a managed Invocation")
    if digest(args.candidate) != args.candidate_sha256:
        parser.error("candidate digest differs from selected build")
    status = json.loads(subprocess.check_output([str(args.windows_cli), "status", args.build_job_id,
                                               "--deadline-seconds", "5"], timeout=15))
    if status["state"] != "final" or status["outcome"] != "succeeded":
        parser.error("the Linux release build Job has not succeeded")
    if {"key": "gate", "value": "wsl-bootstrap-build-release"} not in status["spec"]["labels"]:
        parser.error("selected Job is not the protected Linux release gate")
    root = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "stillyard"
    root = root.absolute()
    installed = root / "bin/stillyard"
    helper = root / "libexec/wsl-service.py"
    unit = Path.home() / ".config/systemd/user/stillyard.service"
    if installed.exists() or unit.exists():
        parser.error("initial installation refuses an existing executable or service; use an audited upgrade")
    source_helper = args.source_root / "scripts/wsl-service.py"
    if not source_helper.is_file():
        parser.error("selected source snapshot does not include the lifetime/delegation helper")
    executors = Path(f"/sys/fs/cgroup/user.slice/user-{os.geteuid()}.slice/user@{os.geteuid()}.service/app.slice/stillyard.service/executors")
    configuration = {"resources": {"ram_mb": 16384, "cargo_slots": 2,
                                  "custom": {name: 4 for name in ("claude1_slots", "claude2_slots", "codex1_slots", "codex2_slots", "grok_slots", "opencode_slots")}},
                     "impact_incompatibilities": {"cpu_heavy": [], "measurement": ["cpu_heavy", "gpu_heavy"]},
                     "observation": {"ram_safety_margin_mb": 1024,
                                     "process_rules": {"block": ["cargo", "rustc", "rust-analyzer"], "ignore": []}}}
    unit_text = f'''[Unit]
Description=Stillyard attached Windows/WSL scheduler
StartLimitIntervalSec=0

[Service]
Type=simple
ExecStart=/usr/bin/python3 {quote(helper)} run --executors {quote(executors)} --ram-mb 16384 --daemon {quote(installed)}
WorkingDirectory={str(root).replace("%", "%%")}
Delegate=cpu memory pids
DelegateSubgroup=manager
Slice=app.slice
KillMode=process
OOMPolicy=continue
TimeoutStopSec=infinity
Restart=no
RestartSec=2
UMask=0077
UnsetEnvironment=STILLYARD_STORE STILLYARD_ENDPOINT STILLYARD_JOB_ID STILLYARD_ATTEMPT STILLYARD_INVOCATION_ID STILLYARD_ROLE

[Install]
WantedBy=default.target
'''
    evidence = args.evidence_directory / ("install-" + uuid.uuid4().hex)
    evidence.mkdir(mode=0o700, parents=True)
    plan = {"build_job_id": args.build_job_id, "candidate": str(args.candidate.resolve()),
            "candidate_sha256": args.candidate_sha256, "installed": str(installed),
            "helper_sha256": digest(source_helper), "unit": str(unit), "unit_text": unit_text,
            "host_configuration": configuration, "executor_cgroup": str(executors),
            "interop_alias": str(root / "interop.sock"), "applied": args.apply}
    save(evidence / "plan.json", plan)
    if args.apply:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077:
            raise RuntimeError("existing Store root is not owner-only")
        if (root / "config.json").exists() or (root / "stillyard.sqlite3").exists():
            raise RuntimeError("initial installation refuses existing Store/configuration history")
        installed.parent.mkdir(mode=0o700, exist_ok=True)
        helper.parent.mkdir(mode=0o700, exist_ok=True)
        shutil.copyfile(args.candidate, installed)
        installed.chmod(0o700)
        shutil.copyfile(source_helper, helper)
        helper.chmod(0o600)
        if digest(installed) != args.candidate_sha256 or digest(helper) != plan["helper_sha256"]:
            raise RuntimeError("copied installation inputs differ from verified plan; nothing executed")
        for path in (installed, helper):
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        save(root / "config.json", configuration)
        receipt = json.loads(subprocess.check_output([str(installed), "wsl-install"], timeout=15))
        if Path(receipt["store_path"]) != root:
            raise RuntimeError("installed binary selected different default coordinates")
        save(evidence / "prepared-store.json", receipt)
        unit.parent.mkdir(parents=True, exist_ok=True)
        with unit.open("x") as stream:
            stream.write(unit_text)
            stream.flush()
            os.fsync(stream.fileno())
        descriptor = os.open(unit.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        subprocess.run(["/usr/bin/systemctl", "--user", "daemon-reload"], check=True, timeout=15)
        save(evidence / "staged.json", {"installed_sha256": digest(installed), "helper_sha256": digest(helper),
                                     "state": "staged_stopped_unpaired"})
    print(evidence)


if __name__ == "__main__":
    main()
