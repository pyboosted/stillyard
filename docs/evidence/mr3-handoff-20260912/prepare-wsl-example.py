#!/usr/bin/env python3
"""Prepare a durable hello Job for an already paired default WSL installation."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("STILLYARD_JOB_ID"):
        parser.error("use the managed child API inside a Job")
    directory = args.directory.expanduser().absolute()
    if directory.exists():
        parser.error("directory already exists; rerun its submit.sh to recover the same Job")
    parent = directory.parent.resolve(strict=True)
    filesystem = subprocess.check_output(
        ["/usr/bin/stat", "-f", "-c", "%T", str(parent)], text=True, timeout=5
    ).strip()
    if filesystem != "ext2/ext3":
        parser.error("select a directory on local ext4, normally inside your WSL home")
    root = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "stillyard"
    cli = root / "bin/stillyard"
    status = json.loads(subprocess.check_output(
        [str(cli), "daemon-status", "--deadline-seconds", "5"], timeout=10
    ))
    scheduling = status.get("machine_scheduling") or {}
    if (scheduling.get("mode") != "attached" or scheduling.get("blocker") is not None
            or Path(status["store_path"]).resolve() != root.resolve()):
        parser.error("the installed default WSL manager must be paired and healthy")
    directory = parent / directory.name
    directory.mkdir(mode=0o700)
    os.umask(0o077)
    key = str(uuid.uuid4())
    spec = {
        "spec_version": 4,
        "executable": "/usr/bin/python3",
        "args": ["-c", "print('Hello from a scheduled WSL Job')"],
        "working_directory": str(Path.home()),
        "environment": {"set": {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}},
        "resources": {},
        "timeout_seconds": 30,
        "labels": [{"key": "project", "value": "stillyard-example"}],
    }
    for name, value in (
        ("job.json", spec),
        ("intent.json", {"idempotency_key": key, "endpoint": status["endpoint"],
                         "store_uuid": status["store_uuid"]}),
    ):
        with (directory / name).open("x") as stream:
            json.dump(value, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    command = [str(cli), "--endpoint", status["endpoint"], "ensure",
               "--spec", str(directory / "job.json"), "--idempotency-key", key,
               "--result-file", str(directory / "receipt.json"), "--wait",
               "--deadline-seconds", "120"]
    with (directory / "submit.sh").open("x") as stream:
        stream.write("#!/usr/bin/env bash\nset -euo pipefail\nexec " + shlex.join(command) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    for path in (directory, parent):
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    print("Prepared Stillyard Job intent:", key)
    print("Submit or recover:", "bash " + shlex.quote(str(directory / "submit.sh")))


if __name__ == "__main__":
    main()
