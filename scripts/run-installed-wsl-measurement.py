#!/usr/bin/env python3
"""Submit the fixed W-C4 source-hashing measurement to the installed WSL manager.

Retains criteria before submission. Use its receipt to recover a lost client.
Coordinator events must separately prove cross-platform impact exclusion.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repository-root", type=Path, required=True)
    p.add_argument("--source-manifest", type=Path, required=True)
    p.add_argument("--evidence-directory", type=Path, required=True)
    p.add_argument("--round", type=int, required=True)
    p.add_argument("--no-wait", action="store_true")
    args = p.parse_args()
    os.umask(0o077)
    root = Path.home() / ".local/share/stillyard"
    cli = str(root / "bin/stillyard")
    directory = args.evidence_directory.resolve() / (f"wc4-round{args.round}-" + uuid.uuid4().hex)
    directory.mkdir(parents=True)
    def save(name, document):
        (directory / name).write_text(json.dumps(document, indent=2) + "\n")
    daemon = json.loads(subprocess.check_output([cli, "daemon-status"], timeout=15))
    if daemon["store_path"] != str(root) or daemon["machine_scheduling"]["mode"] != "attached":
        raise RuntimeError("W-C4 requires the installed default attached manager")
    save("daemon-before.json", daemon)
    source = json.loads(args.source_manifest.read_text())
    save("source.json", source)
    adapter = directory / "consumer.py"
    shutil.copyfile(Path(__file__).with_name("machine-resource-consumer.py"), adapter)
    save("criteria.json", {"metric": "256-round SHA-256 source verification wall and CPU seconds",
                           "source_files_sha256": source["files_sha256"],
                           "adapter_sha256": hashlib.sha256(adapter.read_bytes()).hexdigest(),
                           "criteria": "all source hashes match, positive finite durations, nonempty payload, exactly 256 rounds; no overlap with managed cpu_heavy in either OS",
                           "quiet_cpu_max_percent": 40, "quiet_stable_seconds": 3})
    spec = {"spec_version": 4, "executable": "/usr/bin/python3",
            "args": [str(adapter), "measure", "--repository-root", str(args.repository_root.resolve()),
                     "--source-manifest", str(directory / "source.json"), "--output", str(directory / "measurement.json")],
            "working_directory": str(directory), "resources": {"impacts": ["measurement"]},
            "quiet": {"stable_seconds": 3, "max_sample_age_seconds": 2, "wait_budget_seconds": 240,
                      "detectors": [{"kind": "cpu_utilization", "max_percent": 40}]},
            "timeout_seconds": 180,
            "labels": [{"key": "project", "value": "stillyard"}, {"key": "acceptance", "value": "W-C4"},
                       {"key": "round", "value": str(args.round)}]}
    save("spec.json", spec)
    key = str(uuid.uuid4())
    save("intent.json", {"idempotency_key": key, "endpoint": daemon["endpoint"]})
    command = [cli, "--endpoint", daemon["endpoint"], "ensure", "--spec", str(directory / "spec.json"),
               "--idempotency-key", key, "--result-file", str(directory / "receipt.json"), "--deadline-seconds", "600"]
    if not args.no_wait:
        command += ["--wait"]
    print(directory, flush=True)
    with (directory / "client.stdout").open("wb") as out, (directory / "client.stderr").open("wb") as err:
        result = subprocess.run(command, stdout=out, stderr=err)
    receipt = json.loads((directory / "receipt.json").read_text())
    print(json.dumps({"job_id": receipt["receipt"]["accepted"]["job_id"], "client_exit_code": result.returncode}), flush=True)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
