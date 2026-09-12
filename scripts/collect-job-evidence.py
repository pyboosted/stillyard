#!/usr/bin/env python3
"""Retain public status and canonical log chunks for completed Stillyard Jobs.

Reads durable launcher receipts. Does not submit, build, cancel, or reset anything.
"""

import argparse
import json
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("receipts", nargs="+", type=Path)
    args = parser.parse_args()
    args.destination.mkdir(parents=True, exist_ok=True)
    failures = []
    for receipt_path in args.receipts:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8-sig"))
        if not receipt.get("receipt"):
            # A durable client intent can precede acceptance or its lost reply.
            # Keep that evidence without inventing a Job ID or skipping the rest.
            if receipt_path.resolve() != (args.destination / receipt_path.name).resolve():
                shutil.copy2(receipt_path, args.destination / receipt_path.name)
            failures.append({"receipt": receipt_path.name,
                             "error": "acceptance unknown; recover using the recorded idempotency key"})
            continue
        job_id = receipt["receipt"]["accepted"]["job_id"]
        stem = receipt_path.name.removesuffix(".receipt.json")
        commands = [("status", ["status", job_id, "--deadline-seconds", "5"])]
        commands += [(stream, ["logs", job_id, "--stream", stream, "--json",
                               "--deadline-seconds", "5", "--limit", "1048576"])
                     for stream in ("stdout", "stderr")]
        job_final = False
        for name, command in commands:
            result = subprocess.run([args.cli, *command], capture_output=True, timeout=15)
            output = args.destination / (stem + "." + name + ".json")
            if result.returncode:
                output.with_suffix(".error.txt").write_bytes(result.stderr + result.stdout)
                failures.append({"job_id": job_id, "operation": name, "exit_code": result.returncode})
                continue
            document = json.loads(result.stdout)
            output.write_text(json.dumps(document, indent=2) + "\n")
            if name == "status":
                job_final = document.get("state") == "final"
            if name != "status" and not document.get("eof", False):
                failures.append({"job_id": job_id, "operation": name,
                                 "error": ("log exceeds single evidence chunk; collect continuation"
                                           if job_final else
                                           "Job is not final; retain this partial evidence and recollect after completion")})
        if receipt_path.resolve() != (args.destination / receipt_path.name).resolve():
            shutil.copy2(receipt_path, args.destination / receipt_path.name)
        print(job_id, flush=True)
    if failures:
        print(json.dumps(failures, indent=2))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
