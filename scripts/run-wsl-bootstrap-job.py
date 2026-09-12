#!/usr/bin/env python3
"""Submit first Linux Cargo gates as protected Windows system Stillyard Jobs.

Requires the installed native bootstrap implementation and its completed safety
controls. This script does not execute Cargo itself or start another scheduler.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gate", choices=("check", "test", "clippy", "msrv-check", "msrv-test", "build-release", "msrv-install"))
    parser.add_argument("--test-filter", help="Optional Rust test-name filter for a bounded runtime component check")
    parser.add_argument("--delegate-test-cgroup", action="store_true", help="Expose only this bootstrap Job's nested cgroup for executor tests (requires updated installed bridge)")
    parser.add_argument("--ignored-tests", action="store_true", help="Run explicitly selected ignored runtime acceptance tests")
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    parser.add_argument("--cli", type=Path, default=Path("/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe"))
    args = parser.parse_args()
    if args.test_filter and (args.gate not in ("test", "msrv-test") or args.test_filter.startswith("-")):
        parser.error("--test-filter requires test/msrv-test and cannot be a Cargo option")
    if args.delegate_test_cgroup and args.gate not in ("test", "msrv-test"):
        parser.error("nested test cgroup delegation is only valid for test/msrv-test")
    if args.ignored_tests and (args.gate not in ("test", "msrv-test") or not args.test_filter):
        parser.error("--ignored-tests requires test/msrv-test and an explicit test filter")
    if os.environ.get("STILLYARD_JOB_ID"):
        parser.error("this bootstrap launcher submits a root Job; managed consumers must use the child API")
    root = args.repository_root.resolve()
    manifest = json.loads(args.source_manifest.read_text())

    def changed_inputs():
        return [name for name, expected in manifest["files"].items()
                if not (root / name).is_file()
                or hashlib.sha256((root / name).read_bytes()).hexdigest() != expected]

    if changed_inputs():
        parser.error("Linux source bytes do not match the selected native source manifest")
    directory = args.evidence_directory.resolve() / ("wsl-" + args.gate + "-" + uuid.uuid4().hex)
    directory.mkdir(parents=True)

    def windows(path):
        return subprocess.check_output(["wslpath", "-w", str(path)], text=True).strip()

    def write(name, value):
        path = directory / name
        path.write_text(json.dumps(value, indent=2) + "\n")
        return path

    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo"))).resolve()
    rustup_home = Path(os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup"))).resolve()
    cargo = cargo_home / "bin/cargo"
    gate_args = {
        "check": ["check", "--locked", "--workspace", "--all-features", "--all-targets"],
        "test": ["test", "--locked", "--workspace", "--all-features"],
        "clippy": ["clippy", "--locked", "--workspace", "--all-features", "--all-targets", "--", "-D", "warnings"],
        "msrv-check": ["+1.85.0", "check", "--locked", "--workspace", "--all-features", "--all-targets"],
        "msrv-test": ["+1.85.0", "test", "--locked", "--workspace", "--all-features"],
        "build-release": ["build", "--locked", "--release"],
        "msrv-install": ["toolchain", "install", "1.85.0", "--profile", "minimal", "--component", "rustfmt", "--component", "clippy"],
    }[args.gate]
    if args.test_filter:
        gate_args.append(args.test_filter)
    if args.ignored_tests:
        gate_args.extend(["--", "--ignored", "--nocapture"])
    work = {"distribution": args.distribution, "user": args.user,
            "executable": str(cargo_home / "bin/rustup" if args.gate == "msrv-install" else cargo),
            "args": gate_args,
            "working_directory": str(root), "timeout_seconds": 1800,
            "environment": {"HOME": str(Path.home()), "CARGO_HOME": str(cargo_home),
                            "RUSTUP_HOME": str(rustup_home), "PATH": str(cargo_home / "bin") + ":/usr/bin:/bin",
                            "CARGO_TARGET_DIR": str(root / "target/scheduled-linux"),
                            "CARGO_BUILD_JOBS": "4"}}
    if args.gate in ("test", "msrv-test"):
        work["environment"]["STILLYARD_TEST_EXECUTABLE"] = str(root / "target/scheduled-linux/debug/stillyard")
    if args.delegate_test_cgroup:
        work["delegate_test_cgroup"] = True
    work_file = write("work.json", work)
    spec = {"spec_version": 4, "executable": windows(args.cli.resolve()),
            "args": ["bootstrap", "run", "--spec", windows(work_file)],
            "working_directory": windows(directory),
            "resources": {"cargo_slots": 1, "impacts": ["cpu_heavy"]},
            "labels": [{"key": "project", "value": "stillyard"},
                       {"key": "gate", "value": "wsl-bootstrap-" + args.gate},
                       {"key": "source", "value": manifest["files_sha256"]}],
            "expected_duration_seconds": 180, "timeout_seconds": 1830}
    spec_file = write("spec.json", spec)
    write("source-manifest.json", manifest)
    receipt_file = directory / "receipt.json"
    print("System Stillyard Job wsl-bootstrap-" + args.gate + "; evidence: " + str(directory), flush=True)
    with (directory / "submit.stdout").open("wb") as out, (directory / "submit.stderr").open("wb") as err:
        submitted = subprocess.run([str(args.cli), "submit", "--spec", windows(spec_file),
                                    "--result-file", windows(receipt_file), "--wait", "--passthrough",
                                    "--deadline-seconds", "86400"], stdout=out, stderr=err)
    receipt = json.loads(receipt_file.read_text())
    # NTFS publication can fail after acceptance. Recover the same durable
    # operation into a fresh file; never manufacture a new submission key.
    for attempt in range(3):
        if receipt.get('receipt') is not None:
            break
        recovered = directory / ('recovered-' + uuid.uuid4().hex + '.receipt.json')
        with (directory / f'recovery-{attempt}.stdout').open('wb') as out, (directory / f'recovery-{attempt}.stderr').open('wb') as err:
            submitted = subprocess.run([str(args.cli), '--endpoint', receipt['endpoint'],
                'ensure', '--spec', windows(spec_file), '--idempotency-key', receipt['idempotency_key'],
                '--result-file', windows(recovered), '--wait', '--passthrough',
                '--deadline-seconds', '86400'], stdout=out, stderr=err)
        if recovered.exists():
            receipt = json.loads(recovered.read_text())
    if receipt.get('receipt') is None:
        raise RuntimeError('bootstrap acceptance unknown; original key/spec retained for recovery')
    job_id = receipt["receipt"]["accepted"]["job_id"]
    result = subprocess.run([str(args.cli), "status", job_id, "--deadline-seconds", "5"], capture_output=True, check=True)
    status = json.loads(result.stdout)
    write("status.json", status)
    state = subprocess.run([str(args.cli), "authority", "status"], capture_output=True, check=True)
    write("authority.json", json.loads(state.stdout))
    changed = changed_inputs()
    write("result.json", {"job_id": job_id, "outcome": status["outcome"], "root_exit_code": status["root_exit_code"],
                          "submit_exit_code": submitted.returncode, "source_changed": changed,
                          "source_files_sha256": manifest["files_sha256"]})
    print(job_id, status["outcome"], "source_changed=" + str(bool(changed)), flush=True)
    return 1 if changed else submitted.returncode


if __name__ == "__main__":
    raise SystemExit(main())
