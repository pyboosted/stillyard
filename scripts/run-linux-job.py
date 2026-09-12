#!/usr/bin/env python3
"""Submit Linux Cargo to the installed default standalone Linux coordinator.

The daemon enforces local atomic Lease admission and cgroup containment. This
launcher checks the selected installation; it cannot authorize Cargo itself.
Explicit endpoints are connect-only. WSL uses run-wsl-job.py instead.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
from string import Template
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    gates = ("fmt", "fmt-write", "check", "test", "clippy", "msrv-check", "msrv-test",
             "schema-update", "build-release")
    parser.add_argument("gate", choices=gates)
    parser.add_argument("--repository-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    parser.add_argument("--no-wait", action="store_true")
    args = parser.parse_args()
    if os.environ.get("STILLYARD_JOB_ID"):
        parser.error("root launcher cannot run inside a Job; use managed-build with its child policy")
    os.umask(0o077)
    root = args.repository_root.resolve()
    installation = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "stillyard"
    cli = installation / "bin/stillyard"
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel:
        parser.error('native launcher cannot bypass the Windows/WSL coordinator')
    def query(executable, *command):
        return json.loads(subprocess.check_output([str(executable), *command], timeout=15))
    local = query(cli, 'daemon-status')
    coordinator = local.get('machine_scheduling')
    if (Path(local['store_path']) != installation or coordinator is None
            or coordinator['mode'] not in ('standalone', 'coordinator') or coordinator['blocker'] is not None
            or not (installation / 'native-linux/anchor.json').is_file()):
        parser.error('installed default native Linux coordinator is not ready')
    source = None
    if args.source_manifest:
        source = json.loads(args.source_manifest.read_text())
        if any(not (root / n).is_file() or hashlib.sha256((root / n).read_bytes()).hexdigest() != h
               for n, h in source["files"].items()):
            parser.error("source bytes differ from the selected manifest")
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo"))).resolve()
    rustup_home = Path(os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup"))).resolve()
    toolchain = "1.85.0" if args.gate.startswith("msrv-") else subprocess.check_output(
        [str(cargo_home / "bin/rustup"), "show", "active-toolchain"], cwd=root, text=True).split()[0]
    values = {"REPOSITORY_ROOT": str(root), "USER_HOME": str(Path.home()),
              "CARGO_HOME": str(cargo_home), "RUSTUP_HOME": str(rustup_home),
              "RUSTUP_TOOLCHAIN": toolchain}
    template = root / ".stillyard/jobs/linux" / (args.gate + ".json.in")
    # Substitute into parsed string values, so unusual paths cannot inject JSON.
    def expand(value):
        if isinstance(value, str):
            return Template(value).substitute(values)
        if isinstance(value, list):
            return [expand(v) for v in value]
        if isinstance(value, dict):
            return {k: expand(v) for k, v in value.items()}
        return value
    spec = expand(json.loads(template.read_text()))
    for label in spec.get('labels', []):
        if label['key'] == 'gate':
            label['value'] = 'native-linux-' + args.gate
    directory = args.evidence_directory.resolve() / ("native-linux-" + args.gate + "-" + uuid.uuid4().hex)
    directory.mkdir(parents=True)
    def write(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + "\n")
    key = str(uuid.uuid4())
    write("spec.json", spec)
    write("intent.json", {"idempotency_key": key, "endpoint": local["endpoint"]})
    write("installation.json", {"native_linux": local,
                               "linux_sha256": hashlib.sha256(cli.read_bytes()).hexdigest()})
    if source:
        write("source-manifest.json", source)
    print("System native Linux Stillyard Job " + args.gate + "; evidence: " + str(directory), flush=True)
    command = [str(cli), "--endpoint", local["endpoint"], "ensure", "--spec", str(directory / "spec.json"),
               "--idempotency-key", key, "--result-file", str(directory / "receipt.json"),
               "--deadline-seconds", "86400"]
    if not args.no_wait:
        command += ["--wait", "--passthrough"]
    with (directory / "submit.stdout").open("wb") as out, (directory / "submit.stderr").open("wb") as err:
        result = subprocess.run(command, stdout=out, stderr=err)
    if not (directory / "receipt.json").exists():
        raise RuntimeError("submission did not produce a receipt: " +
                           (directory / "submit.stderr").read_text())
    receipt = json.loads((directory / "receipt.json").read_text())
    if not receipt.get("receipt"):
        raise RuntimeError("acceptance unknown: recover the retained intent on the same endpoint")
    job = receipt["receipt"]["accepted"]["job_id"]
    status = query(cli, "status", job)
    write("status.json", status)
    print(job, status["state"], status["outcome"], flush=True)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
