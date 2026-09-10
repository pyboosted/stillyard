#!/usr/bin/env python3
"""Explicit owner pairing for an already staged, stopped WSL default manager.

Stable registration files remain private on ext4. Re-running resumes the exact
registration identities; it never issues a new identity to bypass lost history.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_once(path, value):
    if path.exists():
        if read_json(path) != value:
            raise RuntimeError("existing installation input differs: " + str(path))
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staging-evidence", type=Path, required=True)
    parser.add_argument("--distribution", required=True)
    parser.add_argument("--distribution-guid", required=True)
    parser.add_argument("--windows-cli", type=Path, default=Path("/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--parent-vm-domain", required=True)
    args = parser.parse_args()
    if os.environ.get("STILLYARD_JOB_ID"):
        parser.error("pairing requires the unmanaged installation context")
    if os.environ.get("WSL_DISTRO_NAME") != "Stillyard-MR3-Test" or args.distribution != "Stillyard-MR3-Test":
        parser.error("fixture pairing refuses every other distribution")
    prepared = read_json(args.staging_evidence / "prepared-store.json")
    plan = read_json(args.staging_evidence / "plan.json")
    root = Path(prepared["store_path"])
    private = root / "pairing-installation"
    private.mkdir(mode=0o700, exist_ok=True)
    if private.stat().st_mode & 0o077 or private.stat().st_uid != os.geteuid():
        raise RuntimeError("pairing installation inputs must be owner-only")

    def windows(*arguments):
        if arguments[0] == "machine":
            arguments = ("--endpoint", status["endpoint"], *arguments)
        result = subprocess.run([str(args.windows_cli), *arguments], capture_output=True, timeout=15)
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace"))
        return json.loads(result.stdout)

    status = windows("daemon-status", "--deadline-seconds", "5")
    authority = windows("authority", "status")
    if status["version"] != "0.1.0-alpha.20" or authority["blocker"] is not None:
        raise RuntimeError("pairing requires the validated IPC-25 coordinator without an authority blocker")
    context = windows("context", "--json", "--deadline-seconds", "5")
    if context["parent"] is not None:
        raise RuntimeError("pairing cannot impersonate an unmanaged owner")
    machine = authority["domains"]
    registry = private / "identities.json"
    if not registry.exists():
        write_once(registry, {"machine_id": machine["machine_id"],
                             "manager_store_uuid": prepared["store_uuid"],
                             "distribution": args.distribution,
                             "distribution_guid": str(uuid.UUID(args.distribution_guid)),
                             "vm_domain": str(uuid.UUID(args.parent_vm_domain)), "vm_installation": str(uuid.uuid4()),
                             "vm_store": str(uuid.uuid4()), "executor_domain": str(uuid.uuid4()),
                             "executor_installation": str(uuid.uuid4()), "journal": str(uuid.uuid4())})
    identities = read_json(registry)
    if (identities["machine_id"] != machine["machine_id"] or identities["manager_store_uuid"] != prepared["store_uuid"]
            or identities["distribution"] != args.distribution or identities["distribution_guid"] != str(uuid.UUID(args.distribution_guid))):
        raise RuntimeError("retained pairing coordinates differ; no automatic replacement is permitted")
    if identities["vm_domain"] != str(uuid.UUID(args.parent_vm_domain)):
        raise RuntimeError("retained VM parent changed")
    registration_specs = []
    for name, role, store, parent, budgets, runtime in [
        ("executor", "executor", prepared["store_uuid"], identities["vm_domain"], {"ram_mb": 1024, "cargo_slots": 1},
         "wsl2-distribution:" + identities["distribution_guid"] + ":" + args.distribution),
    ]:
        path = private / (name + "-registration.json")
        secret = read_json(path)["secret"] if path.exists() else list(os.urandom(32))
        registration = {"installation": {"installation_nonce": identities[name + "_installation"],
                                          "domain_id": identities[name + "_domain"], "owner_uid": os.geteuid(),
                                          "runtime_registration": runtime, "role": role},
                        "manager_store_uuid": store, "parent_domain": parent,
                        "budgets": budgets, "aliases": {}, "secret": secret}
        write_once(path, registration)
        registration_specs.append(path)
    with args.windows_cli.open("rb") as stream:
        bridge_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    configuration = {"version": 1, "pairing": read_json(registration_specs[0]),
                     "coordinator_installation": machine["machine_id"], "machine_id": machine["machine_id"],
                     "bridge_executable": str(args.windows_cli.resolve()), "bridge_sha256": bridge_sha256,
                     "coordinator_endpoint": status["endpoint"], "interop_socket": plan["interop_alias"],
                     "executor_cgroup": plan["executor_cgroup"], "journal": identities["journal"]}
    configuration_file = private / "configuration.json"
    write_once(configuration_file, configuration)
    write_once(args.staging_evidence / "pairing-plan.json", {"identities": identities,
               "bridge_sha256": bridge_sha256, "coordinator_endpoint": status["endpoint"],
               "registrations": [str(p) for p in registration_specs], "configuration": str(configuration_file)})
    if args.apply:
        for name, path in zip(("executor",), registration_specs):
            selected = subprocess.check_output(["wslpath", "-w", str(path)], text=True).strip()
            participant = windows("machine", "pair", "--spec", selected)
            write_once(args.staging_evidence / (name + "-paired.json"), participant)
        anchor = root / "attachment/anchor.json"
        if anchor.exists():
            if read_json(anchor)["configuration"] != configuration:
                raise RuntimeError("installed anchor differs from retained pairing input")
            # This read/open still validates the SQLite/anchor continuity.
            result = subprocess.check_output([plan["installed"], "wsl-install"], timeout=15)
        else:
            result = subprocess.check_output([plan["installed"], "wsl-install", "--configuration", str(configuration_file)], timeout=15)
        write_once(args.staging_evidence / "paired-store.json", {"store_uuid": json.loads(result)["store_uuid"],
                   "state": "paired_stopped", "executor_domain": identities["executor_domain"]})
    print(args.staging_evidence)


if __name__ == "__main__":
    main()
