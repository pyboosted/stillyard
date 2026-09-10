#!/usr/bin/env python3
"""Installed WSL lifetime/delegation helper, outside user Invocation namespaces.

The Windows scheduled task runs keepalive in a foreground wsl.exe session. It
publishes only a transport alias, never resource or cleanup authority. The user
service runs delegate before the pinned daemon. Neither operation invokes Cargo.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import uuid


def private_directory(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
        raise RuntimeError("service state directory must be owner-only")


def keepalive(root):
    private_directory(root)
    descriptor = os.open(root / "keepalive.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
        raise RuntimeError("unsafe keepalive singleton file")
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    socket = Path(os.environ["WSL_INTEROP"])
    metadata = socket.lstat()
    if not socket.is_absolute() or socket.parent != Path("/run/WSL") or not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != 0:
        raise RuntimeError("keepalive must inherit a real WSL interop server")
    alias = root / "interop.sock"
    if alias.exists() and not alias.is_symlink():
        raise RuntimeError("refusing to replace a non-alias interop path")
    staging = root / (".interop-" + uuid.uuid4().hex)
    staging.symlink_to(socket)
    staging.replace(alias)
    print(json.dumps({"pid": os.getpid(), "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                      "interop_alias": str(alias), "interop_server": str(socket), "kind": "wsl_keepalive"}), flush=True)
    environment = {"XDG_RUNTIME_DIR": f"/run/user/{os.geteuid()}",
                   "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{os.geteuid()}/bus"}
    subprocess.run(["/usr/bin/systemctl", "--user", "start", "--no-block", "stillyard.service"],
                   env=environment, check=True, timeout=15)
    termination = {signal.SIGTERM, signal.SIGINT}
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, termination)
    try:
        signal.sigwait(termination)
    finally:
        if alias.is_symlink() and alias.readlink() == socket:
            alias.unlink()
        os.close(descriptor)
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)


def delegate(executors, ram_mb):
    # DelegateSubgroup=manager keeps service processes out of the delegated
    # parent, which must be empty before enabling domain controllers.
    entry = Path("/proc/self/cgroup").read_text().strip()
    if not entry.startswith("0::/") or "\n" in entry:
        raise RuntimeError("unified cgroup identity is unavailable")
    current = Path("/sys/fs/cgroup") / entry[4:]
    parent = current.parent
    if current.name not in ("manager", ".control") or parent.name != "stillyard.service" or executors != parent / "executors":
        raise RuntimeError(f"service cgroup differs from installed delegation: current={current}, expected={executors}")
    controllers = (parent / "cgroup.controllers").read_text().split()
    if not {"memory", "pids", "cpu"}.issubset(controllers):
        raise RuntimeError("required cgroup controllers were not delegated")
    (parent / "cgroup.subtree_control").write_text("+memory +pids +cpu")
    executors.mkdir(exist_ok=True)
    (executors / "cgroup.subtree_control").write_text("+memory +pids +cpu")
    if not 512 <= ram_mb <= 1048576:
        raise RuntimeError("invalid executor RAM hard limit")
    (executors / "memory.max").write_text(str(ram_mb * 1024 * 1024))
    (executors / "pids.max").write_text("4096")
    print(json.dumps({"kind": "wsl_delegation", "executors": str(executors),
                      "memory_max": (executors / "memory.max").read_text().strip(),
                      "controllers": (executors / "cgroup.subtree_control").read_text().strip()}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("keepalive")
    p.add_argument("--root", type=Path, required=True)
    p = sub.add_parser("delegate")
    p.add_argument("--executors", type=Path, required=True)
    p.add_argument("--ram-mb", type=int, required=True)
    p = sub.add_parser("run")
    p.add_argument("--executors", type=Path, required=True)
    p.add_argument("--ram-mb", type=int, required=True)
    p.add_argument("--daemon", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "keepalive":
        keepalive(args.root)
    else:
        delegate(args.executors, args.ram_mb)
        if args.command == "run":
            if not args.daemon.is_absolute():
                raise RuntimeError("installed daemon path must be absolute")
            os.execv(args.daemon, [str(args.daemon), "daemon"])


if __name__ == "__main__":
    main()
