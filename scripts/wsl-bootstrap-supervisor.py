#!/usr/bin/env python3
"""Trusted, one-shot MR-0 Linux supervisor embedded by the Windows bootstrap CLI.

The Windows authority must durably hold admission BEFORE invoking `run` here.
This helper never treats a dead intermediary, missing unit, or missing directory
as proof of cleanup. `inspect` can only return a previously sealed durable proof.
"""

import hashlib
import json
import os
from pathlib import Path
import pwd
import select
import subprocess
import sys
import time
import uuid


def publish(path, document, exclusive=False):
    data = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".pending")
    with temporary.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    if exclusive:
        os.link(temporary, path)  # Atomic create-if-absent; never overwrite intent.
        temporary.unlink()
    else:
        os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def emit(document):
    print(json.dumps(document, separators=(",", ":")), flush=True)


def boot():
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def load(path):
    if path.stat().st_size > 1024 * 1024:
        raise RuntimeError("bootstrap record exceeds byte limit")
    return json.loads(path.read_text())


def operation_directory(operation_id):
    operation_id = str(uuid.UUID(operation_id))
    base = Path.home() / ".local/state/stillyard/bootstrap"
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    if base.is_symlink() or base.stat().st_uid != os.getuid():
        raise RuntimeError("bootstrap state root must belong to this owner")
    os.chmod(base, 0o700)
    return base / operation_id


def inspect(directory, expected_hash):
    intent = load(directory / "intent.json")
    if intent["request_sha256"] != expected_hash:
        raise RuntimeError("bootstrap operation payload conflict")
    record = load(directory / "proof.json")
    proof = record["proof"]
    encoded = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode()
    if hashlib.sha256(encoded).hexdigest() != record["sha256"]:
        raise RuntimeError("bootstrap proof checksum mismatch")
    boundary = load(directory / "boundary.json")
    claimed = load(directory / "claimed.json")
    if (proof["request_sha256"] != expected_hash or proof["operation_id"] != directory.name
            or proof["phase"] != "sealed_empty"):
        raise RuntimeError("bootstrap proof does not match the durable operation")
    if claimed["boot_id"] != proof["boot_id"] or any(
            boundary[key] != proof[key] for key in
            ("operation_id", "request_sha256", "boot_id", "uid", "cgroup_path", "cgroup_inode")):
        raise RuntimeError("bootstrap boundary history is not continuous")
    return proof


def supervise(directory):
    intent = load(directory / "intent.json")
    request = json.loads(intent["request"])
    # A second supervisor, even after a crash, has no right to release user code.
    publish(directory / "claimed.json", {"boot_id": boot()}, exclusive=True)
    cgroup_relative = Path("/proc/self/cgroup").read_text().strip()
    if not cgroup_relative.startswith("0::/") or "\n" in cgroup_relative:
        raise RuntimeError("unified delegated cgroup is required")
    parent = Path("/sys/fs/cgroup") / cgroup_relative[4:]
    boundary = parent / "work"
    boundary.mkdir()  # Exclusive creation, before any untrusted child exists.
    identity = {"operation_id": directory.name, "request_sha256": intent["request_sha256"],
                "boot_id": boot(), "uid": os.getuid(), "cgroup_path": str(boundary),
                "cgroup_inode": boundary.stat().st_ino, "phase": "prepared"}
    publish(directory / "boundary.json", identity, exclusive=True)
    if not (boundary / "cgroup.kill").exists():
        raise RuntimeError("cgroup.kill capability is required")
    test_cgroup_mount = None
    if request.get("delegate_test_cgroup", False):
        # The only writable delegation is INSIDE the already held outer Job.
        # Limit the sub-hierarchy so cleanup remains a bounded supervisor operation.
        (boundary / "cgroup.max.depth").write_text("16")
        (boundary / "cgroup.max.descendants").write_text("1024")
        test_cgroup_mount = directory / "test-cgroup"
        test_cgroup_mount.mkdir(mode=0o700)
    output = (directory / "stdout.bin").open("xb", buffering=0)
    errors = (directory / "stderr.bin").open("xb", buffering=0)
    barrier_read, barrier_write = os.pipe()
    child = os.fork()
    if child == 0:
        try:
            os.close(barrier_write)
            # Trusted child stub is already inside the supervisor unit boundary.
            # No user instruction runs before the parent's cgroup attachment and
            # durable release intent. Parent death closes this one-shot barrier.
            permission = os.read(barrier_read, 1)
            os.close(barrier_read)
            if permission != b"R":
                os._exit(125)
            os.dup2(output.fileno(), 1)
            os.dup2(errors.fileno(), 2)
            stdin = os.open("/dev/null", os.O_RDONLY)
            os.dup2(stdin, 0)
            os.close(stdin)
            environment = dict(request["environment"])
            environment.pop("STILLYARD_TEST_CGROUP_ROOT", None)
            command = ["/usr/bin/bwrap", "--bind", "/", "/", "--dev", "/dev",
                       "--proc", "/proc", "--unshare-user", "--unshare-pid",
                       "--unshare-uts", "--unshare-cgroup", "--new-session",
                       "--die-with-parent", "--ro-bind", "/dev/null", "/init",
                       "--tmpfs", "/run", "--ro-bind", "/sys/fs/cgroup", "/sys/fs/cgroup"]
            if test_cgroup_mount is not None:
                command.extend(["--bind", str(boundary), str(test_cgroup_mount)])
                environment["STILLYARD_TEST_CGROUP_ROOT"] = str(test_cgroup_mount)
            command.extend(["--chdir", request["working_directory"], "--", request["executable"], *request["args"]])
            os.execve(command[0], command, environment)
        except BaseException:
            os._exit(126)
    os.close(barrier_read)
    root_exit = None
    pidfd = None
    termination = "exited"
    try:
        pidfd = os.pidfd_open(child)
        tail = Path("/proc", str(child), "stat").read_text().rsplit(")", 1)[1].split()
        publish(directory / "root.json", {"pid": child, "start_ticks": tail[19],
                                          "boot_id": identity["boot_id"]}, exclusive=True)
        (boundary / "cgroup.procs").write_text(str(child))
        # Verify attachment before the only release byte. A fork failure or
        # attachment error never reaches the requested executable.
        if child not in {int(pid) for pid in (boundary / "cgroup.procs").read_text().split()}:
            raise RuntimeError("child did not enter its recorded cgroup")
        publish(directory / "release-intent.json", identity, exclusive=True)
        os.write(barrier_write, b"R")
        os.close(barrier_write)
        barrier_write = None
        deadline = time.monotonic() + request["timeout_seconds"]
        while True:
            reaped, status = os.waitpid(child, os.WNOHANG)
            if reaped:
                root_exit = os.waitstatus_to_exitcode(status)
                break
            if (directory / "cancel").exists():
                termination = "canceled"
                break
            if time.monotonic() >= deadline:
                termination = "timed_out"
                break
            # Loss of liveness is only a reason to kill; it never frees authority.
            if time.time() - (directory / "heartbeat").stat().st_mtime > 5:
                termination = "intermediary_lost"
                break
            select.select([pidfd], [], [], 0.05)
    finally:
        if barrier_write is not None:
            os.close(barrier_write)
        (boundary / "cgroup.kill").write_text("1")
        cleanup_deadline = time.monotonic() + 10
        while "populated 1" in (boundary / "cgroup.events").read_text():
            if time.monotonic() >= cleanup_deadline:
                raise RuntimeError("cgroup cleanup remains uncertain; no proof published")
            time.sleep(0.02)
        if root_exit is None:
            _, status = os.waitpid(child, 0)
            root_exit = os.waitstatus_to_exitcode(status)
        if boundary.stat().st_ino != identity["cgroup_inode"]:
            raise RuntimeError("cgroup identity changed; no proof published")
        if test_cgroup_mount is not None:
            # populated=0 is recursive. Remove only empty cgroup directories;
            # any race/failure leaves the outer obligation unsealed and retained.
            descendants = []
            for current, children, _ in os.walk(boundary, followlinks=False):
                descendants.extend(Path(current) / name for name in children)
                if len(descendants) > 1024:
                    raise RuntimeError("nested cgroup count exceeds cleanup bound")
            for nested in sorted(descendants, key=lambda p: len(p.parts), reverse=True):
                nested.rmdir()
        boundary.rmdir()  # Seal: the recorded boundary cannot receive another task.
        for stream in (output, errors):
            os.fsync(stream.fileno())
            stream.close()
        proof = dict(identity, phase="sealed_empty", root_exit_code=root_exit,
                     termination=termination)
        encoded = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode()
        publish(directory / "proof.json", {"proof": proof, "sha256": hashlib.sha256(encoded).hexdigest()}, exclusive=True)
        if pidfd is not None:
            os.close(pidfd)


def run(directory, request_text, source):
    request = json.loads(request_text)
    if pwd.getpwnam(request["user"]).pw_uid != os.getuid():
        raise RuntimeError("selected WSL owner does not match the executing UID")
    if request["operation_id"] != directory.name or not 1 <= request["timeout_seconds"] <= 86400:
        raise RuntimeError("invalid bootstrap operation or timeout")
    if not Path(request["executable"]).is_absolute() or not Path(request["working_directory"]).is_absolute():
        raise RuntimeError("bootstrap executable and working directory must be absolute")
    directory.mkdir(mode=0o700)  # Never replay a possibly started operation.
    request_hash = hashlib.sha256(request_text.encode()).hexdigest()
    publish(directory / "intent.json", {"request": request_text, "request_sha256": request_hash}, exclusive=True)
    helper = directory / "supervisor.py"
    with helper.open("x") as stream:
        stream.write(source)
        stream.flush()
        os.fsync(stream.fileno())
    (directory / "heartbeat").touch()
    started = subprocess.run([
        "/usr/bin/systemd-run", "--user", "--quiet", "--collect",
        "--unit=stillyard-bootstrap-" + directory.name, "--property=Delegate=yes",
        "--property=KillMode=control-group", "--property=TimeoutStopSec=10",
        "--property=RuntimeMaxSec=" + str(request["timeout_seconds"] + 30),
        "/usr/bin/python3", str(helper), "supervise", directory.name,
    ], stdin=subprocess.DEVNULL, capture_output=True, timeout=15)
    if started.returncode:
        raise RuntimeError("supervisor start uncertain: " + started.stderr.decode(errors="replace"))
    offsets = {"stdout": 0, "stderr": 0}
    deadline = time.monotonic() + request["timeout_seconds"] + 30
    complete = False
    try:
        while time.monotonic() < deadline:
            (directory / "heartbeat").touch()
            complete = (directory / "proof.json").exists()
            for name in offsets:
                path = directory / (name + ".bin")
                if not path.exists():
                    continue
                with path.open("rb") as stream:
                    stream.seek(offsets[name])
                    while data := stream.read(4096):
                        emit({"stream": name, "bytes": list(data)})
                        offsets[name] += len(data)
            if complete:
                emit({"proof": inspect(directory, request_hash)})
                return
            emit({"heartbeat": True})  # Broken Windows output triggers cancellation.
            time.sleep(0.1)
        raise RuntimeError("supervisor outcome unknown; retain Windows authority")
    finally:
        if not complete:
            (directory / "cancel").touch()


def main(source):
    # Source is supplied from the installed native executable, not an arbitrary
    # helper path selected by a Job. The supervisor receives an exact durable copy.
    mode, operation_id = sys.argv[1:3]
    runtime = "/run/user/" + str(os.getuid())
    os.environ.setdefault("XDG_RUNTIME_DIR", runtime)
    os.environ.setdefault("DBUS_SESSION_BUS_ADDRESS", "unix:path=" + runtime + "/bus")
    directory = operation_directory(operation_id)
    if mode == "supervise":
        supervise(directory)
    elif mode == "inspect":
        emit({"proof": inspect(directory, sys.argv[3])})
    elif mode == "run":
        run(directory, sys.argv[3], source)
    else:
        raise RuntimeError("unknown bootstrap helper operation")


if __name__ == "__main__":
    main(Path(__file__).read_text())
