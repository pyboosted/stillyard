#!/usr/bin/env python3
"""Installed WSL lifetime/delegation helper, outside user Invocation namespaces.

A Windows lifetime owner runs keepalive in a foreground wsl.exe session. It
publishes only a transport alias, never resource or cleanup authority. The user
service owns delegation and supervises the pinned daemon in a separate subgroup,
so daemon crashes cannot make systemd remove unsealed executor boundaries.
Neither operation invokes Cargo.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import selectors
import stat
import subprocess
import time
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


def supervise(executors, daemon):
    """Keep delegation alive across daemon crashes; never clean user cgroups.

    systemd 259 may spawn a restarted unit into a non-leaf cgroup (EBUSY).
    More importantly, stopping that unit can remove empty but unsealed cgroups.
    Its persistent main process therefore owns delegation; the daemon alone is
    restarted here and must reconcile its unchanged journal/boundary identities.
    """
    parent = executors.parent
    control = parent / "manager"
    keeper = parent / "supervisor"
    keeper.mkdir(exist_ok=True)
    (keeper / "cgroup.procs").write_text(str(os.getpid()))
    closing = False
    def stop(_number, _frame):
        nonlocal closing
        closing = True
    read_fd, write_fd = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
    previous_wakeup = signal.set_wakeup_fd(write_fd)
    previous_term = signal.signal(signal.SIGTERM, stop)
    previous_int = signal.signal(signal.SIGINT, stop)
    selector = selectors.DefaultSelector()
    selector.register(read_fd, selectors.EVENT_READ)
    def drain_signal():
        try:
            while os.read(read_fd, 4096):
                pass
        except BlockingIOError:
            pass
    def emit(value):
        # A failed journal sink must not tear down the lifetime owner.
        try:
            print(json.dumps(value), flush=True)
        except OSError:
            pass
    def clear_control():
        # Only trusted daemon/bridge processes inherit manager. Invocations are
        # born separately under executors and are cleaned ONLY by their journal.
        if not control.exists():
            return
        (control / "cgroup.kill").write_text("1")
        until = time.monotonic() + 5
        while dict(line.split() for line in (control / "cgroup.events").read_text().splitlines())["populated"] != "0":
            if time.monotonic() >= until:
                raise RuntimeError("control-plane cgroup did not empty; executor history retained")
            time.sleep(.01)
    def place_child():
        # This process must remain single-threaded: preexec_fn runs Python
        # between fork and exec to place the trusted daemon before it can spawn.
        (control / "cgroup.procs").write_text(str(os.getpid()))
        signal.set_wakeup_fd(-1)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    delay = 1
    child = None
    try:
        while True:
            pidfd = None
            registered = False
            started = time.monotonic()
            try:
                # A prior failed teardown must complete before another spawn.
                clear_control()
                if child is not None:
                    child.wait(timeout=1)
                    child = None
                if closing:
                    break
                control.mkdir(exist_ok=True)
                child = subprocess.Popen([str(daemon), "daemon"], preexec_fn=place_child)
                # Unreaped direct child: no PID reuse before pidfd_open.
                pidfd = os.pidfd_open(child.pid)
                selector.register(pidfd, selectors.EVENT_READ)
                registered = True
                emit({"kind": "wsl_daemon_started", "supervisor_pid": os.getpid(),
                      "daemon_pid": child.pid, "executable": str(daemon)})
                while child.poll() is None and not closing:
                    for key, _ in selector.select():
                        if key.fd == read_fd:
                            drain_signal()
                if closing and child.poll() is None:
                    try:
                        signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        clear_control()
                code = child.wait(timeout=1)
                clear_control()
                emit({"kind": "wsl_daemon_exited", "daemon_pid": child.pid,
                      "exit_code": code, "service_stopping": closing})
                child = None
                if closing:
                    break
            except Exception as error:
                # Retain the main process and delegated tree even when spawn,
                # inspection or teardown fails. Never infer executor emptiness.
                emit({"kind": "wsl_supervisor_retry", "error": str(error),
                      "service_stopping": closing})
            finally:
                if registered:
                    selector.unregister(pidfd)
                if pidfd is not None:
                    os.close(pidfd)
            delay = 1 if time.monotonic() - started >= 10 else min(delay * 2, 30)
            # No idle polling. Only failure backoff has a timer; stop wakes it.
            for key, _ in selector.select(timeout=delay):
                if key.fd == read_fd:
                    drain_signal()
    finally:
        # Routine child failures are handled inside the loop. Always restore
        # signal/fd state if an exceptional interpreter shutdown escapes it.
        try:
            if child is not None and child.poll() is None:
                clear_control()
                child.wait(timeout=1)
        finally:
            selector.close()
            signal.set_wakeup_fd(previous_wakeup)
            signal.signal(signal.SIGTERM, previous_term)
            signal.signal(signal.SIGINT, previous_int)
            os.close(read_fd)
            os.close(write_fd)


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
            supervise(args.executors, args.daemon)


if __name__ == "__main__":
    main()
