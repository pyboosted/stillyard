#!/usr/bin/env python3
"""Native Linux service lifetime owner and explicit one-shot installation request.

The daemon never implicitly initializes authority. Only the installer-created,
durable request can trigger linux-install, once, while the Store is stopped.
The shared supervisor retains executor boundaries across daemon crashes.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import uuid


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_private(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(descriptor, 'rb') as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077:
            raise RuntimeError('unsafe native installation request')
        content = stream.read(65537)
    if len(content) > 65536:
        raise RuntimeError('native installation request exceeds byte bound')
    return json.loads(content)


def initialize(root, daemon, executors):
    request = root / 'native-install-request.json'
    claimed = root / 'native-install-started.json'
    if not request.exists():
        # Rust validates the completed anchor and its full history before starts.
        # Partial installation is never retried using a fresh authority.
        read_private(root / 'native-linux/anchor.json')
        return
    if claimed.exists() or (root / 'native-linux').exists() or (root / 'authority').exists():
        raise RuntimeError('installation request conflicts with prior native history')
    intent = read_private(request)
    if intent != {'daemon_sha256': hashlib.sha256(daemon.read_bytes()).hexdigest(),
                  'store': str(root), 'executor_cgroup': str(executors)}:
        raise RuntimeError('native installation request differs from service inputs')
    # Hard-link claim is exclusive; fsync it before removing the request. A
    # crash at either boundary leaves a visible claim that forbids replay.
    os.link(request, claimed, follow_symlinks=False)
    sync_directory(root)
    request.unlink()
    sync_directory(root)
    result = subprocess.run([str(daemon), 'linux-install', '--executor-cgroup', str(executors)],
                            capture_output=True, text=True, timeout=30, check=True)
    receipt = json.loads(result.stdout)
    if receipt['store_path'] != str(root):
        raise RuntimeError('native installer selected a different default Store')
    path = root / 'native-install-result.json'
    staging = root / ('.native-install-result-' + uuid.uuid4().hex)
    descriptor = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(receipt, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(staging, path, follow_symlinks=False)
    sync_directory(root)
    staging.unlink()
    sync_directory(root)


def delegated_unit_setup(root, executors, ram_mb):
    """Attach after the oneshot unit's initial prune.

    The active/exited delegated unit keeps the cgroup without a resident helper.
    A missing installed tree requires the stopped Rust quiescence verifier.
    """
    unit = 'stillyard-delegation.service'
    def property_value(name):
        return subprocess.check_output(['/usr/bin/systemctl', '--user', 'show', unit,
                                        '--property=' + name, '--value'], text=True, timeout=10).strip()
    if (property_value('ActiveState') != 'active' or property_value('SubState') != 'exited'
            or property_value('MainPID') != '0'):
        raise RuntimeError('native delegation unit is not active/exited at the installed path')
    first = (root / 'native-install-request.json').exists()
    missing = not executors.exists()
    if first:
        if executors.exists() or (root / 'native-install-started.json').exists():
            raise RuntimeError('first delegation setup conflicts with prior state')
    elif missing:
        try:
            read_private(root / 'native-linux/anchor.json')
            read_private(root / 'config.json')
        except OSError as error:
            raise RuntimeError('native restoration requires retained installation and configuration') from error
    if first or missing:
        current = property_value('ControlGroup')
        existing_setup = (Path('/sys/fs/cgroup') / current.lstrip('/') / 'setup') if current else None
        suffix = ''
        if existing_setup is not None and existing_setup.exists():
            metadata = existing_setup.lstat()
            if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise RuntimeError('native setup subgroup identity is unsafe')
            suffix = '/setup'
        subprocess.run(['/usr/bin/busctl', '--user', 'call', 'org.freedesktop.systemd1',
                        '/org/freedesktop/systemd1', 'org.freedesktop.systemd1.Manager',
                        'AttachProcessesToUnit', 'ssau', unit, suffix, '1', str(os.getpid())], check=True, timeout=10)
    # ControlGroup is empty after the initial SERVICE_EXITED prune. The
    # supported attach operation realizes it; only then can it be compared.
    group = Path('/sys/fs/cgroup') / property_value('ControlGroup').lstrip('/')
    if executors != group / 'executors':
        raise RuntimeError('native delegated cgroup differs from the installed path')
    if first or missing:
        setup = group / 'setup'
        setup.mkdir(exist_ok=True)
        (setup / 'cgroup.procs').write_text(str(os.getpid()))
        (group / 'cgroup.subtree_control').write_text('+cpu +memory +pids')
    if first:
        executors.mkdir()
        (executors / 'cgroup.subtree_control').write_text('+cpu +memory +pids')
        (executors / 'memory.max').write_text(str(ram_mb * 1024 * 1024))
        (executors / 'pids.max').write_text('4096')
    elif missing:
        result = subprocess.run([str(root / 'bin/stillyard'), 'linux-restore-executors'],
                                capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError('native executor restoration refused: ' + result.stderr.strip())
        receipt = json.loads(result.stdout)
        path = root / ('native-executor-restored-' + uuid.uuid4().hex + '.json')
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), 'w') as stream:
            json.dump(receipt, stream)
            stream.flush()
            os.fsync(stream.fileno())
        sync_directory(root)
    if (not executors.is_dir() or not {'cpu', 'memory', 'pids'}.issubset(
            (executors / 'cgroup.subtree_control').read_text().split())
            or (executors / 'memory.max').read_text().strip() != str(ram_mb * 1024 * 1024)
            or (executors / 'pids.max').read_text().strip() != '4096'):
        raise RuntimeError('native delegated executor tree is missing or changed; history retained')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--executors', type=Path, required=True)
    parser.add_argument('--ram-mb', type=int, required=True)
    parser.add_argument('--setup-only', action='store_true')
    args = parser.parse_args()
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel:
        parser.error('native service cannot replace WSL authority')
    root = args.root
    if not root.is_absolute() or root != root.resolve():
        parser.error('native Store requires a canonical absolute path')
    module = importlib.util.spec_from_file_location('stillyard_linux_lifetime',
                                                  Path(__file__).with_name('wsl-service.py'))
    lifetime = importlib.util.module_from_spec(module)
    module.loader.exec_module(lifetime)
    lifetime.private_directory(root)
    if args.setup_only:
        delegated_unit_setup(root, args.executors, args.ram_mb)
    else:
        lifetime.delegate(args.executors, args.ram_mb, kind='native_linux')
    daemon = root / 'bin/stillyard'
    initialize(root, daemon, args.executors)
    if not args.setup_only:
        lifetime.supervise(args.executors, daemon, kind='native_linux')


if __name__ == '__main__':
    main()
