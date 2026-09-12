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
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(receipt, stream)
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--executors', type=Path, required=True)
    parser.add_argument('--ram-mb', type=int, required=True)
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
    lifetime.delegate(args.executors, args.ram_mb, kind='native_linux')
    daemon = root / 'bin/stillyard'
    initialize(root, daemon, args.executors)
    lifetime.supervise(args.executors, daemon, kind='native_linux')


if __name__ == '__main__':
    main()
