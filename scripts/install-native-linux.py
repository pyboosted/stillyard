#!/usr/bin/env python3
"""Prepare or apply a first standalone Linux user-service installation.

Use a separately identified native host. The development WSL pair is rejected.
The candidate must have a retained build origin and SHA-256. This script does
not compile it. Installation is not consumer or lifecycle acceptance.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_new(path, data, mode=0o600):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(path.parent)


def save(path, value):
    write_new(path, (json.dumps(value, indent=2) + '\n').encode())


def quote(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%').replace('$', '$$') + '"'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--candidate-sha256', required=True)
    parser.add_argument('--build-origin', required=True,
                        help='Retained Stillyard build Job ID or native CI build URL')
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    parser.add_argument('--ram-mb', type=int, default=4096)
    parser.add_argument('--cargo-slots', type=int, default=2)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--no-helper', action='store_true', help=argparse.SUPPRESS)  # Preview compatibility; now the default.
    args = parser.parse_args()
    os.umask(0o077)
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel:
        parser.error('standalone installation requires a separate native host, outside WSL')
    if os.environ.get('STILLYARD_JOB_ID'):
        parser.error('service installation requires the host context outside a managed Invocation')
    if subprocess.run(['/usr/bin/systemd-detect-virt', '--container', '--quiet'], timeout=10).returncode != 1:
        parser.error('this installer supports a host user service; container profiles require their adapter')
    if digest(args.candidate) != args.candidate_sha256:
        parser.error('candidate differs from the selected build digest')
    if not 512 <= args.ram_mb <= 1048576 or not 1 <= args.cargo_slots <= 64:
        parser.error('invalid RAM budget or Cargo slot capacity')
    version = subprocess.check_output(['/usr/bin/systemctl', '--version'], text=True, timeout=10)
    match = re.match(r'systemd (\d+)', version)
    if match is None or int(match[1]) < 254:
        parser.error('native profile requires systemd >=254 with DelegateSubgroup')
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    root = root.absolute()
    if any(ord(character) < 32 for character in str(root)):
        parser.error('native service installation path must not contain control characters')
    unit = Path.home() / '.config/systemd/user/stillyard.service'
    delegation_unit = unit.with_name('stillyard-delegation.service')
    if delegation_unit.exists() or delegation_unit.is_symlink():
        parser.error('first installation refuses an existing delegation service')
    if root.exists() or root.is_symlink() or unit.exists() or unit.is_symlink():
        parser.error('first installation refuses existing Store or service; preserve it for audited upgrade/recovery')
    if root != root.resolve():
        parser.error('installation root must be canonical and contain no symlink component')
    probe = subprocess.run(['/usr/bin/python3', str(args.source_root / 'scripts/probe-native-linux.py'),
                            '--store', str(root)], capture_output=True, text=True, timeout=45)
    preflight = json.loads(probe.stdout)
    evidence = args.evidence_directory.resolve() / ('native-install-' + uuid.uuid4().hex)
    evidence.mkdir(mode=0o700, parents=True)
    save(evidence / 'prerequisites.json', preflight)
    if probe.returncode != 0 or not preflight['prerequisites_passed']:
        parser.error('native prerequisites did not pass; retained at ' + str(evidence))
    executors = Path(f'/sys/fs/cgroup/user.slice/user-{os.geteuid()}.slice/user@{os.geteuid()}.service/app.slice/stillyard-delegation.service/executors')
    daemon = root / 'bin/stillyard'
    helper = root / 'libexec/native-linux-service.py'
    configuration = {
        'resources': {'cpu_units': len(os.sched_getaffinity(0)), 'ram_mb': args.ram_mb,
                      'cargo_slots': args.cargo_slots},
        'impact_incompatibilities': {'cpu_heavy': [], 'measurement': ['cpu_heavy', 'gpu_heavy']},
        'observation': {'ram_safety_margin_mb': 256,
                        'process_rules': {'block': ['cargo', 'rustc', 'rust-analyzer'], 'ignore': []}},
    }
    delegation_text = '''[Unit]
Description=Stillyard retained native executor delegation
StopWhenUnneeded=no

[Service]
Type=oneshot
ExecStart=/usr/bin/true
RemainAfterExit=yes
Delegate=cpu memory pids
Slice=app.slice
KillMode=control-group
'''
    unit_text = f'''[Unit]
Description=Stillyard standalone Linux scheduler
Requires=stillyard-delegation.service
After=stillyard-delegation.service
StartLimitIntervalSec=0

[Service]
Type=exec
ExecStartPre=/usr/bin/python3 {quote(helper)} --root {quote(root)} --executors {quote(executors)} --ram-mb {args.ram_mb} --setup-only
ExecStart={quote(daemon)} daemon
WorkingDirectory={str(root).replace('%', '%%')}
Environment={quote('XDG_DATA_HOME=' + str(root.parent))}
Slice=app.slice
KillMode=control-group
TimeoutStopSec=10
Restart=on-failure
RestartSec=2
UMask=0077
UnsetEnvironment=STILLYARD_STORE STILLYARD_ENDPOINT STILLYARD_JOB_ID STILLYARD_ATTEMPT STILLYARD_INVOCATION_ID STILLYARD_ROLE

[Install]
WantedBy=default.target
'''
    inputs = {name: args.source_root / 'scripts' / name
              for name in ('native-linux-service.py', 'wsl-service.py')}
    hashes = {name: digest(path) for name, path in inputs.items()}
    save(evidence / 'plan.json', {'candidate_sha256': args.candidate_sha256,
                                'build_origin': args.build_origin, 'store': str(root),
                                'helper_sha256': hashes, 'unit': str(unit), 'unit_text': unit_text,
                                'delegation_unit_text': delegation_text, 'no_helper': True,
                                'configuration': configuration, 'apply': args.apply})
    if args.apply:
        root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        root.mkdir(mode=0o700)
        sync_directory(root.parent)
        for directory in (daemon.parent, helper.parent):
            directory.mkdir(mode=0o700)
        write_new(daemon, args.candidate.read_bytes(), 0o700)
        if digest(daemon) != args.candidate_sha256:
            raise RuntimeError('installed candidate digest changed; daemon not started')
        for name, source in inputs.items():
            path = helper.parent / name
            write_new(path, source.read_bytes())
            if digest(path) != hashes[name]:
                raise RuntimeError('service helper digest changed; daemon not started')
        save(root / 'config.json', configuration)
        save(root / 'native-install-request.json', {
            'daemon_sha256': args.candidate_sha256, 'store': str(root),
            'executor_cgroup': str(executors),
        })
        unit.parent.mkdir(parents=True, exist_ok=True)
        if delegation_text:
            write_new(delegation_unit, delegation_text.encode())
        write_new(unit, unit_text.encode())
        subprocess.run(['/usr/bin/systemd-analyze', '--user', 'verify', str(unit)], check=True, timeout=15)
        subprocess.run(['/usr/bin/systemctl', '--user', 'daemon-reload'], check=True, timeout=15)
        subprocess.run(['/usr/bin/systemctl', '--user', 'enable', 'stillyard.service'],
                       check=True, timeout=15)
        subprocess.run(['/usr/bin/systemctl', '--user', 'start', 'stillyard.service'],
                       check=True, timeout=30)
        until = time.monotonic() + 45
        while not (root / 'native-install-result.json').exists():
            if time.monotonic() >= until:
                raise RuntimeError('native setup did not finish; retain service journal and partial installation')
            time.sleep(.25)
        receipt = json.loads((root / 'native-install-result.json').read_text())
        save(evidence / 'setup-receipt.json', receipt)
        until = time.monotonic() + 45
        while True:
            try:
                status = json.loads(subprocess.check_output(
                    [str(daemon), '--endpoint', receipt['endpoint'], 'daemon-status', '--deadline-seconds', '5'],
                    text=True, timeout=10))
                machine = status['machine_scheduling']
                if not (evidence / 'first-daemon-status.json').exists():
                    save(evidence / 'first-daemon-status.json', status)
                if (status['store_uuid'] == receipt['store_uuid'] and status['store_path'] == str(root)
                        and machine['mode'] in ('standalone', 'coordinator') and machine['blocker'] is None
                        and machine['domains']['native_domain'] == receipt['configuration']['domain']
                        and os.path.samefile('/proc/' + str(status['pid']) + '/exe', daemon)):
                    save(evidence / 'installed.json', {'setup': receipt, 'daemon': status,
                                                     'installed_sha256': digest(daemon)})
                    break
            except (subprocess.SubprocessError, OSError, KeyError, TypeError, json.JSONDecodeError):
                pass
            if time.monotonic() >= until:
                raise RuntimeError('native setup completed but installed daemon did not become healthy; preserve service logs')
            time.sleep(.25)
    print(evidence)


if __name__ == '__main__':
    main()
