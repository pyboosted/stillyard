#!/usr/bin/env python3
"""Drained runtime-tree loss on a disposable native VM; never a reboot claim."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    parser.add_argument('--disposable-native-host', action='store_true', required=True)
    args = parser.parse_args()
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel or os.environ.get('STILLYARD_JOB_ID'):
        parser.error('runtime loss requires a separately selected unmanaged native VM')
    os.umask(0o077)
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    cli = root / 'bin/stillyard'
    directory = args.evidence_directory.resolve() / ('native-restore-' + uuid.uuid4().hex)
    directory.mkdir(parents=True)

    def save(name, value):
        (directory / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

    def query(*command):
        return json.loads(subprocess.check_output([str(cli), *command], timeout=15))

    def systemctl(*command):
        subprocess.run(['/usr/bin/systemctl', '--user', *command], check=True, timeout=45)

    before = query('daemon-status')
    endpoint = before['endpoint']
    if (Path(before['store_path']) != root or before['running_jobs'] or before['queued_jobs']
            or before['machine_scheduling']['mode'] not in ('standalone', 'coordinator')
            or before['machine_scheduling']['blocker'] is not None):
        raise RuntimeError('restore control requires a healthy drained native default')
    save('before', before)
    anchor = json.loads((root / 'native-linux/anchor.json').read_text())['configuration']
    executors = Path(anchor['executor_cgroup'])
    history = root / 'native-linux/executor/state.json'
    marker = directory / 'launches.txt'
    key = str(uuid.uuid4())
    save('canary.spec', {'spec_version': 4, 'executable': '/usr/bin/python3',
                         'args': ['-c', "from pathlib import Path; p=Path('launches.txt'); "
                                  "p.write_text((p.read_text() if p.exists() else '')+'launch\\n')"],
                         'working_directory': str(directory), 'timeout_seconds': 10,
                         'resources': {'cargo_slots': 1},
                         'labels': [{'key': 'gate', 'value': 'native-restore-canary'}]})

    def canary(name, intent_key):
        save(name + '.intent', {'idempotency_key': intent_key, 'endpoint': endpoint})
        subprocess.run([str(cli), '--endpoint', endpoint, 'ensure', '--spec', str(directory / 'canary.spec.json'),
                        '--idempotency-key', intent_key, '--result-file', str(directory / (name + '.receipt.json')),
                        '--wait', '--deadline-seconds', '30'], check=True, timeout=40)
        job = json.loads((directory / (name + '.receipt.json')).read_text())['receipt']['accepted']['job_id']
        until = time.monotonic() + 30
        while True:
            status = query('--endpoint', endpoint, 'status', job)
            save(name + '.status', status)
            if (status['outcome'] == 'succeeded' and status['allocations']
                    and all(a['state'] == 'released' for a in status['allocations'])
                    and json.loads(history.read_bytes())['state']['records'][status['invocation_id']]['seal'] is not None):
                return job
            if time.monotonic() >= until:
                raise RuntimeError('native canary did not settle and seal')
            time.sleep(.1)

    original_job = canary('original', key)
    if marker.read_text() != 'launch\n':
        raise RuntimeError('original canary launch count differs')

    def refused(name, expected=None):
        result = subprocess.run([str(cli), '--endpoint', endpoint, 'linux-restore-executors', '--store', str(root)],
                                capture_output=True, text=True, timeout=40)
        save(name, {'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
        if result.returncode == 0 or (expected is not None and expected not in result.stderr):
            raise RuntimeError('restore did not refuse ' + name + ': ' + result.stderr)

    refused('live-daemon-refused')
    systemctl('stop', 'stillyard.service')
    # Explicit stop prevents Restart=on-failure while negative durable inputs
    # are installed. All mutations are restored before any service starts.
    retained = [root / 'stillyard.sqlite3', root / 'config.json', root / 'native-linux/anchor.json',
                root / 'native-linux/executor/anchor.json', history,
                root / 'authority/anchor.json', root / 'authority/registry.json']
    original = {p: p.read_bytes() for p in retained}
    if any(record['seal'] is None for record in json.loads(original[history])['state']['records'].values()):
        raise RuntimeError('native history was not completely sealed before runtime loss')
    save('retained-hashes', {str(p.relative_to(root)): hashlib.sha256(data).hexdigest()
                             for p, data in original.items()})
    old_inode = executors.stat().st_ino
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    systemctl('stop', 'stillyard-delegation.service')
    until = time.monotonic() + 15
    while executors.exists():
        if time.monotonic() >= until:
            raise RuntimeError('stopped delegation did not remove its drained runtime tree')
        time.sleep(.1)
    save('runtime-absent', {'boot_id': boot, 'old_inode': old_inode, 'path': str(executors), 'absent': True})
    systemctl('start', 'stillyard-delegation.service')
    # Realize a valid parent before every durable negative control. Otherwise
    # a missing parent could mask a skipped history check as a harmless error.
    prepare = """import os,sys,subprocess
from pathlib import Path
unit='stillyard-delegation.service'
subprocess.run(['/usr/bin/busctl','--user','call','org.freedesktop.systemd1',
 '/org/freedesktop/systemd1','org.freedesktop.systemd1.Manager',
 'AttachProcessesToUnit','ssau',unit,'','1',str(os.getpid())],check=True,timeout=10)
current=subprocess.check_output(['/usr/bin/systemctl','--user','show',unit,
 '--property=ControlGroup','--value'],text=True,timeout=10).strip()
group=Path('/sys/fs/cgroup')/current.lstrip('/')
assert group==Path(sys.argv[1]) and group.stat().st_uid==os.geteuid()
setup=group/'setup';setup.mkdir()
(setup/'cgroup.procs').write_text(str(os.getpid()))
(group/'cgroup.subtree_control').write_text('+cpu +memory +pids')
"""
    subprocess.run(['/usr/bin/python3', '-c', prepare, str(executors.parent)], check=True, timeout=30)
    if executors.exists() or not {'cpu', 'memory', 'pids'}.issubset(
            (executors.parent / 'cgroup.subtree_control').read_text().split()):
        raise RuntimeError('negative controls require a valid delegated parent and absent executor')
    save('negative-parent', {'path': str(executors.parent), 'inode': executors.parent.stat().st_ino,
                             'controllers': (executors.parent / 'cgroup.subtree_control').read_text().split()})
    for path in retained:
        for mutation in ('missing', 'corrupt'):
            try:
                if mutation == 'missing':
                    path.unlink()
                else:
                    path.write_bytes(b'corrupt restoration control; do not reset\n')
                expected_bytes = None if mutation == 'missing' else path.read_bytes()
                refused(str(path.relative_to(root)).replace('/', '-') + '-' + mutation)
                if (path.read_bytes() if path.exists() else None) != expected_bytes or executors.exists():
                    raise RuntimeError('refused restore modified durable input or created runtime tree')
                if any(p.read_bytes() != data for p, data in original.items() if p != path):
                    raise RuntimeError('refused restore changed another durable history file')
            finally:
                with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600), 'wb') as stream:
                    stream.write(original[path])
                    stream.flush()
                    os.fsync(stream.fileno())
    receipts_before = set(root.glob('native-executor-restored-*.json'))
    systemctl('start', 'stillyard-delegation.service')
    systemctl('start', 'stillyard.service')
    until = time.monotonic() + 45
    while True:
        try:
            after = query('--endpoint', endpoint, 'daemon-status', '--deadline-seconds', '3')
            if after['machine_scheduling']['blocker'] is None:
                break
        except (subprocess.SubprocessError, OSError):
            pass
        if time.monotonic() >= until:
            raise RuntimeError('drained native restore did not become healthy')
        time.sleep(.2)
    save('after', after)
    receipts = set(root.glob('native-executor-restored-*.json')) - receipts_before
    if len(receipts) != 1:
        raise RuntimeError('expected exactly one durable kernel restore receipt')
    receipt = json.loads(receipts.pop().read_text())
    save('kernel-restoration', receipt)
    immutable = [root / name for name in ('config.json', 'native-linux/anchor.json',
                                          'native-linux/executor/anchor.json', 'authority/anchor.json')]
    if (after['store_uuid'] != before['store_uuid'] or after['daemon_generation'] == before['daemon_generation']
            or after['machine_scheduling']['domains'] != before['machine_scheduling']['domains']
            or after['machine_scheduling']['authority_epoch'] != before['machine_scheduling']['authority_epoch']
            or history.read_bytes() != original[history] or executors.stat().st_ino == old_inode
            or receipt['store_uuid'] != before['store_uuid'] or not receipt['history_preserved']
            or any(receipt[name] != anchor[name] for name in ('installation', 'domain', 'journal'))
            or any(path.read_bytes() != original[path] for path in immutable)
            or receipt['executor']['inode'] != executors.stat().st_ino
            or Path('/proc/sys/kernel/random/boot_id').read_text().strip() != boot):
        raise RuntimeError('restoration changed durable identity/history or retained the old kernel boundary')
    replay_job = canary('replay', key)
    if replay_job != original_job or marker.read_text() != 'launch\n':
        raise RuntimeError('restoration relaunched a durably completed submission')
    new_job = canary('after-restore', str(uuid.uuid4()))
    if new_job == original_job or marker.read_text() != 'launch\nlaunch\n':
        raise RuntimeError('fresh restored native canary did not launch exactly once')
    save('result', {'drained_runtime_loss_passed': True, 'same_boot': boot,
                    'original_job': original_job, 'replay_job': replay_job, 'new_job': new_job,
                    'missing_corrupt_controls': len(retained) * 2,
                    'remaining': ['changed boot', 'active-work SQL rollback and boundary loss']})
    print(directory, original_job, new_job, flush=True)


if __name__ == '__main__':
    main()
