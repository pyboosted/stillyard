#!/usr/bin/env python3
"""Final destructive unsealed-boundary control on a disposable native VM.

Run after all healthy consumers and idle observations. Stops only the selected
native daemon, removes its empty kernel executor tree without creating a seal,
and requires restoration to refuse while original durable rights remain.
The daemon intentionally stays stopped; this VM has no further consumer work.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from contextlib import closing
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--disposable-native-host', action='store_true', required=True)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    args = parser.parse_args()
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel or os.environ.get('STILLYARD_JOB_ID'):
        parser.error('boundary loss requires a separately selected unmanaged native VM')
    os.umask(0o077)
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    cli = root / 'bin/stillyard'
    directory = args.evidence_directory.resolve() / ('native-boundary-loss-' + uuid.uuid4().hex)
    directory.mkdir(parents=True)

    def save(name, value):
        (directory / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

    def query(*command):
        return json.loads(subprocess.check_output([str(cli), *command], timeout=15))

    before = query('daemon-status')
    endpoint = before['endpoint']
    if (Path(before['store_path']) != root or before['running_jobs'] or before['queued_jobs']
            or before['machine_scheduling']['blocker'] is not None
            or before['machine_scheduling']['mode'] not in ('standalone', 'coordinator')):
        raise RuntimeError('terminal fault requires the healthy drained native default')
    save('before', before)
    database = root / 'stillyard.sqlite3'
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as source:
        with closing(sqlite3.connect(directory / 'drained.sqlite3')) as destination:
            source.backup(destination)
    authority = root / 'authority/registry.json'
    drained_authority = authority.read_bytes()
    history = root / 'native-linux/executor/state.json'
    anchor = json.loads((root / 'native-linux/anchor.json').read_text())['configuration']
    executors = Path(anchor['executor_cgroup'])
    expected = Path(f'/sys/fs/cgroup/user.slice/user-{os.geteuid()}.slice/user@{os.geteuid()}.service/app.slice/stillyard-delegation.service/executors')
    if executors != expected or executors.resolve(strict=True) != expected or executors.stat().st_uid != os.geteuid():
        raise RuntimeError('terminal fault executor differs from the selected installed native path')
    key = str(uuid.uuid4())
    save('subject.spec', {'spec_version': 4, 'executable': '/usr/bin/python3',
                          'args': ['-c', "from pathlib import Path;import time;Path('started').write_text('started');time.sleep(180)"],
                          'working_directory': str(directory), 'resources': {'cargo_slots': 1},
                          'timeout_seconds': 200, 'labels': [{'key': 'gate', 'value': 'native-terminal-boundary-loss'}]})
    save('subject.intent', {'endpoint': endpoint, 'idempotency_key': key})
    subprocess.run([str(cli), '--endpoint', endpoint, 'ensure', '--spec', str(directory / 'subject.spec.json'),
                    '--idempotency-key', key, '--result-file', str(directory / 'subject.receipt.json'),
                    '--deadline-seconds', '30'], capture_output=True, check=True, timeout=40)
    job = json.loads((directory / 'subject.receipt.json').read_text())['receipt']['accepted']['job_id']
    until = time.monotonic() + 20
    while not (directory / 'started').exists():
        if time.monotonic() >= until:
            raise RuntimeError('terminal subject did not start')
        time.sleep(.05)
    active = query('--endpoint', endpoint, 'status', job)
    save('subject.status-before-fault', active)
    if active['state'] != 'active' or not active['allocations'] or all(a['state'] == 'released' for a in active['allocations']):
        raise RuntimeError('terminal fault needs actual active native rights')
    subprocess.run(['/usr/bin/systemctl', '--user', 'stop', 'stillyard.service'], check=True, timeout=45)
    immutable = [root / name for name in ('native-linux/anchor.json', 'native-linux/executor/anchor.json',
                                          'authority/anchor.json', 'config.json')]
    originals = {path: path.read_bytes() for path in [database, authority, history, *immutable]}
    for suffix in ('-wal', '-shm', '-journal'):
        path = Path(str(database) + suffix)
        if path.exists():
            originals[path] = path.read_bytes()
    for path, data in originals.items():
        with (directory / ('original-' + str(path.relative_to(root)).replace('/', '-'))).open('wb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
    for path in (directory, directory.parent):
        descriptor = os.open(path, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    records = json.loads(originals[history])['state']['records']
    if any(record['seal'] is None for invocation, record in records.items() if invocation != active['invocation_id']):
        raise RuntimeError('terminal fault refuses to remove another unsealed Invocation')
    record = records[active['invocation_id']]
    if record['seal'] is not None:
        raise RuntimeError('terminal fault lost its unsealed obligation')
    until = time.monotonic() + 10
    while 'populated 0' not in (executors / 'cgroup.events').read_text().splitlines():
        if time.monotonic() >= until:
            raise RuntimeError('terminal fault refuses to remove a populated executor')
        time.sleep(.05)
    directories = [p for p in executors.rglob('*') if p.is_dir()]
    inodes = {str(p): p.stat().st_ino for p in [executors, *directories]}
    for path in sorted(directories, key=lambda p: len(p.parts), reverse=True):
        if path.is_symlink() or path.stat().st_uid != os.geteuid():
            raise RuntimeError('unsafe descendant during empty kernel fault')
        path.rmdir()
    executors.rmdir()
    save('removed-kernel', {'inodes': inodes, 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                             'seal_published': False})
    try:
        # Same-Store rollback hides active SQL and authority from their own
        # gates. The original journal must independently refuse an absent root.
        for suffix in ('', '-wal', '-shm', '-journal'):
            path = Path(str(database) + suffix)
            if path.exists():
                path.unlink()
        drained_sql = (directory / 'drained.sqlite3').read_bytes()
        database.write_bytes(drained_sql)
        authority.write_bytes(drained_authority)
        result = subprocess.run([str(cli), '--endpoint', endpoint, 'linux-restore-executors', '--store', str(root)],
                                capture_output=True, text=True, timeout=40)
        save('absent-unsealed-refused', {'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
        if (result.returncode == 0 or 'unsealed executor history forbids kernel restoration' not in result.stderr
                or executors.exists() or history.read_bytes() != originals[history]):
            raise RuntimeError('missing kernel boundary bypassed the retained unsealed journal')
        if (database.read_bytes() != drained_sql or authority.read_bytes() != drained_authority
                or any(path.read_bytes() != originals[path] for path in immutable)):
            raise RuntimeError('refusal rewrote retained durable inputs')
        # SQLite may create shared-memory bookkeeping and an empty WAL when
        # reading a WAL-mode snapshot. No transaction or rollback journal may
        # be written by this read-only restoration path.
        for suffix in ('-wal', '-journal'):
            path = Path(str(database) + suffix)
            if path.exists() and path.stat().st_size:
                raise RuntimeError('refusal wrote a SQL transaction sidecar')
    finally:
        errors = []
        for suffix in ('', '-wal', '-shm', '-journal'):
            path = Path(str(database) + suffix)
            try:
                if path.exists():
                    path.unlink()
            except OSError as error:
                errors.append(str(path) + ': ' + str(error))
        for path, data in originals.items():
            try:
                with path.open('wb') as stream:
                    stream.write(data); stream.flush(); os.fsync(stream.fileno())
            except OSError as error:
                errors.append(str(path) + ': ' + str(error))
        try:
            descriptor = os.open(root, os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            errors.append('Store directory fsync: ' + str(error))
        if errors:
            save('blocked-cleanup', {'errors': errors, 'daemon_intentionally_stopped': True})
            raise RuntimeError('terminal fault durable restoration failed: ' + '; '.join(errors))
    if executors.exists() or any(path.read_bytes() != data for path, data in originals.items()):
        raise RuntimeError('terminal fault did not retain exact outstanding durable rights')
    save('result', {'missing_unsealed_boundary_refused': True, 'job_id': job,
                    'invocation_id': active['invocation_id'], 'daemon_intentionally_stopped': True,
                    'allocation_released': False, 'seal_published': False,
                    'journal_sha256': hashlib.sha256(originals[history]).hexdigest(),
                    'remaining': ['changed boot and operator recovery after lost boundaries']})
    print(directory, job, flush=True)


if __name__ == '__main__':
    main()
