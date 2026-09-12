#!/usr/bin/env python3
"""Refuse restoration of unsealed work hidden by SQL/authority rollback.

Only on a disposable native VM. Restore exact durable bytes before restarting.
Bubblewrap kills user code when the daemon dies; that is not a durable seal.
Kernel-boundary destruction and changed-boot acceptance are separate controls.
"""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
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
        parser.error('active restoration fault requires a separately selected unmanaged native VM')
    os.umask(0o077)
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    cli = root / 'bin/stillyard'
    directory = args.evidence_directory.resolve() / ('native-active-restore-' + uuid.uuid4().hex)
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
            or before['machine_scheduling']['blocker'] is not None
            or before['machine_scheduling']['mode'] not in ('standalone', 'coordinator')):
        raise RuntimeError('active restore control requires a healthy drained native default')
    save('before', before)
    database = root / 'stillyard.sqlite3'
    authority = root / 'authority/registry.json'
    history = root / 'native-linux/executor/state.json'
    executors = Path(json.loads((root / 'native-linux/anchor.json').read_text())['configuration']['executor_cgroup'])
    # A consistent same-Store historical snapshot, not manufactured SQL rows.
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as source:
        with closing(sqlite3.connect(directory / 'drained.sqlite3')) as destination:
            source.backup(destination)
    drained_sql = (directory / 'drained.sqlite3').read_bytes()
    drained_authority = authority.read_bytes()
    key = str(uuid.uuid4())
    code = ("from pathlib import Path;import time; p=Path('launches.txt'); "
            "p.write_text((p.read_text() if p.exists() else '')+'launch\\n'); "
            "end=time.monotonic()+180\nwhile time.monotonic()<end:\n"
            " Path('heartbeat').write_text(str(time.monotonic_ns()));time.sleep(.05)")
    save('subject.spec', {'spec_version': 4, 'executable': '/usr/bin/python3', 'args': ['-c', code],
                          'working_directory': str(directory), 'resources': {'cargo_slots': 1},
                          'timeout_seconds': 200, 'labels': [{'key': 'gate', 'value': 'native-active-restore'}]})
    save('subject.intent', {'endpoint': endpoint, 'idempotency_key': key})
    command = [str(cli), '--endpoint', endpoint, 'ensure', '--spec', str(directory / 'subject.spec.json'),
               '--idempotency-key', key, '--result-file', str(directory / 'subject.receipt.json'),
               '--deadline-seconds', '30']
    result = subprocess.run(command, capture_output=True, text=True, timeout=40, check=True)
    (directory / 'subject-client.txt').write_text(result.stdout)
    job = json.loads((directory / 'subject.receipt.json').read_text())['receipt']['accepted']['job_id']
    until = time.monotonic() + 20
    while True:
        active = query('--endpoint', endpoint, 'status', job)
        if active['started_unix_millis'] is not None and (directory / 'heartbeat').exists():
            break
        if time.monotonic() >= until:
            raise RuntimeError('active restore subject never entered user code')
        time.sleep(.05)
    save('active', active)
    heartbeat = (directory / 'heartbeat').read_bytes()
    until = time.monotonic() + 5
    while (directory / 'heartbeat').read_bytes() == heartbeat:
        if time.monotonic() >= until:
            raise RuntimeError('fault did not target observed live user code')
        time.sleep(.05)
    systemctl('stop', 'stillyard.service')
    live_journal = history.read_bytes()
    record = json.loads(live_journal)['state']['records'][active['invocation_id']]
    if record['seal'] is not None or not Path(record['boundary']['path']).is_dir():
        raise RuntimeError('explicit daemon stop did not retain an unsealed Invocation')
    live_authority = authority.read_bytes()
    original_inode = executors.stat().st_ino
    original_files = {path: path.read_bytes() for suffix in ('', '-wal', '-shm', '-journal')
                      if (path := Path(str(database) + suffix)).exists()}

    def replace(path, data):
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600), 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())

    backups = {**{directory / ('live-' + path.name): data for path, data in original_files.items()},
               directory / 'live-authority.json': live_authority,
               directory / 'live-executor.json': live_journal,
               directory / 'drained-authority.json': drained_authority}
    for path, data in backups.items():
        replace(path, data)
    replace(directory / 'backup-manifest.json', (json.dumps({
        'store': str(root), 'endpoint': endpoint, 'executor': str(executors), 'executor_inode': original_inode,
        'files': {p.name: hashlib.sha256(data).hexdigest()
                                            for p, data in backups.items()}}, indent=2) + '\n').encode())
    descriptor = os.open(directory, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    def refused(name, expected):
        result = subprocess.run([str(cli), '--endpoint', endpoint, 'linux-restore-executors', '--store', str(root)],
                                capture_output=True, text=True, timeout=40)
        boundary = executors
        save(name, {'returncode': result.returncode, 'stderr': result.stderr, 'stdout': result.stdout,
                     'retained_executor_inode': boundary.stat().st_ino,
                     'kernel_events': (boundary / 'cgroup.events').read_text(),
                     'unsealed_invocation': active['invocation_id']})
        if result.returncode == 0 or expected not in result.stderr or history.read_bytes() != live_journal:
            raise RuntimeError('missing expected restoration refusal: ' + name + ': ' + result.stderr)
        if boundary.stat().st_ino != original_inode:
            raise RuntimeError('refusal changed the retained unsealed executor boundary')

    try:
        refused('active-sql-refused', 'requires fully drained standalone history')
        # No process has SQLite open after explicit service stop. Preserve
        # the whole live SQLite family, including WAL, and restore exact bytes.
        for suffix in ('', '-wal', '-shm', '-journal'):
            path = Path(str(database) + suffix)
            if path.exists():
                path.unlink()
        replace(database, drained_sql)
        refused('sql-rollback-authority-refused', 'requires quiescent standalone authority')
        if authority.read_bytes() != live_authority:
            raise RuntimeError('restore mutated live authority')
        replace(authority, drained_authority)
        refused('sql-authority-rollback-journal-refused', 'unsealed executor history forbids kernel restoration')
        if authority.read_bytes() != drained_authority or database.read_bytes() != drained_sql:
            raise RuntimeError('restore rewrote the installed rollback control')
        save('fault-identities', {'store_uuid': before['store_uuid'], 'job_id': job,
                                  'executor_inode': original_inode,
                                  'unsealed_journal_sha256': hashlib.sha256(live_journal).hexdigest()})
    finally:
        cleanup_errors = []
        try:
            replace(authority, live_authority)
        except OSError as error:
            cleanup_errors.append('restore live authority: ' + str(error))
        for suffix in ('', '-wal', '-shm', '-journal'):
            path = Path(str(database) + suffix)
            try:
                if path.exists():
                    path.unlink()
                if path in original_files:
                    replace(path, original_files[path])
                    if path.read_bytes() != original_files[path]:
                        raise RuntimeError('live SQLite restoration lost exact bytes')
            except (OSError, RuntimeError) as error:
                cleanup_errors.append(str(path) + ': ' + str(error))
        if cleanup_errors:
            save('blocked-cleanup', {'errors': cleanup_errors, 'daemon_remains_stopped': True})
            raise RuntimeError('fault cleanup incomplete; daemon stays stopped: ' + '; '.join(cleanup_errors))
        if history.read_bytes() != live_journal or executors.stat().st_ino != original_inode:
            raise RuntimeError('control failed to restore live durable/kernel history')
    systemctl('start', 'stillyard.service')
    until = time.monotonic() + 45
    while True:
        try:
            after = query('--endpoint', endpoint, 'daemon-status', '--deadline-seconds', '3')
            status = query('--endpoint', endpoint, 'status', job)
            record = json.loads(history.read_bytes())['state']['records'][active['invocation_id']]
            if (status['outcome'] == 'interrupted' and status['allocations']
                    and all(a['state'] == 'released' for a in status['allocations'])
                    and record['seal'] is not None and after['machine_scheduling']['blocker'] is None):
                break
        except (subprocess.SubprocessError, OSError):
            pass
        if time.monotonic() >= until:
            raise RuntimeError('exact live history restoration did not recover and release the subject')
        time.sleep(.2)
    save('after', after)
    save('subject.status', status)
    if (after['store_uuid'] != before['store_uuid']
            or after['machine_scheduling']['authority_epoch'] != before['machine_scheduling']['authority_epoch']
            or after['daemon_generation'] == before['daemon_generation']
            or Path(record['boundary']['path']).exists()):
        raise RuntimeError('active restoration recovery lost durable identity or cleanup')
    replay = command.copy()
    replay[replay.index('--result-file') + 1] = str(directory / 'replay.receipt.json')
    result = subprocess.run([*replay, '--wait'], capture_output=True, text=True, timeout=40)
    save('replay-client', {'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
    replay_job = json.loads((directory / 'replay.receipt.json').read_text())['receipt']['accepted']['job_id']
    if result.returncode != 23 or replay_job != job or (directory / 'launches.txt').read_text() != 'launch\n':
        raise RuntimeError('recovered interrupted subject replayed as new work')
    save('result', {'active_restoration_refusals_passed': True, 'job_id': job,
                    'controls': ['active SQL', 'SQL rollback', 'SQL and authority rollback'],
                    'remaining': ['destroyed kernel boundary', 'changed boot']})
    print(directory, job, flush=True)


if __name__ == '__main__':
    main()
