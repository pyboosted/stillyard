#!/usr/bin/env python3
"""Quiescent installed-history corruption control on a disposable native VM.

Retains the service delegation, corrupts only the executor checksum, observes
an actual failed restart, restores exact bytes, and runs a native canary Job.
This is not an active-work SQL rollback or lost-boundary acceptance case.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import signal
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
        parser.error('history fault requires a separately selected unmanaged native VM')
    os.umask(0o077)
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    cli = root / 'bin/stillyard'
    directory = args.evidence_directory.resolve() / ('native-history-' + uuid.uuid4().hex)
    directory.mkdir(parents=True)

    def save(name, value):
        (directory / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

    def query(*command):
        return json.loads(subprocess.check_output([str(cli), *command], timeout=15))

    def journal():
        return subprocess.check_output(['/usr/bin/journalctl', '--user', '-u', 'stillyard.service',
                                        '--no-pager', '-o', 'cat'], text=True, timeout=10)

    def replace(path, data, mode):
        temporary = path.with_name('.history-control-' + uuid.uuid4().hex)
        with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode), 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    before = query('daemon-status')
    if (Path(before['store_path']) != root or before['running_jobs'] or before['queued_jobs']
            or before['machine_scheduling']['mode'] not in ('standalone', 'coordinator')
            or before['machine_scheduling']['blocker'] is not None
            or any(r['granted'] or r['reserved'] for r in before['resources'].values() if isinstance(r, dict) and 'granted' in r)):
        raise RuntimeError('history control requires a healthy quiescent installed native default')
    endpoint = before['endpoint']
    save('before', before)
    history = root / 'native-linux/executor/state.json'
    original = history.read_bytes()
    state = json.loads(original)
    if not state['state']['records'] or any(record['seal'] is None for record in state['state']['records'].values()):
        raise RuntimeError('all retained executor obligations must be durably sealed before history fault')
    mode = history.stat().st_mode & 0o777
    if mode != 0o600 or history.is_symlink():
        raise RuntimeError('executor history must be a private regular file')
    (directory / 'original-executor.json').write_bytes(original)
    anchors = {str(p.relative_to(root)): p.read_bytes() for p in (root / 'native-linux').rglob('anchor.json')}
    if not anchors:
        raise RuntimeError('native anchor is missing')
    save('anchor-hashes', {name: hashlib.sha256(data).hexdigest() for name, data in anchors.items()})
    # No record, identity or authority field is changed by this mutant.
    checksum_key = 'sha256'
    if checksum_key not in state:
        raise RuntimeError('unrecognized executor checksum envelope')
    state[checksum_key] = '0' * 64
    damaged = (json.dumps(state, separators=(',', ':')) + '\n').encode()
    baseline = journal()
    descriptor = os.pidfd_open(before['pid'])
    corrupted = False
    try:
        proc = Path('/proc') / str(before['pid'])
        identity = before['process_identity']
        fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
        if (int(fields[19]) != identity['start_ticks'] or os.stat(proc / 'ns/pid').st_ino != identity['pid_namespace_inode']
                or Path('/proc/sys/kernel/random/boot_id').read_text().strip() != identity['boot_id']
                or os.stat(proc).st_uid != identity['uid'] or identity['uid'] != os.geteuid()
                or not os.path.samefile(proc / 'exe', cli) or select.select([descriptor], [], [], 0)[0]):
            raise RuntimeError('history fault daemon identity changed')
        # Pin the process out of execution before touching its quiescent journal.
        signal.pidfd_send_signal(descriptor, signal.SIGSTOP)
        until = time.monotonic() + 5
        while (proc / 'stat').read_text().rsplit(')', 1)[1].split()[0] not in ('T', 't'):
            if time.monotonic() >= until:
                raise RuntimeError('daemon did not stop before history fault')
            time.sleep(.01)
        if history.read_bytes() != original:
            raise RuntimeError('executor history changed before fault')
        corrupted = True
        replace(history, damaged, mode)
        signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        until = time.monotonic() + 45
        while True:
            output = journal()
            delta = output[len(baseline):] if output.startswith(baseline) else output
            if 'executor history/anchor is unknown or mismatched' in delta and 'native_linux_daemon_exited' in delta:
                break
            if time.monotonic() >= until:
                (directory / 'failed-service-journal.txt').write_text(output)
                raise RuntimeError('no actual executor-checksum startup rejection was observed')
            time.sleep(.5)
        (directory / 'failed-service-journal.txt').write_text(output)
        if history.read_bytes() != damaged or any((root / name).read_bytes() != data for name, data in anchors.items()):
            raise RuntimeError('startup silently repaired or replaced unknown native history')
        refused = subprocess.run([str(cli), '--endpoint', endpoint, 'daemon-status', '--deadline-seconds', '3'],
                                 capture_output=True, text=True, timeout=10)
        save('unavailable-client', {'returncode': refused.returncode, 'stdout': refused.stdout, 'stderr': refused.stderr})
        if refused.returncode == 0:
            raise RuntimeError('corrupt native executor history exposed a running daemon')
    finally:
        if corrupted:
            replace(history, original, mode)
        try:
            signal.pidfd_send_signal(descriptor, signal.SIGCONT)
        except ProcessLookupError:
            pass
        os.close(descriptor)
    until = time.monotonic() + 65
    while True:
        try:
            after = query('--endpoint', endpoint, 'daemon-status', '--deadline-seconds', '3')
            if after['machine_scheduling']['blocker'] is None:
                break
        except (subprocess.SubprocessError, OSError):
            pass
        if time.monotonic() >= until:
            raise RuntimeError('exact history restoration did not recover native daemon')
        time.sleep(.5)
    save('after', after)
    if (after['store_uuid'] != before['store_uuid'] or after['daemon_generation'] == before['daemon_generation']
            or after['machine_scheduling']['domains'] != before['machine_scheduling']['domains']
            or after['machine_scheduling']['authority_epoch'] != before['machine_scheduling']['authority_epoch']
            or history.read_bytes() != original):
        raise RuntimeError('restored runtime did not preserve native identities/history')
    key = str(uuid.uuid4())
    save('canary.spec', {'spec_version': 4, 'executable': '/usr/bin/python3',
                         'args': ['-c', "print('native-history-restored',flush=True)"],
                         'working_directory': str(directory), 'timeout_seconds': 10,
                         'resources': {'cargo_slots': 1}, 'labels': [{'key': 'gate', 'value': 'native-history-canary'}]})
    save('canary.intent', {'idempotency_key': key, 'endpoint': endpoint})
    subprocess.run([str(cli), '--endpoint', endpoint, 'ensure', '--spec', str(directory / 'canary.spec.json'),
                    '--idempotency-key', key, '--result-file', str(directory / 'canary.receipt.json'),
                    '--wait', '--deadline-seconds', '30'], check=True, timeout=40)
    job = json.loads((directory / 'canary.receipt.json').read_text())['receipt']['accepted']['job_id']
    status = query('--endpoint', endpoint, 'status', job)
    save('canary.status', status)
    if status['outcome'] != 'succeeded' or not status['allocations'] or any(a['state'] != 'released' for a in status['allocations']):
        raise RuntimeError('restored native history canary did not release its allocation')
    save('result', {'quiescent_corrupt_history_passed': True, 'restored_canary_job': job,
                    'remaining': ['active-work history loss', 'SQL rollback', 'pre-release fault boundaries']})
    print(directory, job, flush=True)


if __name__ == '__main__':
    main()
