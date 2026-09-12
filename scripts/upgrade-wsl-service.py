#!/usr/bin/env python3
"""Replace the installed lifetime helper under independent idle barriers.

Preserves binary, pairing and all executor history. The helper's signal controls
must have succeeded as a default-manager Job on these exact helper bytes.
"""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import select
import shutil
import sqlite3
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--helper', type=Path, required=True)
    parser.add_argument('--validation-job-id', required=True)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get('STILLYARD_JOB_ID'):
        parser.error('service maintenance must run outside its own manager')
    os.umask(0o077)
    module = importlib.util.spec_from_file_location('upgrade', Path(__file__).with_name('upgrade-wsl-daemon.py'))
    upgrade = importlib.util.module_from_spec(module)
    module.loader.exec_module(upgrade)
    root = Path.home() / '.local/share/stillyard'
    cli = root / 'bin/stillyard'
    helper = root / 'libexec/wsl-service.py'
    unit = Path.home() / '.config/systemd/user/stillyard.service'
    def query(*command):
        return json.loads(subprocess.check_output([str(cli), '--endpoint', str(root / 'stillyard-v6.sock'), *command], timeout=10))
    before = query('daemon-status')
    validation = query('status', args.validation_job_id)
    validated_helper = Path(validation['spec']['working_directory']) / 'wsl-service.py'
    expected = upgrade.digest(args.helper)
    if (before['store_path'] != str(root) or before['machine_scheduling']['mode'] != 'attached'
            or validation['state'] != 'final' or validation['outcome'] != 'succeeded'
            or validation['spec']['args'] != [str(validated_helper.with_name('test-wsl-service.py'))]
            or {'key': 'gate', 'value': 'python-supervisor-signal-controls'} not in validation['spec']['labels']
            or upgrade.digest(validated_helper) != expected):
        raise RuntimeError('default manager or exact helper validation differs')
    evidence = args.evidence_directory.resolve() / ('service-upgrade-' + uuid.uuid4().hex)
    evidence.mkdir(parents=True)
    def save(name, value):
        with (evidence / name).open('x') as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        upgrade.sync_directory(evidence)
    save('before.json', before)
    save('validation.json', validation)
    shutil.copyfile(helper, evidence / 'prior-helper.py')
    shutil.copyfile(unit, evidence / 'prior-unit.service')
    unit_text = unit.read_text().replace('Restart=on-failure', 'Restart=no')
    for key, value in [('OOMPolicy', 'continue'), ('TimeoutStopSec', 'infinity')]:
        lines = [line for line in unit_text.splitlines() if not line.startswith(key + '=')]
        position = lines.index('KillMode=process') + 1
        lines.insert(position, key + '=' + value)
        unit_text = '\n'.join(lines) + '\n'
    lock = os.open(root / 'upgrade.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    staged_helper = helper.with_name('.next-' + uuid.uuid4().hex)
    staged_unit = unit.with_name('.next-' + uuid.uuid4().hex)
    shutil.copyfile(args.helper, staged_helper)
    staged_unit.write_text(unit_text)
    if upgrade.digest(staged_helper) != expected:
        raise RuntimeError('staged helper changed')
    for staged in (staged_helper, staged_unit):
        with staged.open('rb') as stream:
            os.fsync(stream.fileno())
        upgrade.sync_directory(staged.parent)
    save('plan.json', {'helper_sha256': expected, 'validation_job_id': args.validation_job_id,
                       'binary_sha256': upgrade.digest(cli), 'unit_text': unit_text})
    print(evidence, flush=True)
    pidfd = os.pidfd_open(before['pid'])
    process = Path('/proc') / str(before['pid'])
    if ((process / 'exe').resolve() != cli
            or int((process / 'stat').read_text().rsplit(')', 1)[1].split()[19]) != before['process_identity']['start_ticks']):
        raise RuntimeError('daemon process identity changed')
    db = sqlite3.connect((root / 'stillyard.sqlite3').as_uri() + '?mode=rw', uri=True, timeout=5, isolation_level=None)
    stopped = False
    try:
        db.execute('BEGIN IMMEDIATE')
        barrier = upgrade.executor_barrier(root, db=db, store_uuid=before['store_uuid'])
        save('barrier.json', {'executor': barrier})
        stopped = True
        subprocess.run(['systemctl', '--user', 'stop', 'stillyard.service'], check=True, timeout=20)
        poll = select.poll(); poll.register(pidfd, select.POLLIN)
        if not poll.poll(5000):
            raise RuntimeError('old daemon did not exit')
        after_stop = upgrade.executor_barrier(root, allow_absent=True)
        if after_stop['journal_sha256'] != barrier['journal_sha256']:
            raise RuntimeError('journal changed after idle barrier')
        save('stopped-barrier.json', after_stop)
        for target, staged in [(helper, staged_helper), (unit, staged_unit)]:
            os.link(target, target.with_name(target.name + '.previous-' + uuid.uuid4().hex))
            upgrade.sync_directory(target.parent)
            os.replace(staged, target)
            upgrade.sync_directory(target.parent)
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True, timeout=15)
    finally:
        if db.in_transaction:
            db.execute('ROLLBACK')
        db.close(); os.close(pidfd)
        if stopped:
            # systemd 259 can report transient EBUSY after stopping the old
            # exec-only service. Retry only before a new MainPID exists.
            for attempt in range(5):
                start = subprocess.run(['systemctl', '--user', 'start', 'stillyard.service'], capture_output=True, timeout=15)
                save(f'start-{attempt}.json', {'exit_code': start.returncode, 'stderr': start.stderr.decode()})
                if start.returncode == 0:
                    break
                main_pid = int(subprocess.check_output(['systemctl', '--user', 'show', 'stillyard.service', '--property=MainPID', '--value']))
                if main_pid:
                    break
                upgrade.executor_barrier(root, allow_absent=True)
                time.sleep(1)
    end = time.monotonic() + 45
    while True:
        try:
            after = query('daemon-status')
            if after.get('machine_scheduling') and after['machine_scheduling']['blocker'] is None:
                break
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        if time.monotonic() >= end:
            raise RuntimeError('service did not recover; history and prior helper retained')
        time.sleep(.2)
    main_pid = int(subprocess.check_output(['systemctl', '--user', 'show', 'stillyard.service', '--property=MainPID', '--value']))
    if (after['store_uuid'] != before['store_uuid'] or after['daemon_generation'] == before['daemon_generation']
            or main_pid <= 0 or main_pid == after['pid']):
        raise RuntimeError('replacement is not a new supervised daemon with unchanged Store')
    save('after.json', after)
    save('installed.json', {'supervisor_pid': main_pid, 'daemon_pid': after['pid'], 'helper_sha256': upgrade.digest(helper)})
    os.close(lock)
    print(json.dumps({'passed': True, 'supervisor_pid': main_pid, 'daemon_pid': after['pid']}), flush=True)


if __name__ == '__main__':
    main()
