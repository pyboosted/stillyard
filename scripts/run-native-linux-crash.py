#!/usr/bin/env python3
"""Crash only the exact installed daemon on a selected disposable native host.

The native user-service supervisor and delegated executor cgroups remain alive.
The retained Job must become interrupted with a durable whole-boundary seal,
its allocation released, and its original submission identity preserved.
Never use this controller on WSL or inside a managed Job.
"""
import argparse
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
        parser.error('crash subject must be a separately selected unmanaged native host')
    os.umask(0o077)
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    cli = root / 'bin/stillyard'
    directory = args.evidence_directory.resolve() / ('native-crash-' + uuid.uuid4().hex)
    directory.mkdir(parents=True)

    def save(name, value):
        (directory / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

    def query(*command):
        return json.loads(subprocess.check_output([str(cli), *command], timeout=15))

    before = query('daemon-status')
    if (Path(before['store_path']) != root or before['running_jobs'] or before['queued_jobs']
            or before['machine_scheduling']['mode'] not in ('standalone', 'coordinator')
            or before['machine_scheduling']['blocker'] is not None):
        raise RuntimeError('crash test needs one healthy installed native default with no other Jobs')
    endpoint = before['endpoint']
    save('before', before)
    unit = subprocess.check_output(['/usr/bin/systemctl', '--user', 'show', 'stillyard.service',
                                    '--property=MainPID', '--value'], text=True, timeout=10)
    supervisor = int(unit.strip())
    delegation = None
    if supervisor == before['pid']:
        def delegation_snapshot():
            state = subprocess.check_output(['/usr/bin/systemctl', '--user', 'show', 'stillyard-delegation.service',
                                              '--property=ActiveState', '--property=SubState', '--property=MainPID',
                                              '--property=ControlGroup'], text=True, timeout=10)
            values = dict(line.split('=', 1) for line in state.splitlines())
            group = Path('/sys/fs/cgroup') / values['ControlGroup'].lstrip('/')
            if values['ActiveState'] != 'active' or values['SubState'] != 'exited' or values['MainPID'] != '0':
                raise RuntimeError('native delegation unit needs active/exited state without a helper')
            return {'unit': values, 'group_inode': group.stat().st_ino,
                    'executor_inode': (group / 'executors').stat().st_ino}
        delegation = delegation_snapshot()
        save('delegation-before', delegation)
    elif supervisor <= 1:
        raise RuntimeError('a distinct live delegation supervisor is required')
    subject = '/proc/' + str(before['pid'])
    descriptor = os.pidfd_open(before['pid'])
    try:
        def assert_subject():
            identity = before['process_identity']
            fields = Path(subject + '/stat').read_text().rsplit(')', 1)[1].split()
            if (identity['platform'] != 'linux' or identity['pid'] != before['pid']
                    or identity['start_ticks'] != int(fields[19])
                    or identity['boot_id'] != Path('/proc/sys/kernel/random/boot_id').read_text().strip()
                    or identity['pid_namespace_inode'] != os.stat(subject + '/ns/pid').st_ino
                    or identity['uid'] != os.stat(subject).st_uid or identity['uid'] != os.geteuid()
                    or not os.path.samefile(subject + '/exe', cli)
                    or select.select([descriptor], [], [], 0)[0]):
                raise RuntimeError('pinned daemon identity changed before crash test')

        assert_subject()
        key = str(uuid.uuid4())
        child = "import pathlib,time;p=pathlib.Path('heartbeat');end=time.monotonic()+60\nwhile time.monotonic()<end:\n p.write_text(str(time.monotonic_ns()));time.sleep(.05)"
        code = ("import pathlib,subprocess,time; "
                "p=pathlib.Path('launch-count');p.write_text(str(int(p.read_text())+1) if p.exists() else '1'); "
                f"subprocess.Popen(['/usr/bin/python3','-c',{child!r}],start_new_session=True); "
                "print('native-crash-subject-started',flush=True);time.sleep(60)")
        spec = {'spec_version': 4, 'executable': '/usr/bin/python3', 'args': ['-c', code],
                'working_directory': str(directory), 'resources': {'cargo_slots': 1},
                'timeout_seconds': 90, 'labels': [{'key': 'project', 'value': 'stillyard'},
                                                {'key': 'gate', 'value': 'native-daemon-crash'}]}
        save('subject.spec', spec)
        save('subject.intent', {'idempotency_key': key, 'endpoint': endpoint})
        command = [str(cli), '--endpoint', endpoint, 'ensure', '--spec', str(directory / 'subject.spec.json'),
                   '--idempotency-key', key, '--result-file', str(directory / 'subject.receipt.json'),
                   '--deadline-seconds', '30']
        with (directory / 'subject.client.stdout').open('wb') as out, (directory / 'subject.client.stderr').open('wb') as err:
            subprocess.run(command, stdout=out, stderr=err, check=True, timeout=40)
        job = json.loads((directory / 'subject.receipt.json').read_text())['receipt']['accepted']['job_id']
        until = time.monotonic() + 20
        while True:
            state = query('--endpoint', endpoint, 'status', job)
            if state['started_unix_millis'] is not None and (directory / 'heartbeat').exists():
                break
            if time.monotonic() >= until:
                raise RuntimeError('native crash subject never reached live user code')
            time.sleep(.05)
        save('active', state)
        current = query('--endpoint', endpoint, 'daemon-status')
        if (current['daemon_generation'] != before['daemon_generation']
                or current['process_identity'] != before['process_identity']
                or state['daemon_generation'] != before['daemon_generation'] or state['state'] == 'final'):
            raise RuntimeError('fault no longer targets the observed active daemon generation')
        assert_subject()
        save('crash', {'job_id': job, 'daemon_pid': before['pid'], 'supervisor_pid': supervisor,
                       'daemon_generation': before['daemon_generation'], 'signal': 'SIGKILL'})
        signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        until = time.monotonic() + 45
        timeline = []
        while True:
            try:
                after = query('--endpoint', endpoint, 'daemon-status', '--deadline-seconds', '3')
                state = query('--endpoint', endpoint, 'status', job)
                timeline.append({'generation': after['daemon_generation'], 'state': state['state'],
                                 'outcome': state['outcome'], 'allocations': [a['state'] for a in state['allocations']]})
                if (after['daemon_generation'] != before['daemon_generation'] and state['state'] == 'final'
                        and state['allocations'] and all(a['state'] == 'released' for a in state['allocations'])
                        and after['machine_scheduling']['blocker'] is None):
                    journal = (root / 'native-linux/executor/state.json').read_bytes()
                    record = json.loads(journal)['state']['records'][state['invocation_id']]
                    if record['seal'] is not None:
                        break
            except (subprocess.SubprocessError, OSError):
                pass
            if time.monotonic() >= until:
                save('recovery-timeline', timeline)
                raise RuntimeError('native crash recovery did not settle the retained Job')
            time.sleep(.2)
        save('recovery-timeline', timeline)
        save('after', after)
        save('subject.status', state)
        for stream in ('stdout', 'stderr'):
            save('subject.' + stream, query('logs', job, '--stream', stream, '--json', '--limit', '1048576'))
        if (after['store_uuid'] != before['store_uuid'] or state['outcome'] != 'interrupted'
                or after['machine_scheduling']['authority_epoch'] != before['machine_scheduling']['authority_epoch']
                or after['machine_scheduling']['blocker'] is not None
                or not state['allocations'] or any(a['state'] != 'released' for a in state['allocations'])):
            raise RuntimeError('native recovery changed identity, lost interruption or retained an unproven debit')
        current_supervisor = int(subprocess.check_output(['/usr/bin/systemctl', '--user', 'show', 'stillyard.service',
                                                        '--property=MainPID', '--value'], text=True, timeout=10))
        if delegation is None:
            if current_supervisor != supervisor:
                raise RuntimeError('delegation supervisor restarted during daemon-only crash')
        else:
            current_delegation = delegation_snapshot()
            save('delegation-after', current_delegation)
            if current_supervisor != after['pid'] or current_delegation != delegation:
                raise RuntimeError('retained delegation changed during native daemon restart')
        journal = (root / 'native-linux/executor/state.json').read_bytes()
        (directory / 'executor-journal.json').write_bytes(journal)
        record = json.loads(journal)['state']['records'][state['invocation_id']]
        if record['seal'] is None or not record['seal']['possibly_released'] or Path(record['boundary']['path']).exists():
            raise RuntimeError('crashed Invocation has no durable whole-boundary cleanup')
        heartbeat = (directory / 'heartbeat').read_bytes()
        time.sleep(.3)
        if heartbeat != (directory / 'heartbeat').read_bytes():
            raise RuntimeError('a descendant survived daemon crash recovery')
        # Same durable logical operation, explicit endpoint, a separate receipt.
        replay = command.copy()
        replay[replay.index('--result-file') + 1] = str(directory / 'replay.receipt.json')
        replay.append('--wait')
        with (directory / 'replay.client.stdout').open('wb') as out, (directory / 'replay.client.stderr').open('wb') as err:
            replayed = subprocess.run(replay, stdout=out, stderr=err, timeout=40)
        save('replay.client-exit', {'exit_code': replayed.returncode, 'expected_interrupted_exit': 23})
        if replayed.returncode != 23:
            raise RuntimeError('replayed interrupted Job did not preserve its terminal outcome')
        retained = json.loads((directory / 'replay.receipt.json').read_text())['receipt']['accepted']['job_id']
        if retained != job or (directory / 'launch-count').read_text() != '1':
            raise RuntimeError('crashed submission was replayed as new user work')
        save('result', {'daemon_crash_recovery_passed': True, 'job_id': job,
                        'old_generation': before['daemon_generation'], 'new_generation': after['daemon_generation'],
                        'supervisor_pid': supervisor if delegation is None else None,
                        'retained_delegation_unit': delegation, 'remaining': ['pre-release fault boundaries',
                            'native unknown-history controls', 'service/logout/reboot lifecycle']})
        print(directory, job, flush=True)
    finally:
        os.close(descriptor)


if __name__ == '__main__':
    main()
