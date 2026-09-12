#!/usr/bin/env python3
"""Bounded installed native Linux process acceptance through public Stillyard Jobs.

Run outside Jobs on the selected native host. No Cargo, service shutdown or
runtime reset. This first suite does not claim logout/reboot/fault/idle coverage.
"""
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
    args = parser.parse_args()
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel or os.environ.get('STILLYARD_JOB_ID'):
        parser.error('native acceptance controller requires an unmanaged native Linux host')
    os.umask(0o077)
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    cli = root / 'bin/stillyard'
    directory = args.evidence_directory.resolve() / ('native-processes-' + uuid.uuid4().hex)
    directory.mkdir(parents=True)

    def save(name, value):
        (directory / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

    def query(*command):
        return json.loads(subprocess.check_output([str(cli), *command], timeout=15))

    until = time.monotonic() + 30
    while True:
        try:
            before = query('daemon-status')
            if before['machine_scheduling']['mode'] == 'coordinator' and before['machine_scheduling']['blocker'] is None:
                break
        except (subprocess.SubprocessError, OSError, KeyError, TypeError):
            pass
        if time.monotonic() >= until:
            raise RuntimeError('installed native coordinator did not become healthy')
        time.sleep(.25)
    if Path(before['store_path']) != root or before['running_jobs'] or before['queued_jobs']:
        raise RuntimeError('native suite requires its installed default Store and empty initial queue')
    endpoint = before['endpoint']
    save('installation-before', {'daemon': before, 'binary_sha256': hashlib.sha256(cli.read_bytes()).hexdigest(),
                                 'kernel': kernel, 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()})
    save('doctor-before', query('doctor', '--json'))
    jobs = {}

    def submit(name, code, **changes):
        spec = {'spec_version': 4, 'executable': '/usr/bin/python3', 'args': ['-c', code],
                'working_directory': str(directory), 'timeout_seconds': 30,
                'environment': {'set': {'PATH': '/usr/bin:/bin'}},
                'resources': {'cargo_slots': 1},
                'labels': [{'key': 'project', 'value': 'stillyard'},
                           {'key': 'gate', 'value': 'native-process-' + name}]} | changes
        key = str(uuid.uuid4())
        save(name + '.spec', spec)
        save(name + '.intent', {'idempotency_key': key, 'endpoint': endpoint})
        command = [str(cli), '--endpoint', endpoint, 'ensure', '--spec', str(directory / (name + '.spec.json')),
                   '--idempotency-key', key, '--result-file', str(directory / (name + '.receipt.json')),
                   '--deadline-seconds', '30']
        with (directory / (name + '.client.stdout')).open('wb') as out, (directory / (name + '.client.stderr')).open('wb') as err:
            subprocess.run(command, stdout=out, stderr=err, check=True, timeout=40)
        job = json.loads((directory / (name + '.receipt.json')).read_text())['receipt']['accepted']['job_id']
        jobs[name] = job
        print(name, job, flush=True)
        return job

    def finish(name, expected='succeeded'):
        until = time.monotonic() + 75
        while True:
            state = query('status', jobs[name])
            if state['state'] == 'final':
                break
            if time.monotonic() >= until:
                save(name + '.timeout-status', state)
                raise RuntimeError(name + ' did not settle')
            time.sleep(.1)
        save(name + '.status', state)
        for stream in ('stdout', 'stderr'):
            save(name + '.' + stream, query('logs', jobs[name], '--stream', stream, '--json', '--limit', '1048576'))
        if state['outcome'] != expected:
            raise RuntimeError(name + ': expected ' + expected + ', observed ' + str(state['outcome']))
        for allocation in state['allocations']:
            if allocation['state'] != 'released' or allocation['grant_id'] != before['store_uuid'] + '~' + allocation['lease_id']:
                raise RuntimeError('native allocation was not the same released local Lease')
        return state

    submit('canary', "import os;print('native-canary',os.environ['STILLYARD_JOB_ID'],flush=True)")
    finish('canary')
    # Real overlapping Invocations share two local slots; a third waits.
    for name in ('parallel-a', 'parallel-b', 'parallel-c'):
        submit(name, "import time;print('started',flush=True);time.sleep(4)")
    parallel = [finish(name) for name in ('parallel-a', 'parallel-b', 'parallel-c')]
    intervals = sorted((s['started_unix_millis'], s['finished_unix_millis']) for s in parallel)
    if not (intervals[1][0] < intervals[0][1] and intervals[2][0] >= min(intervals[0][1], intervals[1][1])):
        raise RuntimeError('two-slot overlap/third-waits contract was not observed')
    submit('probe-and-postcondition', "print('primary-after-probe',flush=True)", conditions=[{
        'predicate': {'kind': 'probe', 'probe': {'executable': '/usr/bin/python3',
            'args': ['-c', "print('probe',flush=True)"], 'working_directory': str(directory),
            'resources': {'cargo_slots': 1}, 'timeout_seconds': 5, 'interval_seconds': 1, 'accepted_exit_codes': [0]}},
        'deadline': {'kind': 'relative', 'seconds': 20}, 'on_deadline': 'failed'}],
        postconditions=[{'executable': '/usr/bin/python3', 'args': ['-c', "print('postcondition',flush=True)"],
                         'accepted_exit_codes': [0]}])
    state = finish('probe-and-postcondition')
    roles = {invocation['role'] for attempt in state['attempts'] for invocation in attempt['invocations']}
    if roles != {'primary', 'probe', 'postcondition'}:
        raise RuntimeError('native primary/probe/postcondition roles were not all executed')
    submit('retry', "import pathlib,sys;p=pathlib.Path('retry-marker');first=not p.exists();p.write_text('seen');sys.exit(5 if first else 0)",
           retry={'max_attempts': 2, 'backoff_seconds': 0, 'retryable': ['process_failed']})
    if len(finish('retry')['attempts']) != 2:
        raise RuntimeError('retry did not create two Attempts')
    submit('timeout', 'import time;time.sleep(60)', timeout_seconds=1)
    finish('timeout', 'timed_out')
    submit('cancel', 'import time;time.sleep(60)')
    query('--endpoint', endpoint, 'cancel', jobs['cancel'])
    finish('cancel', 'canceled')
    save('installation-after', query('daemon-status'))
    save('doctor-after', query('doctor', '--json'))
    save('result', {'native_process_suite_passed': True, 'jobs': jobs,
                    'remaining': ['whole-tree descendant and managed-child controls', 'daemon crash and history faults',
                                  'logout/reboot and idle budget', 'persistent host consumers', 'container matrix']})


if __name__ == '__main__':
    main()
