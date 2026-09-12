#!/usr/bin/env python3
"""Observe actual installed timer paths under bounded, reversible faults.

Run outside Jobs with both queues empty. Stop only the installed daemon's exact
interop proxy, temporarily park its reconnect alias, and retain one canary Job.
The alias and stopped proxy are restored in finally. No Cargo is invoked.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get('STILLYARD_JOB_ID'):
        parser.error('fault controller must live outside the tested manager')
    os.umask(0o077)
    root = Path.home() / '.local/share/stillyard'
    linux = str(root / 'bin/stillyard')
    windows = '/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe'
    directory = args.evidence_directory.resolve() / ('timer-controls-' + uuid.uuid4().hex)
    directory.mkdir(parents=True)
    shutil.copyfile(__file__, directory / 'controller.py')

    def save(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + '\n')

    def query(cli, *command):
        return json.loads(subprocess.check_output([cli, *command], timeout=15))

    def doctor():
        value = query(linux, 'doctor', '--json')
        counters = [c for c in value['checks'] if c['code'] == 'runtime_timer_expirations']
        if len(counters) != 1:
            raise RuntimeError('installed counters are unavailable')
        return value, json.loads(counters[0]['summary'])

    pair = {side: query(cli, 'daemon-status') for side, cli in [('linux', linux), ('windows', windows)]}
    if any(s['queued_jobs'] or s['running_jobs'] for s in pair.values()):
        raise RuntimeError('timer fault controls require both queues empty')
    if pair['linux']['machine_scheduling']['blocker'] is not None:
        raise RuntimeError('healthy paired manager required')
    save('pair-before.json', pair)
    before, counts = doctor()
    save('doctor-before.json', before)
    tail = ['--endpoint', pair['windows']['endpoint'], 'machine', 'bridge']
    proxies = {}
    for task in (Path('/proc') / str(pair['linux']['pid']) / 'task').iterdir():
        for child in (task / 'children').read_text().split():
            proc = Path('/proc') / child
            try:
                command = (proc / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
                if (command[-4:] == tail and command[1:3] == [windows] * 2
                        and (proc / 'exe').resolve() == Path('/init')):
                    proxies[int(child)] = int((proc / 'stat').read_text().rsplit(')', 1)[1].split()[19])
            except FileNotFoundError:
                pass
    if len(proxies) != 1:
        raise RuntimeError('could not identify one exact installed bridge proxy')
    pid, ticks = next(iter(proxies.items()))
    descriptor = os.pidfd_open(pid)
    proc = Path('/proc') / str(pid)
    if (proc.stat().st_uid != os.geteuid()
            or int((proc / 'stat').read_text().rsplit(')', 1)[1].split()[19]) != ticks):
        os.close(descriptor)
        raise RuntimeError('bridge identity changed before fault')
    alias = root / 'interop.sock'
    target = alias.readlink()
    parked = alias.with_name('.interop-timer-control-' + uuid.uuid4().hex)
    endpoint = pair['linux']['endpoint']

    def submit(name, spec):
        key = str(uuid.uuid4())
        save(name + '.spec.json', spec)
        save(name + '.intent.json', {'idempotency_key': key})
        with (directory / (name + '.client.stdout')).open('wb') as out, (directory / (name + '.client.stderr')).open('wb') as err:
            subprocess.run([linux, '--endpoint', endpoint, 'ensure', '--spec',
                            str(directory / (name + '.spec.json')), '--idempotency-key', key,
                            '--result-file', str(directory / (name + '.receipt.json')),
                            '--deadline-seconds', '30'], stdout=out, stderr=err, check=True)
        return json.loads((directory / (name + '.receipt.json')).read_text())['receipt']['accepted']['job_id']

    base_spec = {'spec_version': 4, 'executable': '/usr/bin/true',
                 'working_directory': str(directory), 'timeout_seconds': 30,
                 'labels': [{'key': 'project', 'value': 'stillyard'},
                            {'key': 'gate', 'value': 'installed-timer-controls'}]}
    alias.rename(parked)
    try:
        signal.pidfd_send_signal(descriptor, signal.SIGSTOP)
        save('fault.json', {'proxy_pid': pid, 'start_ticks': ticks, 'interop_target': str(target),
                           'action': 'SIGSTOP exact proxy; park reconnect alias', 'unix_ns': time.time_ns()})
        job = submit('transport-canary', base_spec | {'resources': {'cargo_slots': 1}})
        print(directory, job, flush=True)
        until = time.monotonic() + 45
        while True:
            value, current = doctor()
            if current['transport'] > counts['transport'] and current['backoff'] >= counts['backoff'] + 2:
                save('doctor-fault.json', value)
                break
            if time.monotonic() >= until:
                save('doctor-fault-timeout.json', value)
                raise RuntimeError('actual transport/backoff timers did not become observable')
            time.sleep(.2)
        pending = query(linux, 'status', job)
        save('pending-canary.json', pending)
        if pending['started_unix_millis'] is not None or pending['state'] == 'final':
            raise RuntimeError('canary did not remain pending through the transport fault')
    finally:
        try:
            if parked.is_symlink():
                if alias.exists() or alias.is_symlink():
                    raise RuntimeError('interop alias changed during fault; retained parked alias')
                parked.rename(alias)
        finally:
            try:
                signal.pidfd_send_signal(descriptor, signal.SIGCONT)
            except ProcessLookupError:
                pass  # Driver may already have killed/reaped its timed-out proxy.
            finally:
                os.close(descriptor)
    until = time.monotonic() + 60
    while True:
        final = query(linux, 'status', job)
        if (final['state'] == 'final' and final['allocations']
                and all(a['state'] == 'released' for a in final['allocations'])):
            break
        if time.monotonic() >= until:
            raise RuntimeError('same canary receipt did not reconcile after bridge recovery')
        time.sleep(.2)
    save('transport-final.json', final)
    if final['outcome'] != 'succeeded':
        raise RuntimeError('recovered canary did not succeed')
    # A deadline-driven pending condition exercises the real reactor. A wait
    # subscriber on that same Job exercises the real subscription timeout path.
    prior, prior_counts = doctor()
    save('doctor-before-condition.json', prior)
    condition = submit('condition-canary', base_spec | {'conditions': [
        {'predicate': {'kind': 'path_exists', 'path': str(directory / 'never-created')},
         'deadline': {'kind': 'relative', 'seconds': 5}, 'on_deadline': 'failed'}]})
    with (directory / 'subscriber.stdout').open('wb') as out, (directory / 'subscriber.stderr').open('wb') as err:
        waited = subprocess.run([linux, '--endpoint', endpoint, 'wait', condition,
                                 '--deadline-seconds', '2'], stdout=out, stderr=err)
    save('subscriber-exit.json', {'exit_code': waited.returncode, 'job_id': condition})
    until = time.monotonic() + 15
    while True:
        state = query(linux, 'status', condition)
        if state['state'] == 'final':
            break
        if time.monotonic() >= until:
            raise RuntimeError('condition deadline did not settle')
        time.sleep(.2)
    after, counters = doctor()
    save('condition-final.json', state)
    save('doctor-after.json', after)
    if (after['daemon']['daemon_generation'] != before['daemon']['daemon_generation']
            or state['outcome'] != 'failed' or state['started_unix_millis'] is not None
            or counters['reactor'] <= prior_counts['reactor']
            or counters['subscriber'] <= prior_counts['subscriber']):
        raise RuntimeError('actual reactor/subscriber timer control failed')
    save('result.json', {'passed': True, 'transport_job': job, 'condition_job': condition,
                         'delta': {key: value - counts[key] for key, value in counters.items()},
                         'scope': 'actual installed Linux daemon product wait paths; not an idle interval'})
    print('Installed timer controls passed', flush=True)


if __name__ == '__main__':
    main()
