#!/usr/bin/env python3
"""Force reversible rmdir failure on one owned live Invocation boundary.

A trusted external controller adds an empty child cgroup; user code cannot do
this through its read-only cgroup mount. Remove only that exact empty fault
child, leaving the executor to seal its own boundary and release its Grant.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    parser.add_argument('--windows-evidence-directory', type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get('STILLYARD_JOB_ID'):
        parser.error('fault controller must be outside the tested manager')
    os.umask(0o077)
    root = Path.home() / '.local/share/stillyard'
    linux = str(root / 'bin/stillyard')
    windows = '/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe'
    name = 'cleanup-failure-' + uuid.uuid4().hex
    directory = args.evidence_directory.resolve() / name
    native = args.windows_evidence_directory.resolve() / name
    directory.mkdir(parents=True); native.mkdir(parents=True)
    def save(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + '\n')
    def query(cli, *command):
        return json.loads(subprocess.check_output([cli, *command], timeout=15))
    def winpath(path):
        return subprocess.check_output(['wslpath', '-w', str(path)], text=True).strip()
    def wait_status(cli, job, predicate, seconds):
        end = time.monotonic() + seconds
        while True:
            value = query(cli, 'status', job)
            if predicate(value):
                return value
            if time.monotonic() >= end:
                save('timeout-status.json', value)
                raise RuntimeError('acceptance bound exceeded; retained Job requires diagnosis')
            time.sleep(.1)
    before = query(linux, 'daemon-status')
    native_before = query(windows, 'daemon-status')
    save('pair-before.json', {'linux': before, 'windows': native_before})
    marker = directory / 'running'
    finish = directory / 'finish'
    code = ('import pathlib,subprocess,time; '
            "subprocess.Popen(['/usr/bin/python3','-c','import time;time.sleep(120)']); "
            f'pathlib.Path({str(marker)!r}).write_text("running"); '
            f'p=pathlib.Path({str(finish)!r})\n'
            'while not p.exists(): time.sleep(.02)\n'
            'print("root exits leaving descendant",flush=True)')
    def submit(cli, endpoint, spec, folder, path_fn):
        (folder / 'spec.json').write_text(json.dumps(spec, indent=2))
        key = str(uuid.uuid4())
        (folder / 'intent.json').write_text(json.dumps({'idempotency_key': key}))
        with (folder / 'client.stdout').open('wb') as out, (folder / 'client.stderr').open('wb') as err:
            subprocess.run([cli, '--endpoint', endpoint, 'ensure', '--spec', path_fn(folder / 'spec.json'),
                            '--idempotency-key', key, '--result-file', path_fn(folder / 'receipt.json'),
                            '--deadline-seconds', '30'], stdout=out, stderr=err, check=True)
        return json.loads((folder / 'receipt.json').read_text())['receipt']['accepted']['job_id']
    labels = [{'key': 'project', 'value': 'stillyard'}, {'key': 'gate', 'value': 'installed-cleanup-failure'}]
    job = submit(linux, before['endpoint'], {'spec_version': 4, 'executable': '/usr/bin/python3',
                 'args': ['-c', code], 'working_directory': str(directory), 'resources': {'cargo_slots': 1},
                 'timeout_seconds': 120, 'labels': labels}, directory, str)
    print(directory, job, flush=True)
    wait_status(linux, job, lambda s: marker.exists() and s['started_unix_millis'] is not None, 30)
    running = query(linux, 'status', job); save('running.json', running)
    invocation = running['attempts'][0]['invocations'][0]['invocation_id']
    journal = root / 'attachment/executor/state.json'
    record = json.loads(journal.read_text())['state']['records'][invocation]
    boundary = Path(record['boundary']['path'])
    if record['seal'] is not None or len((boundary / 'cgroup.procs').read_text().split()) < 2:
        raise RuntimeError('required live root and descendant not observed')
    save('executor-before.json', record)
    fault = boundary / ('acceptance-fault-' + uuid.uuid4().hex)
    fault.mkdir()
    identity = fault.stat()
    save('fault.json', {'path': str(fault), 'device': identity.st_dev, 'inode': identity.st_ino})
    try:
        windows_job = submit(windows, native_before['endpoint'], {'spec_version': 4,
            'executable': r'C:\Users\User\AppData\Local\Programs\Python\Python313\python.exe',
            'args': ['-c', 'print("shared token released only after Linux cleanup")'],
            'working_directory': winpath(native), 'resources': {'cargo_slots': 1},
            'timeout_seconds': 30, 'labels': labels}, native, winpath)
        finish.write_text('exit')
        failed = wait_status(linux, job, lambda s: any(
            i['containment']['state'] == 'uncertain' for a in s['attempts'] for i in a['invocations']), 30)
        save('cleanup-failed.json', failed)
        retained = json.loads(journal.read_text())['state']['records'][invocation]
        queued = query(windows, 'status', windows_job)
        events = dict(line.split() for line in (boundary / 'cgroup.events').read_text().splitlines())
        save('retained.json', {'executor': retained, 'windows_job': queued, 'cgroup_events': events})
        if (retained['seal'] is not None or events['populated'] != '0' or queued['state'] != 'pending'
                or not failed['allocations'] or any(a['state'] == 'released' for a in failed['allocations'])):
            raise RuntimeError('cleanup failure did not retain the original global debit')
    finally:
        metadata = fault.stat()
        events = dict(line.split() for line in (fault / 'cgroup.events').read_text().splitlines())
        if (metadata.st_dev, metadata.st_ino) != (identity.st_dev, identity.st_ino) or events['populated'] != '0':
            raise RuntimeError('fault child changed identity or became populated; retained for diagnosis')
        fault.rmdir()
        finish.write_text('exit')
        save('fault-removed.json', {'removed_empty_child': str(fault), 'unix_ns': time.time_ns()})
    final = wait_status(linux, job, lambda s: bool(s['allocations']) and all(a['state'] == 'released' for a in s['allocations']), 60)
    windows_final = wait_status(windows, windows_job, lambda s: s['state'] == 'final', 30)
    seal = json.loads(journal.read_text())['state']['records'][invocation]
    save('final.json', final); save('windows-final.json', windows_final); save('executor-after.json', seal)
    if seal['seal'] is None or boundary.exists() or windows_final['outcome'] != 'succeeded':
        raise RuntimeError('automatic reconciliation did not produce exact-boundary cleanup')
    save('verdict.json', {'passed': True, 'linux_job': job, 'windows_job': windows_job})
    print(json.dumps({'passed': True, 'linux_job': job, 'windows_job': windows_job}), flush=True)


if __name__ == '__main__':
    main()
