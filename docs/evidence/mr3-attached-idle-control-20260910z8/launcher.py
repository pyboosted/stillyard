#!/usr/bin/env python3
"""Exercise the real attached wait counter using a validated, prebuilt test binary.

The polling control runs alone as a default attached Stillyard Job. Success means
the deliberately short wait exceeded the idle budget and was correctly counted.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', required=True, type=Path)
    parser.add_argument('--source-manifest', required=True, type=Path)
    parser.add_argument('--test-job', required=True)
    parser.add_argument('--evidence-directory', required=True, type=Path)
    parser.add_argument('--resume-evidence', type=Path)
    args = parser.parse_args()
    cli = str(Path.home() / '.local/share/stillyard/bin/stillyard')

    def query(*command):
        return json.loads(subprocess.check_output([cli, *command], timeout=15))

    root = args.repository_root.resolve()
    manifest = json.loads(args.source_manifest.read_text())
    for name, digest in manifest['files'].items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError('source differs from manifest: ' + name)
    build = query('status', args.test_job)
    if (build['state'] != 'final' or build['outcome'] != 'succeeded'
            or Path(build['spec']['working_directory']) != root
            or {'key': 'gate', 'value': 'wsl-test'} not in build['spec']['labels']):
        raise RuntimeError('a successful full Linux test Job for this source is required')
    log = query('logs', args.test_job, '--stream', 'stderr', '--json', '--limit', '1048576')
    paths = re.findall(r'Running unittests src/lib.rs\s+\(([^)]+)\)', bytes(log['bytes']).decode())
    if len(paths) != 1 or not log['eof']:
        raise RuntimeError('could not identify one validated library test executable')
    relative = Path(paths[0])
    if relative.is_absolute() or '..' in relative.parts:
        raise RuntimeError('unexpected test executable path')
    image = root / relative
    directory = (args.resume_evidence.resolve() if args.resume_evidence else
                 args.evidence_directory.resolve() / ('attached-idle-control-' + uuid.uuid4().hex))
    if not args.resume_evidence:
        directory.mkdir(parents=True, mode=0o700)
    else:
        if json.loads((directory / 'source.json').read_text()) != manifest:
            raise RuntimeError('resume source differs from retained evidence')

    def save(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + '\n')

    before = query('daemon-status')
    if (before['machine_scheduling']['mode'] != 'attached'
            or before['machine_scheduling']['blocker'] is not None):
        raise RuntimeError('healthy default attached daemon required')
    subject = {'path': str(image), 'sha256': hashlib.sha256(image.read_bytes()).hexdigest()}
    if not args.resume_evidence:
        save('system-before.json', before)
        save('source.json', manifest)
        save('build.json', build)
        save('subject.json', subject)
    elif json.loads((directory / 'subject.json').read_text()) != subject:
        raise RuntimeError('resume binary differs from retained evidence')
    spec = {'spec_version': 4, 'executable': str(image),
            'args': ['runtime_metrics::tests::attached_wait_polling_mutant_exceeds_idle_budget',
                     '--exact', '--ignored', '--nocapture', '--test-threads=1'],
            'working_directory': str(root), 'timeout_seconds': 30,
            'environment': {'set': {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}},
            'resources': {'cargo_slots': 1},
            'labels': [{'key': 'project', 'value': 'stillyard'},
                       {'key': 'gate', 'value': 'attached-idle-control'},
                       {'key': 'source', 'value': manifest['files_sha256']}]}
    if args.resume_evidence:
        if json.loads((directory / 'spec.json').read_text()) != spec:
            raise RuntimeError('resume JobSpec differs from retained evidence')
        key = json.loads((directory / 'intent.json').read_text())['idempotency_key']
    else:
        save('spec.json', spec)
        key = str(uuid.uuid4())
        save('intent.json', {'idempotency_key': key})
    print(directory, flush=True)
    prefix = 'resumed-client' if args.resume_evidence else 'client'
    with (directory / (prefix + '.stdout')).open('wb') as out, (directory / (prefix + '.stderr')).open('wb') as err:
        result = subprocess.run([cli, '--endpoint', before['endpoint'], 'ensure', '--spec',
                                 str(directory / 'spec.json'), '--idempotency-key', key,
                                 '--result-file', str(directory / 'receipt.json'),
                                 '--wait', '--deadline-seconds', '300'], stdout=out, stderr=err)
    receipt = json.loads((directory / 'receipt.json').read_text())
    job = receipt['receipt']['accepted']['job_id']
    status = query('status', job)
    # Job finality precedes the remote release acknowledgement. Wait for that
    # separate transition using the same receipt, without another submission.
    until = time.monotonic() + 30
    while (status['outcome'] == 'succeeded' and status['allocations']
           and any(a['state'] != 'released' for a in status['allocations'])
           and time.monotonic() < until):
        time.sleep(.1)
        status = query('status', job)
    save('reconciled-status.json', status)
    output = query('logs', job, '--stream', 'stdout', '--json', '--limit', '65536')
    save('stdout.json', output)
    save('stderr.json', query('logs', job, '--stream', 'stderr', '--json', '--limit', '65536'))
    if result.returncode or status['outcome'] != 'succeeded':
        raise RuntimeError('control Job failed: ' + job)
    values = [json.loads(line) for line in bytes(output['bytes']).decode().splitlines()
              if line.startswith('{')]
    if (not output['eof'] or len(values) != 1
            or values[0].get('control') != 'attached_wait_polling_mutant'
            or values[0].get('idle_budget_pass') is not False
            or values[0].get('timer_expirations') != 8
            or values[0].get('expirations_per_minute', 0) <= 6
            or not status['allocations']
            or any(a['state'] != 'released' for a in status['allocations'])):
        raise RuntimeError('missing negative-control output or released machine Grant')
    save('result.json', {'job_id': job, 'negative_control_pass': True, 'measurement': values[0]})
    print(job, 'negative control passed', flush=True)


if __name__ == '__main__':
    main()
