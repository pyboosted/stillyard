#!/usr/bin/env python3
"""Run the prebuilt native/Linux fault subjects as a default Windows Job.

Requires successful system test Jobs for both subjects on the same source map.
The installed CLI primary runs the native controller alongside protected bootstrap; only
isolated subject Stores are reset. This launcher never invokes Cargo directly.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--windows-source', type=Path, required=True)
    parser.add_argument('--linux-source', type=Path, required=True)
    parser.add_argument('--source-manifest', type=Path, required=True)
    parser.add_argument('--windows-test-job', required=True)
    parser.add_argument('--linux-test-job', required=True)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    parser.add_argument('--distribution', required=True)
    parser.add_argument('--user', required=True)
    args = parser.parse_args()
    if os.environ.get('STILLYARD_JOB_ID'):
        parser.error('root fault launcher must run outside a managed Invocation')
    windows = '/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe'
    linux = str(Path.home() / '.local/share/stillyard/bin/stillyard')
    manifest = json.loads(args.source_manifest.read_text())
    def winpath(path):
        return subprocess.check_output(['wslpath', '-w', str(path.resolve())], text=True).strip()
    def query(cli, *command):
        return json.loads(subprocess.check_output([cli, *command], timeout=15))
    for root in (args.windows_source, args.linux_source):
        for name, expected in manifest['files'].items():
            if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
                raise RuntimeError('subject source differs from manifest: ' + str(root / name))
    directory = args.evidence_directory.resolve() / ('cross-os-reset-' + uuid.uuid4().hex)
    if not directory.is_relative_to('/mnt/c'):
        parser.error('native durable evidence must be on NTFS')
    directory.mkdir(parents=True)
    def save(name, value):
        (directory / name).write_text(json.dumps(value, indent=2) + '\n')
    def subject(cli, job, root, expected_gate, module):
        status = query(cli, 'status', job)
        expected_source = str(root.resolve()) if cli == linux else winpath(root)
        if (status['state'] != 'final' or status['outcome'] != 'succeeded'
                or {'key': 'gate', 'value': expected_gate} not in status['spec']['labels']
                or status['spec']['working_directory'].casefold() != expected_source.casefold()):
            raise RuntimeError('subject test Job does not validate the selected source')
        log = query(cli, 'logs', job, '--stream', 'stderr', '--json', '--limit', '1048576')
        matches = re.findall(r'Running ' + re.escape(module) + r'\s+\(([^)]+)\)', bytes(log['bytes']).decode().replace('\\', '/'))
        if len(matches) != 1 or not log['eof']:
            raise RuntimeError('cannot identify exactly one completed subject binary')
        relative = Path(matches[0].replace('\\', '/'))
        if relative.is_absolute() or '..' in relative.parts:
            raise RuntimeError('unexpected Cargo executable log path')
        image = root / relative
        if not image.is_file():
            raise RuntimeError('validated subject binary is missing')
        save(expected_gate + '-build.json', status)
        save(expected_gate + '-subject.json', {'path': str(image), 'sha256': hashlib.sha256(image.read_bytes()).hexdigest()})
        return image, status
    native_binary, native_job = subject(windows, args.windows_test_job, args.windows_source, 'test', 'tests/isolated_daemon.rs')
    linux_binary, _ = subject(linux, args.linux_test_job, args.linux_source, 'wsl-test', 'unittests src/lib.rs')
    mailbox = directory / 'private-mailbox'
    mailbox.mkdir()
    # This mailbox carries an isolated fixture pairing secret. Remove inherited
    # access before either subject writes it; retain owner and SYSTEM only.
    powershell = '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
    import base64
    acl_script = "$p = $args[0]; $sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value; & icacls.exe $p /inheritance:r /grant:r ('*' + $sid + ':(OI)(CI)F') '*S-1-5-18:(OI)(CI)F'; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }"
    # Use -Command with a properly quoted literal path, not interpolated shell text.
    acl_script = acl_script.replace('$args[0]', "'" + winpath(mailbox).replace("'", "''") + "'")
    subprocess.run([powershell, '-NoProfile', '-EncodedCommand', base64.b64encode(acl_script.encode('utf-16le')).decode()], check=True, capture_output=True)
    work = {'distribution': args.distribution, 'user': args.user,
            'executable': str(linux_binary.resolve()),
            'args': ['linux_executor_cross_os_reset_subject', '--ignored', '--nocapture'],
            'working_directory': str(args.linux_source.resolve()), 'timeout_seconds': 240,
            'delegate_test_cgroup': True,
            'environment': {'PATH': '/usr/bin:/bin',
                            'STILLYARD_TEST_EXECUTABLE': str((args.linux_source / 'target/scheduled-linux/debug/stillyard').resolve()),
                            'MR_WSL_FAULT_MAILBOX': str(mailbox)}}
    save('work.json', work)
    environment = native_job['spec']['environment']
    environment['set'].update({
        'MR_WSL_FAULT_MAILBOX_WINDOWS': winpath(mailbox),
        'MR_WSL_TEST_DISTRO': args.distribution, 'MR_WSL_TEST_USER': args.user,
        'MR_WSL_TEST_BINARY': str(linux_binary.resolve()),
        'MR_WSL_TEST_SOURCE': str(args.linux_source.resolve()),
        'MR_WSL_TEST_EXECUTABLE': str((args.linux_source / 'target/scheduled-linux/debug/stillyard').resolve()),
        'MR_WSL_FAULT_EVIDENCE': winpath(directory / 'subject'),
    })
    spec = {'spec_version': 4, 'executable': winpath(Path(windows)),
            'args': ['bootstrap', 'run', '--spec', winpath(directory / 'work.json'),
                     '--native-controller', winpath(native_binary), '--',
                     'machine_cross_os_reset_retains_live_linux_executor_and_guest_history', '--ignored', '--nocapture', '--test-threads=1'],
            'working_directory': winpath(args.windows_source), 'environment': environment,
            'resources': {'cargo_slots': 1, 'impacts': ['cpu_heavy']}, 'timeout_seconds': 600,
            'labels': [{'key': 'project', 'value': 'stillyard'}, {'key': 'gate', 'value': 'cross-os-reset'},
                       {'key': 'source', 'value': manifest['files_sha256']}]}
    save('spec.json', spec); save('source.json', manifest)
    key = str(uuid.uuid4()); save('intent.json', {'idempotency_key': key})
    before = query(windows, 'daemon-status'); save('system-before.json', before)
    print(directory, flush=True)
    with (directory / 'client.stdout').open('wb') as out, (directory / 'client.stderr').open('wb') as err:
        result = subprocess.run([windows, '--endpoint', before['endpoint'], 'ensure', '--spec', winpath(directory / 'spec.json'),
                                 '--idempotency-key', key, '--result-file', winpath(directory / 'receipt.json'),
                                 '--wait', '--deadline-seconds', '660'], stdout=out, stderr=err)
    receipt = json.loads((directory / 'receipt.json').read_text())
    job = receipt['receipt']['accepted']['job_id']
    status = query(windows, 'status', job); save('status.json', status)
    save('system-after.json', query(windows, 'daemon-status'))
    authority = query(windows, 'authority', 'status')
    holds = [hold for hold in authority['holds']
             if hold.get('bootstrap', {}).get('parent', {}).get('job_id') == job]
    save('bootstrap-holds.json', holds)
    if status['outcome'] == 'succeeded':
        if len(holds) != 1 or not holds[0]['released'] or holds[0].get('cleanup_proof', {}).get('phase') != 'sealed_empty' or holds[0]['cleanup_proof']['root_exit_code'] != 0 or holds[0]['cleanup_proof']['termination'] != 'exited':
            raise RuntimeError('successful controller lacks its actual outer bootstrap seal')
        # Completed test mailbox is disposable; public artifacts and actual
        # bootstrap proof have been retained outside it. Failed mailboxes remain
        # private for diagnosis and are never exported to repository evidence.
        import shutil
        shutil.rmtree(mailbox)
    print(job, status['state'], status['outcome'], flush=True)
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
