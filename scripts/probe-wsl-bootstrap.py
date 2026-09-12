#!/usr/bin/env python3
"""Probe a Windows system Job's containment of a bounded WSL descendant.

No Cargo and no daemon stop/reset. Exit 2 means unsafe bootstrap was demonstrated;
exit 0 only means this particular escape was not observed, NOT safe bootstrap.
Requires WSL, the installed Windows CLI, and a Windows-visible evidence directory.
"""

import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid


def documents(data):
    decoder = json.JSONDecoder()
    remaining = data.strip()
    result = []
    while remaining:
        value, length = decoder.raw_decode(remaining)
        result.append(value)
        remaining = remaining[length:].lstrip()
    return result


def process_identity(pid):
    try:
        # comm can contain spaces or ')'; fields following the last ')' are fixed.
        tail = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
        return {"pid": pid, "start_ticks": tail[19], "state": tail[0]}
    except FileNotFoundError:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli', type=Path, required=True)
    parser.add_argument('--distro', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--windows-cwd', required=True)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    args = parser.parse_args()
    directory = args.evidence_directory.resolve() / ('bootstrap-' + uuid.uuid4().hex)
    directory.mkdir(parents=True, exist_ok=False)
    marker = directory / 'descendant.json'
    finished = directory / 'descendant-finished.json'
    child = (
        'import json,os,pathlib,signal,time; '
        'signal.signal(signal.SIGHUP,signal.SIG_IGN); '
        'tail=pathlib.Path("/proc/self/stat").read_text().rsplit(")",1)[1].split(); '
        f'pathlib.Path({str(marker)!r}).write_text(json.dumps({{"pid":os.getpid(),'
        '"start_ticks":tail[19],"cgroup":pathlib.Path("/proc/self/cgroup").read_text()})); '
        'time.sleep(8); '
        f'pathlib.Path({str(finished)!r}).write_text(json.dumps({{"completed":True}}))'
    )
    root = (
        'import subprocess,time; '
        f'subprocess.Popen(["/usr/bin/python3","-c",{child!r}], '
        'stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL, '
        'start_new_session=True); print("bounded WSL descendant spawned",flush=True); '
        'time.sleep(12)'
    )
    spec = {
        'spec_version': 4, 'executable': r'C:\Windows\System32\wsl.exe',
        'args': ['--distribution', args.distro, '--user', args.user,
                 '--cd', str(directory), '--exec', '/usr/bin/python3', '-c', root],
        'working_directory': args.windows_cwd,
        'resources': {'cargo_slots': 1},
        'labels': [{'key': 'project', 'value': 'stillyard'},
                   {'key': 'gate', 'value': 'mr0-bootstrap-negative-control'}],
        'timeout_seconds': 2,
    }
    spec_path = directory / 'spec.json'
    spec_path.write_text(json.dumps(spec, indent=2) + '\n')
    win_spec = subprocess.check_output(['wslpath', '-w', str(spec_path)], text=True).strip()
    win_receipt = subprocess.check_output(
        ['wslpath', '-w', str(directory / 'receipt.json')], text=True).strip()
    result = subprocess.run([str(args.cli), 'submit', '--spec', win_spec,
                             '--result-file', win_receipt, '--wait'], capture_output=True)
    (directory / 'submit.stdout').write_bytes(result.stdout)
    (directory / 'submit.stderr').write_bytes(result.stderr)
    replies = documents(result.stdout.decode())
    final = replies[-1]
    if final.get('state') != 'final' or 'job_id' not in final:
        raise RuntimeError('No terminal system Job snapshot; probe inconclusive')
    (directory / 'status.json').write_text(json.dumps(final, indent=2) + '\n')
    recorded = json.loads(marker.read_text()) if marker.exists() else None
    actual = process_identity(recorded['pid']) if recorded else None
    live = bool(actual and actual['state'] != 'Z'
                and actual['start_ticks'] == recorded['start_ticks'])
    containment = final['attempts'][-1]['invocations'][0]['containment']
    unsafe = containment['state'] == 'empty' and live
    report = {
        'job_id': final['job_id'], 'job_outcome': final['outcome'],
        'cli_exit_code': result.returncode, 'windows_containment': containment,
        'recorded_descendant': recorded, 'descendant_after_job_final': actual,
        'matching_descendant_alive_after_windows_empty': unsafe,
        'bootstrap_acceptance': 'failed' if unsafe else 'inconclusive',
    }
    # Observe natural bounded completion; never kill another workload by PID/name.
    deadline = time.monotonic() + 15
    while recorded and time.monotonic() < deadline:
        current = process_identity(recorded['pid'])
        if not current or current['start_ticks'] != recorded['start_ticks'] or current['state'] == 'Z':
            break
        time.sleep(0.25)
    current = process_identity(recorded['pid']) if recorded else None
    report['descendant_still_running_at_end'] = bool(
        current and current['state'] != 'Z' and current['start_ticks'] == recorded['start_ticks'])
    report['natural_completion_marker'] = finished.exists()
    (directory / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(directory / 'report.json')
    print(json.dumps(report, indent=2))
    return 2 if unsafe else 0


if __name__ == '__main__':
    raise SystemExit(main())
