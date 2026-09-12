#!/usr/bin/env python3
"""External five-minute native idle measurement, including every service process.

This reports quantitative limits separately from A-19's no-helper condition.
No polling during the measured interval; boundary doctor calls are excluded
from the denominator and included conservatively in the counter deltas.
"""
import argparse
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
    args = parser.parse_args()
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel or os.environ.get('STILLYARD_JOB_ID'):
        parser.error('idle measurement requires an unmanaged native Linux host')
    os.umask(0o077)
    root = Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'stillyard'
    cli = root / 'bin/stillyard'
    directory = args.evidence_directory.resolve() / ('native-idle-' + uuid.uuid4().hex)
    directory.mkdir(parents=True)
    (directory / 'observer.py').write_bytes(Path(__file__).read_bytes())

    def save(name, value):
        (directory / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')

    def timers(doctor):
        values = [json.loads(c['summary']) for c in doctor['checks'] if c['code'] == 'runtime_timer_expirations']
        if len(values) != 1:
            raise RuntimeError('daemon timer coverage is unavailable')
        return values[0]

    def snapshot():
        started = time.monotonic()
        doctor = json.loads(subprocess.check_output([str(cli), 'doctor', '--json'], timeout=20))
        daemon = doctor['daemon']
        if (Path(daemon['store_path']) != root or daemon['running_jobs'] or daemon['queued_jobs']
                or daemon['machine_scheduling']['mode'] not in ('standalone', 'coordinator')
                or daemon['machine_scheduling']['blocker'] is not None):
            raise RuntimeError('native idle requires the healthy installed default with no Jobs')
        with sqlite3.connect((root / 'stillyard.sqlite3').as_uri() + '?mode=ro', uri=True) as db:
            history = list(db.execute("select count(*),coalesce(max(rowid),0),sum(state!='final') from jobs").fetchone())
        group = subprocess.check_output(['/usr/bin/systemctl', '--user', 'show', 'stillyard.service',
                                         '--property=ControlGroup', '--value'], text=True, timeout=10).strip()
        if not group.startswith('/') or not group.endswith('/stillyard.service'):
            raise RuntimeError('cannot identify the installed service process boundary')
        processes = {}
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit():
                continue
            try:
                membership = (proc / 'cgroup').read_text().strip()
                try:
                    exe = os.readlink(proc / 'exe')
                except PermissionError:
                    exe = None
                owned = membership == '0::' + group or membership.startswith('0::' + group + '/')
                if exe == str(cli) and not owned:
                    raise RuntimeError('external Stillyard client/subscriber invalidates idle measurement')
                if not owned:
                    continue
                fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
                status = dict(line.split(':', 1) for line in (proc / 'status').read_text().splitlines() if ':' in line)
                threads = {}
                for thread in (proc / 'task').iterdir():
                    info = dict(line.split(':', 1) for line in (thread / 'status').read_text().splitlines() if ':' in line)
                    threads[thread.name] = int(info['voluntary_ctxt_switches'])
                processes[proc.name] = {'start_ticks': int(fields[19]), 'cpu_ticks': int(fields[11]) + int(fields[12]),
                                       'rss_anon_kib': int(status['RssAnon'].split()[0]), 'exe': exe,
                                       'threads': threads, 'cgroup': membership}
            except (FileNotFoundError, ProcessLookupError):
                continue
        if str(daemon['pid']) not in processes:
            raise RuntimeError('installed daemon is outside the measured service')
        return {'capture_started': started, 'capture_finished': time.monotonic(), 'doctor': doctor,
                'history': history, 'processes': processes, 'timers': timers(doctor),
                'binary_sha256': hashlib.sha256(cli.read_bytes()).hexdigest(),
                'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}

    before = snapshot()
    save('before', before)
    print('native idle baseline captured', directory, flush=True)
    for index in range(6):
        time.sleep(50)
        print('native idle seconds', (index + 1) * 50, flush=True)
    after = snapshot()
    save('after', after)
    seconds = after['capture_started'] - before['capture_finished']
    if (seconds < 300 or before['history'] != after['history'] or before['boot_id'] != after['boot_id']
            or before['binary_sha256'] != after['binary_sha256']
            or before['doctor']['daemon']['process_identity'] != after['doctor']['daemon']['process_identity']
            or before['doctor']['daemon']['daemon_generation'] != after['doctor']['daemon']['daemon_generation']
            or before['processes'].keys() != after['processes'].keys()):
        raise RuntimeError('native idle interval identity or history changed')
    deltas = []
    for pid, current in after['processes'].items():
        old = before['processes'][pid]
        if (current['start_ticks'] != old['start_ticks'] or current['exe'] != old['exe']
                or current['threads'].keys() != old['threads'].keys()):
            raise RuntimeError('native idle process/thread identity changed')
        cpu = current['cpu_ticks'] - old['cpu_ticks']
        switches = sum(v - old['threads'][k] for k, v in current['threads'].items())
        if cpu < 0 or switches < 0:
            raise RuntimeError('native idle counters moved backwards')
        deltas.append({'pid': int(pid), 'role': 'daemon' if int(pid) == after['doctor']['daemon']['pid'] else 'helper',
                       'cpu_percent_one_core': cpu * 100 / os.sysconf('SC_CLK_TCK') / seconds,
                       'memory_mib': max(old['rss_anon_kib'], current['rss_anon_kib']) / 1024,
                       'voluntary_switches': switches})
    expirations = {key: value - before['timers'][key] for key, value in after['timers'].items()}
    if any(v < 0 for v in expirations.values()):
        raise RuntimeError('native daemon timer counters moved backwards')
    cpu = sum(p['cpu_percent_one_core'] for p in deltas)
    memory = sum(p['memory_mib'] for p in deltas)
    rate = sum(expirations.values()) * 60 / seconds
    quantitative = cpu < .5 and memory < 40 and rate <= 2
    no_helper = len(deltas) == 1
    result = {'duration_seconds': seconds, 'processes': deltas, 'timer_expirations': expirations,
              'timer_expirations_per_minute': rate, 'aggregate_cpu_percent_one_core': cpu,
              'aggregate_memory_mib': memory, 'quantitative_limits_passed': quantitative,
              'no_helper_condition_passed': no_helper, 'a19_passed': quantitative and no_helper,
              'memory_scope': 'Endpoint RssAnon samples excluding mapped SQLite, not interval peak.',
              'timer_scope': 'Application daemon counters; helper context switches retained separately, not called timer wakes.',
              'remaining': [] if no_helper else ['Native supervisor must be removed before A-19 can pass.']}
    save('result', result)
    print(json.dumps(result), flush=True)
    if not quantitative:
        raise RuntimeError('native aggregate idle quantitative budget exceeded')


if __name__ == '__main__':
    main()
