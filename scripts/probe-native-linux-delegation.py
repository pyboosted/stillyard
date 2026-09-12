#!/usr/bin/env python3
"""Disposable native systemd probe for an empty delegated unit with no keeper.

This is an installation prerequisite experiment, not Stillyard Job acceptance.
The fixture owns its unit and every child cgroup; never run on the workstation.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid


def systemctl(*args):
    return subprocess.check_output(['/usr/bin/systemctl', '--user', *args], text=True, timeout=15).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--disposable-native-host', action='store_true', required=True)
    parser.add_argument('--evidence-directory', type=Path, required=True)
    parser.add_argument('--setup-unit')
    args = parser.parse_args()
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel or os.environ.get('STILLYARD_JOB_ID'):
        parser.error('delegation experiment requires a disposable unmanaged native host')
    os.umask(0o077)
    evidence = args.evidence_directory.resolve()
    if args.setup_unit:
        unit = args.setup_unit
        if not unit.startswith('stillyard-delegation-probe-') or not unit.endswith('.service'):
            parser.error('setup may target only the experiment unit')
        if systemctl('show', unit, '--property=SubState', '--value') != 'exited':
            raise RuntimeError('initial service prune has not completed')
        subprocess.run(['/usr/bin/busctl', '--user', 'call', 'org.freedesktop.systemd1',
                        '/org/freedesktop/systemd1', 'org.freedesktop.systemd1.Manager',
                        'AttachProcessesToUnit', 'ssau', unit, '', '1', str(os.getpid())], check=True, timeout=10)
        group = Path('/sys/fs/cgroup') / systemctl('show', unit, '--property=ControlGroup', '--value').lstrip('/')
        setup = group / 'setup'
        setup.mkdir()
        (setup / 'cgroup.procs').write_text(str(os.getpid()))
        (group / 'cgroup.subtree_control').write_text('+cpu +memory +pids')
        executors = group / 'executors'
        executors.mkdir()
        (executors / 'cgroup.subtree_control').write_text('+cpu +memory +pids')
        empty, live = executors / 'empty', executors / 'live'
        empty.mkdir()
        live.mkdir()
        def place():
            (live / 'cgroup.procs').write_text(str(os.getpid()))
        child = subprocess.Popen(['/usr/bin/python3', '-c', 'import time;time.sleep(60)'],
                                 preexec_fn=place, stdin=subprocess.DEVNULL,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result = {'unit': unit, 'group': str(group), 'child_pid': child.pid,
                  'inodes': {str(p): p.stat().st_ino for p in (group, setup, executors, empty, live)},
                  'controllers': (executors / 'cgroup.controllers').read_text().split()}
        (evidence / 'setup.json').write_text(json.dumps(result, indent=2) + '\n')
        return
    evidence.mkdir(parents=True, exist_ok=False)
    unit = 'stillyard-delegation-probe-' + uuid.uuid4().hex + '.service'
    path = Path.home() / '.config/systemd/user' / unit
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('[Unit]\nDescription=Disposable native delegation lifetime probe\nStopWhenUnneeded=no\n'
                    '[Service]\nType=oneshot\nExecStart=/usr/bin/true\nRemainAfterExit=yes\n'
                    'Delegate=cpu memory pids\nKillMode=control-group\n')
    try:
        systemctl('daemon-reload')
        systemctl('start', unit)
        subprocess.run([sys.executable, __file__, '--disposable-native-host', '--evidence-directory', str(evidence),
                        '--setup-unit', unit], check=True, timeout=20)
        setup = json.loads((evidence / 'setup.json').read_text())
        if not {'cpu', 'memory', 'pids'}.issubset(setup['controllers']):
            raise RuntimeError('required controllers are not delegated')
        group = Path(setup['group'])
        frames = []
        def frame(name, populated):
            state = systemctl('show', unit, '--property=ActiveState', '--property=SubState', '--property=MainPID')
            inodes = {p: Path(p).stat().st_ino for p in setup['inodes']}
            events = dict(line.split() for line in (group / 'cgroup.events').read_text().splitlines())
            frames.append({'name': name, 'unit_state': state, 'inodes': inodes, 'events': events})
            (evidence / 'frames.json').write_text(json.dumps(frames, indent=2) + '\n')
            if (inodes != setup['inodes'] or 'ActiveState=active' not in state
                    or 'SubState=exited' not in state or 'MainPID=0' not in state or events['populated'] != populated):
                raise RuntimeError('delegated boundary lifetime or helper-free unit state changed')
        time.sleep(2)
        frame('setup-exited-child-alive', '1')
        (group / 'executors/live/cgroup.kill').write_text('1')
        until = time.monotonic() + 5
        while dict(line.split() for line in (group / 'cgroup.events').read_text().splitlines())['populated'] != '0':
            if time.monotonic() >= until:
                raise RuntimeError('fixture child failed to empty')
            time.sleep(.05)
        time.sleep(2)
        frame('all-processes-exited', '0')
        systemctl('daemon-reload')
        time.sleep(2)
        frame('empty-after-daemon-reload', '0')
        (evidence / 'result.json').write_text(json.dumps({'delegation_prerequisite_passed': True,
            'unit': unit, 'remaining': ['installed daemon restart with actual journal', 'whole-host lifecycle']}, indent=2) + '\n')
    finally:
        systemctl('stop', unit)
        path.unlink()
        systemctl('daemon-reload')


if __name__ == '__main__':
    main()
