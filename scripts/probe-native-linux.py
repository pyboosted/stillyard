#!/usr/bin/env python3
"""Inspect a prospective standalone Linux host without installing or reconfiguring it.

The namespace/exec-stop control starts only its own short-lived descendants.
This is a prerequisite probe, not native installation or containment acceptance.
On the development workstation invoke this script as a default Stillyard Job.
"""

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys


EXEC_STOP_PROBE = r'''
import ctypes, os, signal
libc = ctypes.CDLL(None, use_errno=True)
libc.ptrace.argtypes = [ctypes.c_uint, ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p]
libc.ptrace.restype = ctypes.c_long
def trace(request, pid, data=0):
    if libc.ptrace(request, pid, None, data) == -1:
        raise OSError(ctypes.get_errno(), 'ptrace failed')
reader, writer = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(writer)
    assert os.read(reader, 1) == b'x'
    os.close(reader)
    os.execve('/usr/bin/true', ['true'], {})
os.close(reader)
try:
    trace(0x4206, pid, 0x10 | 0x100000)  # SEIZE, TRACEEXEC | EXITKILL
    os.write(writer, b'x')
    os.close(writer)
    waited, status = os.waitpid(pid, 0)
    assert waited == pid and os.WIFSTOPPED(status) and status >> 16 == 4, status
    trace(17, pid)  # DETACH at exec stop, before target user code
    waited, status = os.waitpid(pid, 0)
    assert waited == pid and os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    pid = None
    print('namespace-exec-stop-passed', flush=True)
finally:
    if pid is not None:
        try:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        except ProcessLookupError:
            pass
'''


def command(argv, timeout=10):
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        return {"exit_code": result.returncode,
                "stdout": result.stdout.strip()[:4096],
                "stderr": result.stderr.strip()[:4096]}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"exit_code": None, "error": str(error)}


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError as error:
        return {"error": str(error)}


def mount_field(value):
    for escaped, decoded in [(r'\040', ' '), (r'\011', '\t'),
                             (r'\012', '\n'), (r'\134', '\\')]:
        value = value.replace(escaped, decoded)
    return value


def store_filesystem(path):
    ancestor = path.absolute()
    while not ancestor.exists() and ancestor != ancestor.parent:
        ancestor = ancestor.parent
    ancestor = ancestor.resolve(strict=True)
    selected = None
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        before, after = line.split(' - ', 1)
        mount = Path(mount_field(before.split()[4]))
        if ancestor == mount or mount in ancestor.parents:
            if selected is None or len(mount.parts) > len(Path(selected['mount']).parts):
                selected = {"mount": str(mount), "filesystem": after.split()[0],
                            "existing_ancestor": str(ancestor)}
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store', type=Path,
                        default=Path.home() / '.local/share/stillyard')
    parser.add_argument('--executor-cgroup', type=Path)
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('this probe requires Linux')
    release = platform.release()
    native = 'microsoft' not in release.lower() and 'wsl' not in release.lower()
    filesystem = store_filesystem(args.store)
    user_service = command(['/usr/bin/systemctl', '--user', 'show', '--property=Version', '--value'])
    linger = command(['/usr/bin/loginctl', 'show-user', str(os.geteuid()),
                      '--property=Linger', '--value'])
    bwrap = shutil.which('bwrap')
    namespace = {"exit_code": None, "error": 'bubblewrap is not installed'}
    if bwrap:
        namespace = command([
            bwrap, '--ro-bind', '/', '/', '--dev', '/dev', '--proc', '/proc',
            '--unshare-user', '--unshare-pid', '--unshare-uts', '--unshare-cgroup',
            '--new-session', '--die-with-parent', '--tmpfs', '/run',
            '--clearenv', '--', sys.executable, '-c', EXEC_STOP_PROBE,
        ], timeout=15)
    delegation = None
    if args.executor_cgroup is not None:
        root = args.executor_cgroup
        delegation = {
            "path": str(root),
            "controllers": read(root / 'cgroup.controllers'),
            "subtree_control": read(root / 'cgroup.subtree_control'),
            "events": read(root / 'cgroup.events'),
            "kill_writable": os.access(root / 'cgroup.kill', os.W_OK),
            "procs_writable": os.access(root / 'cgroup.procs', os.W_OK),
            "scope": 'read-only inventory; no execution or cleanup proof',
        }
    prerequisites = {
        "native_linux": native,
        "x86_64": platform.machine() == 'x86_64',
        "local_ext4": filesystem is not None and filesystem['filesystem'] == 'ext4',
        "cgroup_v2": Path('/sys/fs/cgroup/cgroup.controllers').is_file(),
        "systemd_user_manager": user_service['exit_code'] == 0,
        "linger_enabled": linger.get('stdout') == 'yes' and linger['exit_code'] == 0,
        "namespace_exec_stop": namespace['exit_code'] == 0
            and namespace.get('stdout') == 'namespace-exec-stop-passed',
    }
    print(json.dumps({
        "kind": 'native_linux_prerequisite_probe', "kernel": release,
        "managed_job_id": os.environ.get('STILLYARD_JOB_ID'),
        "observation_scope": 'caller-visible namespaces; a managed Job is not a host service inventory',
        "architecture": platform.machine(), "owner_uid": os.geteuid(),
        "boot_id": read('/proc/sys/kernel/random/boot_id'),
        "requested_store": str(args.store.absolute()), "store_filesystem": filesystem,
        "current_cgroup": read('/proc/self/cgroup'), "user_service": user_service,
        "linger": linger, "bubblewrap": bwrap, "namespace_exec_stop": namespace,
        "executor_delegation": delegation, "prerequisites": prerequisites,
        "prerequisites_passed": all(prerequisites.values()),
        "native_installation_accepted": False,
        "remaining": ['installed executor delegation and born-contained canary',
                      'durable installation, authority and executor history',
                      'native consumer, recovery and performance acceptance'],
    }, indent=2))
    return 0 if all(prerequisites.values()) else 2


if __name__ == '__main__':
    raise SystemExit(main())
