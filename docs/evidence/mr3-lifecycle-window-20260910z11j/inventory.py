def require(ok,message):
 if not ok:raise RuntimeError(message)
import os, json, hashlib, sqlite3, stat, subprocess
from pathlib import Path
r = Path('/home/pythonic/.local/share/stillyard')
raw = (r / 'attachment/executor/state.json').read_bytes()
envelope = json.loads(raw)
j = envelope['state']
rows = []
metadata = (r / 'attachment/executor/state.json').lstat()
require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.geteuid() and (not metadata.st_mode & 63), 'inventory integrity check failed')
require(hashlib.sha256(json.dumps(j, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest() == envelope['sha256'], 'inventory integrity check failed')
anchor=r/'attachment/anchor.json';metadata=anchor.lstat()
require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid==os.geteuid() and not metadata.st_mode & 0o077,'unsafe pairing anchor')
cgroup = Path(json.loads(anchor.read_bytes())['configuration']['executor_cgroup'])
unit=subprocess.check_output(['/usr/bin/systemctl','--user','show','--property=ControlGroup','--value','stillyard.service'],env={'XDG_RUNTIME_DIR':'/run/user/1000','DBUS_SESSION_BUS_ADDRESS':'unix:path=/run/user/1000/bus'},text=True,timeout=10).strip()
require(unit.startswith('/') and cgroup==Path('/sys/fs/cgroup')/unit.lstrip('/')/'executors','anchor cgroup differs from active delegated unit')
require(cgroup.is_relative_to('/sys/fs/cgroup') and (not cgroup.is_symlink()) and ('..' not in cgroup.parts) and (cgroup.resolve() == cgroup), 'inventory integrity check failed')
kernel = {'path': str(cgroup), 'events': dict((line.split() for line in (cgroup / 'cgroup.events').read_text().splitlines())), 'children': [p.name for p in cgroup.iterdir() if p.is_dir()]}
for p in Path('/proc').glob('[0-9]*'):
    try:
        fields = (p / 'stat').read_text().rsplit(')', 1)[1].split()
        name = (p / 'comm').read_text().strip()
        cg = (p / 'cgroup').read_text().strip()
        if name in ('cargo', 'rustc', 'codex', 'claude', 'node', 'Runner.Listener', 'Runner.Worker', 'postgres', 'ollama') and 'stillyard.service/executors/' not in cg:
            rows.append({'pid': int(p.name), 'start_ticks': int(fields[19]), 'name': name, 'cgroup': cg})
    except (OSError, ValueError):
        pass
# Reserve the same SQL admission transaction as the accepted maintenance helper.
# No data-changing SQL is executed; rollback releases the reservation.
with sqlite3.connect((r/'stillyard.sqlite3').as_uri()+'?mode=rw',uri=True,timeout=5,isolation_level=None) as db:
    db.execute('BEGIN IMMEDIATE')
    try:
        clearance=sql_barrier(db,j['anchor']['store'],j['records'])
    finally:
        db.execute('ROLLBACK')
    leases=clearance['granted_leases']
    records=clearance['blocking_containments']
alias = r / 'interop.sock'
server = alias.resolve(strict=True)
require(alias.is_symlink() and server.parent == Path('/run/WSL'), 'inventory integrity check failed')
socket = server.lstat()
require(stat.S_ISSOCK(socket.st_mode) and socket.st_uid == 0, 'inventory integrity check failed')
helpers=[]
initpid=int(server.name.removesuffix('_interop'))
for p in Path('/proc').glob('[0-9]*'):
 try:
  argv=(p/'cmdline').read_bytes().split(b'\0');fields=(p/'stat').read_text().rsplit(')',1)[1].split()
  if b'/home/pythonic/.local/share/stillyard/libexec/wsl-service.py' in argv and b'keepalive' in argv and int(fields[1])==initpid:helpers.append({'pid':int(p.name),'start_ticks':int(fields[19]),'init_pid':initpid})
 except (OSError,ValueError):pass
print(json.dumps({'keepalive_processes':helpers,'kernel': kernel, 'journal_checksum_valid': True, 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'journal_sha256': hashlib.sha256(raw).hexdigest(), 'unsealed_invocations': [k for k, v in j['records'].items() if v['seal'] is None], 'active_leases': leases, 'nonempty_containments': records, 'sql_clearance': clearance, 'binary_sha256': hashlib.sha256((r / 'bin/stillyard').read_bytes()).hexdigest(), 'interop_target': str((r / 'interop.sock').resolve(strict=True)), 'foreign_work_candidates': sorted(rows, key=lambda x: (x['pid'], x['start_ticks'])), 'foreign_inventory_scope': 'selected workload names; not a full quiescence proof'}))