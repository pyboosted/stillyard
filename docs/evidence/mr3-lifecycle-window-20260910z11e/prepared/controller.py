"""Native default-Job controller; destructive modes require an agreed window.

Preflight is read-only. Terminate/shutdown are quiescent lifecycle controls,
not proofs of cleanup of live Linux Invocations. A controller-owned keepalive
must be handed off to an external session before this Job completes.
"""
import argparse, hashlib, json, os, subprocess, time
from pathlib import Path

WCLI = Path(r'C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe')
WSL = Path(r'C:\Windows\System32\wsl.exe')
LROOT = '/home/pythonic/.local/share/stillyard'
LCLI = LROOT + '/bin/stillyard'
HELPER = LROOT + '/libexec/wsl-service.py'

# Public evidence only: never read/export the pairing secret.
INVENTORY = '''import os,json,hashlib,sqlite3,stat
from pathlib import Path
r=Path('/home/pythonic/.local/share/stillyard');raw=(r/'attachment/executor/state.json').read_bytes();envelope=json.loads(raw);j=envelope['state'];rows=[]
metadata=(r/'attachment/executor/state.json').lstat()
assert stat.S_ISREG(metadata.st_mode) and metadata.st_uid==os.geteuid() and not metadata.st_mode & 0o077
assert hashlib.sha256(json.dumps(j,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()==envelope['sha256']
# Export only the public cgroup path; no credentials from the private anchor.
cgroup=Path(json.loads((r/'attachment/anchor.json').read_bytes())['configuration']['executor_cgroup'])
assert cgroup.is_relative_to('/sys/fs/cgroup') and not cgroup.is_symlink() and '..' not in cgroup.parts and cgroup.resolve()==cgroup
kernel={'path':str(cgroup),'events':dict(line.split() for line in (cgroup/'cgroup.events').read_text().splitlines()),'children':[p.name for p in cgroup.iterdir() if p.is_dir()]}
for p in Path('/proc').glob('[0-9]*'):
 try:
  fields=(p/'stat').read_text().rsplit(')',1)[1].split();name=(p/'comm').read_text().strip();cg=(p/'cgroup').read_text().strip()
  if name in ('cargo','rustc','codex','claude','node','Runner.Listener','Runner.Worker','postgres','ollama') and 'stillyard.service/executors/' not in cg:rows.append({'pid':int(p.name),'start_ticks':int(fields[19]),'name':name,'cgroup':cg})
 except (OSError,ValueError):pass
with sqlite3.connect('file:'+str(r/'stillyard.sqlite3')+'?mode=ro',uri=True) as db:
 leases=db.execute("select count(*) from leases where state='granted'").fetchone()[0]
 records=db.execute("select count(*) from containments where state!='empty'").fetchone()[0]
print(json.dumps({'kernel':kernel,'journal_checksum_valid':True,'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'journal_sha256':hashlib.sha256(raw).hexdigest(),'unsealed_invocations':[k for k,v in j['records'].items() if v['seal'] is None],'active_leases':leases,'nonempty_containments':records,'binary_sha256':hashlib.sha256((r/'bin/stillyard').read_bytes()).hexdigest(),'interop_target':str((r/'interop.sock').resolve()),'foreign_work_candidates':rows,'foreign_inventory_scope':'selected workload names; not a full quiescence proof'}))'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--operation', choices=['preflight','terminate','shutdown'], default='preflight')
    parser.add_argument('--foreign-work-agreed', action='store_true')
    parser.add_argument('--evidence-directory', type=Path, required=True)
    args = parser.parse_args()
    if os.name != 'nt' or not os.environ.get('STILLYARD_JOB_ID'):
        parser.error('requires a native default Windows Stillyard Job')
    if args.operation != 'preflight' and not args.foreign_work_agreed:
        parser.error('the scheduling plan requires a separate agreed interruption window')
    out = args.evidence_directory.resolve();out.mkdir(parents=True, exist_ok=False)
    def save(name, value):
        target=out/name;tmp=out/(name+'.tmp')
        with tmp.open('w',encoding='utf-8') as f:
            json.dump(value,f,indent=2);f.flush();os.fsync(f.fileno())
        tmp.replace(target)
    def native(*cmd):return json.loads(subprocess.check_output([str(WCLI),*cmd],timeout=20))
    def linux(*cmd):return subprocess.check_output([str(WSL),'-d','Ubuntu-SSD','-u','pythonic','--exec',*cmd],timeout=30)
    def guest():return json.loads(linux(LCLI,'--endpoint',LROOT+'/stillyard-v6.sock','daemon-status'))
    def inventory():return json.loads(linux('/usr/bin/python3','-c',INVENTORY))
    own=native('status',os.environ['STILLYARD_JOB_ID'])
    inv=[i for a in own['attempts'] for i in a['invocations'] if i['invocation_id']==os.environ['STILLYARD_INVOCATION_ID']]
    assert len(inv)==1 and inv[0]['role']=='primary' and inv[0]['root_identity']['pid']==os.getpid()
    win=native('daemon-status');store_path=win['store_path']
    if store_path.startswith(chr(92)*2+'?'+chr(92)):store_path=store_path[4:]
    assert Path(store_path)==WCLI.parent.parent/'data'
    running=subprocess.check_output([str(WSL),'--list','--running','--quiet'],timeout=20).decode('utf-16le').replace('\x00','').splitlines()
    assert 'Ubuntu-SSD' in [s.strip() for s in running], 'preflight must not cold-start an unobserved distro'
    before=guest();public=inventory();authority=native('authority','status')
    plan={'controller_job':own['job_id'],'controller_invocation':inv[0]['invocation_id'],'operation':args.operation,'agreed_window':args.foreign_work_agreed,'running_distributions':running,'windows':win,'linux':before,'executor':public,'authority':authority,'controller_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'disruptive_action_executed':False}
    save('before.json',plan)
    save('windows-doctor.json',native('doctor','--json'))
    save('linux-doctor.json',json.loads(linux(LCLI,'doctor','--json')))
    if args.operation=='preflight':
        print(json.dumps({'preflight_only':True,'foreign_work_candidates':len(public['foreign_work_candidates']),'directory':str(out)}),flush=True);return
    assert win['running_jobs']==1 and win['queued_jobs']==0 and before['running_jobs']==0 and before['queued_jobs']==0
    assert not public['unsealed_invocations'] and public['active_leases']==0 and public['nonempty_containments']==0
    assert not [h for h in authority['holds'] if not h['released']]
    assert not authority['machine_obligations'] and authority['blocker'] is None
    assert before['machine_scheduling']['blocker'] is None
    # A second observation catches changes during preparation. The agreed window
    # must prohibit new submissions; these snapshots are not an atomic fence.
    win2=native('daemon-status');before2=guest();public2=inventory();authority2=native('authority','status')
    save('immediate-barrier.json',{'windows':win2,'linux':before2,'executor':public2,'authority':authority2})
    assert win2['running_jobs']==1 and win2['queued_jobs']==0 and before2['running_jobs']==0 and before2['queued_jobs']==0
    assert public2['journal_sha256']==public['journal_sha256'] and public2['foreign_work_candidates']==public['foreign_work_candidates']
    assert public2['active_leases']==0 and public2['nonempty_containments']==0 and not public2['unsealed_invocations']
    assert public2['kernel']['events']['populated']=='0' and not public2['kernel']['children']
    assert authority2['blocker'] is None and not authority2['machine_obligations'] and not [h for h in authority2['holds'] if not h['released']]
    assert before2['daemon_generation']==before['daemon_generation'] and win2['daemon_generation']==win['daemon_generation']
    command=[str(WSL),'--terminate','Ubuntu-SSD'] if args.operation=='terminate' else [str(WSL),'--shutdown']
    save('fault-intent.json',{'command':command,'unix_ns':time.time_ns(),'scope':'quiescent prior sealed history'})
    result=subprocess.run(command,capture_output=True,timeout=60)
    save('fault-result.json',{'exit_code':result.returncode,'stdout_hex':result.stdout.hex(),'stderr_hex':result.stderr.hex(),'unix_ns':time.time_ns()})
    assert result.returncode==0, 'fault result is not success; original evidence retained'
    with (out/'keepalive.stdout').open('wb') as stdout,(out/'keepalive.stderr').open('wb') as stderr:
        keeper=subprocess.Popen([str(WSL),'-d','Ubuntu-SSD','-u','pythonic','--exec','/usr/bin/python3',HELPER,'keepalive','--root',LROOT],stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr)
    save('keepalive-owner.json',{'windows_pid':keeper.pid,'owned_by_controller_job':own['job_id'],'external_handoff_required':True})
    until=time.monotonic()+120
    while True:
        assert keeper.poll() is None, 'controller keepalive exited before recovery'
        try:
            after=guest()
            if after['machine_scheduling']['blocker'] is None:break
        except (subprocess.CalledProcessError,subprocess.TimeoutExpired,ValueError):pass
        assert time.monotonic()<until,'restart did not become healthy; no history is cleared';time.sleep(2)
    after_public=inventory();after_win=native('daemon-status');save('after-restart.json',{'linux':after,'executor':after_public,'windows':after_win})
    assert after_win['store_uuid']==win['store_uuid'] and after_win['daemon_generation']==win['daemon_generation']
    assert after_win['machine_scheduling']['domains']==win['machine_scheduling']['domains'] and after['machine_scheduling']['domains']==before['machine_scheduling']['domains']
    assert after_public['kernel']['events']['populated']=='0' and not after_public['kernel']['children']
    assert after['store_uuid']==before['store_uuid'] and after['daemon_generation']!=before['daemon_generation']
    assert after_public['journal_sha256']==public['journal_sha256'] and after_public['binary_sha256']==public['binary_sha256']
    assert not after_public['unsealed_invocations'] and after_public['active_leases']==0
    save('awaiting-handoff.json',{'directory':str(out),'instruction':'Resume the external agent in a new WSL session. Capture the exact controller keepalive identity, stop only that helper, start an external foreground keepalive, and submit installed canaries. This controller completes only after the new alias and healthy pair are observed.','controller_keepalive_windows_pid':keeper.pid})
    print('Recovered; awaiting external keepalive handoff',out,flush=True)
    until=time.monotonic()+1500
    while keeper.poll() is None:
        assert time.monotonic()<until,'external handoff not completed; retained receipt identifies the controller';time.sleep(5)
    until=time.monotonic()+120
    while True:
        try:
            handed=guest();state=inventory()
            if handed['machine_scheduling']['blocker'] is None and state['interop_target']!=after_public['interop_target']:break
        except (subprocess.CalledProcessError,subprocess.TimeoutExpired,ValueError):pass
        assert time.monotonic()<until,'external keepalive takeover did not become healthy';time.sleep(2)
    assert handed['store_uuid']==before['store_uuid'] and not state['unsealed_invocations'] and state['active_leases']==0
    save('result.json',{'quiescent_restart_passed':True,'operation':args.operation,'controller_job':own['job_id'],'external_handoff':state,'canary_acceptance':'must be collected separately; this restart result alone does not close M-A12'})

if __name__=='__main__':main()
