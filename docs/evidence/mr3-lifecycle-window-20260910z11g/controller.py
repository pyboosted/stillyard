"""Native default-Job lifecycle control. Only preflight is authorized so far."""
import argparse, json, os, subprocess, time, sys
from pathlib import Path
from common import *

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--operation',choices=['preflight','terminate','shutdown'],default='preflight')
    p.add_argument('--evidence-directory',type=Path,required=True)
    p.add_argument('--approval-file',type=Path)
    p.add_argument('--exchange-directory',type=Path)
    args=p.parse_args()
    require(os.name=='nt' and os.environ.get('STILLYARD_JOB_ID') and os.environ.get('STILLYARD_INVOCATION_ID'),'requires a native default Stillyard Invocation')
    out=args.evidence_directory.resolve();out.mkdir(parents=True,exist_ok=False)
    fault=False
    sources={n:digest(Path(__file__).with_name(n)) for n in ['controller.py','starter.py','common.py','inventory.py']}
    save(out/'sources.json',sources)
    def native(*cmd):return json.loads(subprocess.check_output([str(WCLI),*cmd],timeout=30))
    def running():
        return sorted(s.strip() for s in subprocess.check_output([str(WSL),'--list','--running','--quiet'],timeout=30).decode('utf-16le').replace('\x00','').splitlines() if s.strip())
    def linux(*cmd):
        require('Ubuntu-SSD' in running(),'observer refuses to cold-start the stopped distro')
        return subprocess.check_output([str(WSL),'-d','Ubuntu-SSD','-u','pythonic','--exec',*cmd],timeout=30)
    def snapshot():
        return {'windows':native('daemon-status'),'linux':json.loads(linux(LCLI,'daemon-status')),
                'executor':json.loads(linux('/usr/bin/python3','-c',(Path(__file__).parent/'inventory.py').read_text())),
                'authority':native('authority','status'),'distributions':running()}
    def empty(state):
        win,guest,public,authority=[state[k] for k in ('windows','linux','executor','authority')]
        require(win['running_jobs']==1 and win['queued_jobs']==0 and guest['running_jobs']==0 and guest['queued_jobs']==0,'queues are not quiescent')
        require(public['active_leases']==0 and public['nonempty_containments']==0 and not public['unsealed_invocations'],'retained executor obligations')
        require(public['kernel']['events'].get('populated')=='0' and not public['kernel']['children'],'executor cgroup is not empty')
        require(not [h for h in authority['holds'] if h['released'] is not True] and not authority['machine_obligations'],'authority obligations remain')
        require(authority['blocker'] is None and guest['machine_scheduling']['blocker'] is None,'pair is not healthy')
    try:
        own=native('status',os.environ['STILLYARD_JOB_ID'])
        inv=[i for a in own['attempts'] for i in a['invocations'] if i['invocation_id']==os.environ['STILLYARD_INVOCATION_ID']]
        require(len(inv)==1 and inv[0]['role']=='primary' and inv[0]['root_identity']['pid']==os.getpid(),'controller is not the actual primary')
        identity=process(os.getpid());require(identity['in_job'],'controller is not in a Windows Job')
        save(out/'native-controller-identity.json',identity)
        before=snapshot();save(out/'before.json',before)
        for side in ('windows','linux'):
            require(isinstance(before[side]['store_uuid'],str) and isinstance(before[side]['daemon_generation'],str) and isinstance(before[side]['machine_scheduling']['domains'],dict),'unexpected daemon identity schema')
        store=before['windows']['store_path'];store=store[4:] if store.startswith('\\\\?\\') else store
        require(Path(store)==WCLI.parent.parent/'data','not the default native Store')
        save(out/'windows-doctor.json',native('doctor','--json'))
        save(out/'linux-doctor.json',json.loads(linux(LCLI,'doctor','--json')))
        if args.operation=='preflight':
            print('Read-only preflight passed',out,flush=True);return
        require(args.approval_file and args.exchange_directory,'requires separately agreed scope and external starter')
        approval=json.loads(args.approval_file.read_text());approval_hash=digest(args.approval_file)
        require(approval.get('user_agreement') and approval.get('no_new_submissions') is True,'interruption window is not agreed')
        require(args.operation in approval['operations'],'operation is outside agreed scope')
        expected=sorted(approval['running_distributions'])
        require(before['distributions']==expected,'running distributions differ from the approved inventory')
        require(before['executor']['foreign_work_candidates']==approval['foreign_work_candidates'],'foreign work differs from approved inventory')
        exchange=args.exchange_directory.resolve();ready=wait_json(exchange/'ready.json',time.monotonic()+10)
        require(ready['approval_sha256']==approval_hash and ready['starter_sha256']==digest(Path(__file__).with_name('starter.py')),'external starter does not match approval or reviewed source')
        require(process(ready['identity']['pid'])==ready['identity'] and not ready['identity']['in_job'],'external owner is not alive outside Jobs')
        require(Path(ready['identity']['image']).resolve()==Path(sys.executable).resolve(),'external starter image differs from native Python')
        require(not (exchange/'start-request.json').exists(),'exchange already used')
        empty(before);immediate=snapshot();empty(immediate);save(out/'immediate-barrier.json',immediate)
        require(immediate['distributions']==expected and immediate['executor']['foreign_work_candidates']==approval['foreign_work_candidates'],'inventory changed before fault')
        require(immediate['executor']['journal_sha256']==before['executor']['journal_sha256'],'journal changed before fault')
        for side in ('windows','linux'):
            require(immediate[side]['daemon_generation']==before[side]['daemon_generation'],'daemon changed before fault')
        command=[str(WSL),'--terminate','Ubuntu-SSD'] if args.operation=='terminate' else [str(WSL),'--shutdown']
        save(out/'fault-intent.json',{'command':command,'approval_sha256':approval_hash,'unix_ns':time.time_ns()})
        start=time.monotonic();fault=True
        fault_filetime=time.time_ns()//100+116444736000000000
        result=subprocess.run(command,capture_output=True,timeout=90)
        save(out/'fault-result.json',{'exit_code':result.returncode,'stdout_hex':result.stdout.hex(),'stderr_hex':result.stderr.hex(),'elapsed_seconds':time.monotonic()-start})
        require(result.returncode==0,'fault command failed')
        stop_deadline=time.monotonic()+60
        while True:
            stopped=running()
            if 'Ubuntu-SSD' not in stopped and (args.operation!='shutdown' or not stopped):break
            require(time.monotonic()<stop_deadline,'requested scope did not stop');time.sleep(1)
        save(out/'stopped-distributions.json',stopped)
        require('Ubuntu-SSD' not in stopped and (args.operation!='shutdown' or not stopped),'requested scope did not stop')
        save(exchange/'start-request.json',{'operation':args.operation,'controller_job':own['job_id'],'approval_sha256':approval_hash})
        until=time.monotonic()+600;launched=wait_json(exchange/'launched.json',until)
        require(launched['request']['controller_job']==own['job_id'] and launched['request']['approval_sha256']==approval_hash,'wrong recovery controller')
        require(launched['starter']==ready['identity'],'external starter identity changed')
        require(Path(launched['identity']['image']).resolve()==WSL.resolve() and launched['identity']['creation_time']>=fault_filetime,'keepalive is not a new native wsl.exe')
        keeper=None
        while keeper is None:
            require(process(launched['identity']['pid'])==launched['identity'] and not launched['identity']['in_job'],'external keepalive is no longer live')
            path=exchange/'keepalive.stdout'
            if path.exists():
                for line in path.read_text(errors='replace').splitlines():
                    try:value=json.loads(line)
                    except ValueError:continue
                    if value.get('kind')=='wsl_keepalive':keeper=value
            require(time.monotonic()<until,'external keepalive did not publish its own startup record')
            if keeper is None:time.sleep(1)
        save(out/'external-keepalive.json',{'linux':keeper,'windows':launched,'starter':ready})
        # Only now may observers enter the distro; the keepalive itself cold-started it.
        while True:
            require(process(launched['identity']['pid'])==launched['identity'],'external keepalive died during recovery')
            try:
                after=snapshot()
                if after['linux']['machine_scheduling']['blocker'] is None:break
            except (subprocess.CalledProcessError,subprocess.TimeoutExpired,json.JSONDecodeError):
                pass
            require(time.monotonic()<until,'pair failed to reconnect');time.sleep(2)
        empty(after);save(out/'after.json',after)
        require(keeper['pid'] in [h['pid'] for h in after['executor']['keepalive_processes']], 'reported Linux helper is not the actual installed keepalive under its interop init')
        require(after['executor']['boot_id']==keeper['boot_id'] and after['executor']['interop_target']==keeper['interop_server'],'live interop owner differs from the new external keepalive')
        require(after['executor']['journal_sha256']==before['executor']['journal_sha256'] and after['executor']['binary_sha256']==before['executor']['binary_sha256'],'installed image or sealed history changed')
        for side in ('windows','linux'):
            require(after[side]['store_uuid']==before[side]['store_uuid'] and after[side]['machine_scheduling']['domains']==before[side]['machine_scheduling']['domains'],'Store or machine identity changed')
        require(after['windows']['daemon_generation']==before['windows']['daemon_generation'] and after['linux']['daemon_generation']!=before['linux']['daemon_generation'],'unexpected daemon lifetime result')
        if args.operation=='shutdown':require(after['executor']['boot_id']!=before['executor']['boot_id'],'whole VM shutdown did not change Linux boot')
        # Distro terminate can retain the shared VM kernel boot when another distro lives.
        require(process(launched['identity']['pid'])==launched['identity'],'external keepalive disappeared before completion')
        save(out/'result.json',{'quiescent_restart_passed':True,'operation':args.operation,'controller_job':own['job_id'],'external_keepalive':keeper,'canaries':'submit separately only AFTER this controller is final; M-A12 remains incomplete without them'})
    except BaseException as error:
        save(out/'failure.json',{'error':str(error),'fault_attempted':fault,'unix_ns':time.time_ns(),'external_owner':'inspect exchange evidence; no child is detached or force-killed by this controller'})
        raise

if __name__=='__main__':main()
