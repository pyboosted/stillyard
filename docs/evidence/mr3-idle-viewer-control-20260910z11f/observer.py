"""External five-minute idle observer: run only after all scheduled gates finish."""
import argparse,base64,hashlib,json,os,sqlite3,subprocess,time
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--windows-keepalive-pid',type=int,required=True)
parser.add_argument('--evidence-directory',type=Path,required=True)
args=parser.parse_args()
out=args.evidence_directory.resolve()
out.mkdir(parents=True,exist_ok=False)
if os.environ.get('STILLYARD_JOB_ID'):
 raise RuntimeError('idle observer must run outside Jobs so both queues can stay empty')
linux=Path.home()/'.local/share/stillyard/bin/stillyard'
windows='/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe'
ps='/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
winpython='/mnt/c/Users/User/AppData/Local/Programs/Python/Python313/python.exe'
def query(cli,*args):return json.loads(subprocess.check_output([str(cli),*args],timeout=20))
def save(name,value):(out/name).write_text(json.dumps(value,indent=2)+'\n')
def timer(doctor):
 checks=[c for c in doctor['checks'] if c['code']=='runtime_timer_expirations']
 if len(checks)!=1:raise RuntimeError('installed daemon lacks timer expiration diagnostics')
 return json.loads(checks[0]['summary'])
def sql_counts(path):
 with sqlite3.connect(f'file:{path}?mode=ro',uri=True) as db:
  return list(db.execute("select count(*),coalesce(max(rowid),0),sum(state!='final') from jobs").fetchone())
def snapshot():
 started=time.monotonic()
 docs={'linux':query(linux,'doctor','--json'),'windows':query(windows,'doctor','--json')}
 for side,d in docs.items():
  if d['daemon']['running_jobs'] or d['daemon']['queued_jobs']:raise RuntimeError('idle interval has Jobs: '+side)
  timer(d)
 counts={'linux':sql_counts(Path(docs['linux']['daemon']['store_path'])/'stillyard.sqlite3')}
 code="import sqlite3,json,sys; from pathlib import Path; raw=sys.argv[1]; raw=raw[4:] if raw.startswith(chr(92)*2+'?'+chr(92)) else raw; p=Path(raw)/'stillyard.sqlite3'; c=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True); print(json.dumps(c.execute(\"select count(*),coalesce(max(rowid),0),sum(state!='final') from jobs\").fetchone()))"
 counts['windows']=json.loads(subprocess.check_output([winpython,'-c',code,docs['windows']['daemon']['store_path']],timeout=20))
 alias=Path.home()/'.local/share/stillyard/interop.sock';target=alias.resolve(strict=True)
 unexpected_linux=[]
 if target.parent!=Path('/run/WSL') or not target.name.endswith('_interop'):raise RuntimeError('unexpected keepalive interop alias')
 initpid=int(target.name.removesuffix('_interop'));records=[]
 for proc in Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:
   fields=(proc/'stat').read_text().rsplit(')',1)[1].split();cg=(proc/'cgroup').read_text()
   try: executable=os.readlink(proc/'exe')
   except PermissionError: executable=None
   if executable==str(linux) and int(proc.name)!=docs['linux']['daemon']['pid']:
    unexpected_linux.append({'pid':int(proc.name),'start_ticks':int(fields[19]),'executable':executable})
   if 'stillyard.service/' not in cg and int(proc.name)!=initpid and int(fields[1])!=initpid:continue
   status=dict(line.split(':',1) for line in (proc/'status').read_text().splitlines() if ':' in line)
   threads={}
   for t in (proc/'task').iterdir():
    state=dict(line.split(':',1) for line in (t/'status').read_text().splitlines() if ':' in line)
    threads[t.name]={'voluntary':int(state['voluntary_ctxt_switches']),'involuntary':int(state['nonvoluntary_ctxt_switches'])}
   try: exe=os.readlink(proc/'exe')
   except PermissionError: exe='unavailable: ptrace permission'
   records.append({'pid':int(proc.name),'name':status['Name'].strip(),'start_ticks':int(fields[19]),'cpu_ticks':int(fields[11])+int(fields[12]),'rss_anon_kib':int(status['RssAnon'].split()[0]),'threads':threads,'exe':exe})
  except (FileNotFoundError,ProcessLookupError):pass
 if unexpected_linux:
  save('unexpected-linux-clients.json',unexpected_linux)
  raise RuntimeError('idle requires no external Linux Stillyard clients/subscribers')
 image=subprocess.check_output(['wslpath','-w',windows],text=True).strip().replace("'","''")
 command=f"$ErrorActionPreference='Stop'; $ProgressPreference='SilentlyContinue'; $owned=@(Get-CimInstance Win32_Process | Where-Object {{$_.ExecutablePath -ieq '{image}'}}); $ids=@($owned | Select-Object -ExpandProperty ProcessId) + {args.windows_keepalive_pid}; @(foreach($processId in $ids) {{$p=Get-Process -Id $processId; $threads=@(Get-CimInstance Win32_PerfRawData_PerfProc_Thread -Filter ('IDProcess = '+$processId) | Select-Object IDThread,ElapsedTime,ContextSwitchesPersec); [PSCustomObject]@{{pid=$p.Id; command_line=($owned | Where-Object {{$_.ProcessId -eq $processId}} | Select-Object -ExpandProperty CommandLine); threads=$threads; name=$p.ProcessName; cpu_seconds=$p.TotalProcessorTime.TotalSeconds; private_bytes=$p.PrivateMemorySize64; start_filetime=$p.StartTime.ToFileTimeUtc(); start_time=$p.StartTime.ToUniversalTime().ToString('o'); image=$p.Path}}}}) | ConvertTo-Json -Depth 4"
 native=json.loads(subprocess.check_output([ps,'-NoProfile','-EncodedCommand',base64.b64encode(command.encode('utf-16le')).decode()],timeout=60))
 if not isinstance(native,list):native=[native]
 cycle_source=Path(__file__).with_name('windows_process_cycles.py').read_text()
 (out/'windows_process_cycles.py').write_text(cycle_source)
 cycles=json.loads(subprocess.check_output([winpython,'-c',cycle_source,json.dumps([p['pid'] for p in native])],timeout=15))
 indexed={p['pid']:p for p in cycles}
 for p in native:
  if indexed[p['pid']]['creation_filetime']!=p['start_filetime']:raise RuntimeError('native PID reused during collection')
  p['process_cycles']=indexed[p['pid']]['cycles']
 unexpected=[p for p in native if p['pid'] not in (docs['windows']['daemon']['pid'],args.windows_keepalive_pid) and not (p.get('command_line') or '').strip().endswith(' machine bridge')]
 if unexpected:
  save('unexpected-native-clients.json',unexpected)
  raise RuntimeError('idle requires no external Stillyard clients/subscribers: '+str([p['pid'] for p in unexpected]))
 hashes={side:hashlib.sha256(Path(cli).read_bytes()).hexdigest() for side,cli in [('linux',linux),('windows',windows)]}
 return {'unix_ns':time.time_ns(),'capture_started_monotonic':started,'monotonic':time.monotonic(),'binary_sha256':hashes,'doctor':docs,'job_counts':counts,'linux':records,'windows':native,'interop_target':str(target)}
before=snapshot();save('before.json',before)
# Retain the exact observer used for this interval.
(out/'observer.py').write_bytes(Path(__file__).read_bytes())
print('idle baseline captured',flush=True)
for i in range(6):time.sleep(50);print('idle interval elapsed',50*(i+1),flush=True)
after=snapshot();save('after.json',after)
# Exclude both collection windows from the denominator. Counter deltas include
# those windows, making rate estimates conservative even when WMI is slow.
seconds=after['capture_started_monotonic']-before['monotonic'];assert seconds>=300
if before['job_counts']!=after['job_counts']:raise RuntimeError('Job history changed during idle interval')
if before['interop_target']!=after['interop_target']:raise RuntimeError('keepalive changed')
result={'duration_seconds':seconds,'processes':[],'daemon_timer_expirations':{},'limits':{'daemon_cpu_percent':.5,'daemon_memory_mib':40,'bridge_cpu_percent':.1,'bridge_memory_mib':16,'aggregate_cpu_percent':1.1,'aggregate_memory_mib':96,'aggregate_timer_expirations_per_minute':6},'helper_timer_coverage':'Daemon counters cover application-owned timer expirations. Helper event-only waits require separate source audit; context switches are retained only as diagnostics, never relabeled as timer counts.'}
for side,cpu,scale,mem,memscale,identity in [('linux','cpu_ticks',os.sysconf('SC_CLK_TCK'),'rss_anon_kib',1024,'start_ticks'),('windows','cpu_seconds',1,'private_bytes',1048576,'start_time')]:
 prior={p['pid']:p for p in before[side]}
 for p in after[side]:
  b=prior.pop(p['pid'],None)
  if b is None or b[identity]!=p[identity]:raise RuntimeError('process identity changed')
  item={'side':side,'pid':p['pid'],'name':p['name'],'cpu_percent_one_core':100*(p[cpu]-b[cpu])/scale/seconds,'memory_mib':max(p[mem],b[mem])/memscale}
  if side=='linux':
   if p['threads'].keys()!=b['threads'].keys():raise RuntimeError('thread identity changed')
   item['voluntary_switches']=sum(v['voluntary']-b['threads'][k]['voluntary'] for k,v in p['threads'].items())
  if side=='windows':
   old_threads={str(t['IDThread']):t for t in b['threads']}
   new_threads={str(t['IDThread']):t for t in p['threads']}
   if old_threads.keys()!=new_threads.keys() or any(t['ElapsedTime']!=old_threads[k]['ElapsedTime'] for k,t in new_threads.items()):
    raise RuntimeError('native thread identity changed')
   deltas=[int(t['ContextSwitchesPersec'])-int(old_threads[k]['ContextSwitchesPersec']) for k,t in new_threads.items()]
   if any(d<0 for d in deltas):raise RuntimeError('native raw thread counter moved backwards')
   item['thread_context_switches']=sum(deltas)
   item['process_cycles']=p['process_cycles']-b['process_cycles']
   if item['process_cycles']<0:raise RuntimeError('native process cycle counter moved backwards')
  item['identity_before']=b[identity]
  item['identity_after']=p[identity]
  if p['pid']==before['doctor'][side]['daemon']['pid']:
   item['role']='executor_daemon' if side=='linux' else 'coordinator_daemon'
  elif side=='windows':
   item['role']='windows_keepalive' if p['pid']==args.windows_keepalive_pid else 'native_bridge'
  elif p['name']=='stillyard.exe':item['role']='interop_proxy'
  elif p['pid']==int(Path(before['interop_target']).name.removesuffix('_interop')):item['role']='interop_init'
  elif p['name'].startswith('python'):item['role']='python_keepalive_or_supervisor'
  else:raise RuntimeError('unclassified helper process: '+str(p['pid']))
  result['processes'].append(item)
 if prior:raise RuntimeError('process disappeared during idle measurement')
 bd,ad=before['doctor'][side],after['doctor'][side]
 if bd['daemon']['daemon_generation']!=ad['daemon']['daemon_generation']:raise RuntimeError('daemon restarted')
 delta={k:v-timer(bd)[k] for k,v in timer(ad).items()}
 if any(v<0 for v in delta.values()):raise RuntimeError('timer counters moved backwards')
 applicable=['reactor','subscriber','backoff'] if side=='windows' else ['reactor','attached','subscriber','transport','backoff']
 result.setdefault('applicable_timers',{})[side]=applicable
 result.setdefault('inapplicable_buckets',{})[side]={k:v for k,v in delta.items() if k not in applicable}
 result['daemon_timer_expirations'][side]={k:delta[k] for k in applicable}
 result.setdefault('daemon_generations',{})[side]=ad['daemon']['daemon_generation']
result['aggregate_cpu_percent']=sum(p['cpu_percent_one_core'] for p in result['processes'])
result['aggregate_memory_mib']=sum(p['memory_mib'] for p in result['processes'])
result['daemon_timer_expirations_per_minute']=sum(sum(v.values()) for v in result['daemon_timer_expirations'].values())*60/seconds
result['preconditions']={'unchanged_job_history':before['job_counts']==after['job_counts'],'job_counts':after['job_counts'],'unchanged_interop':True,'unchanged_process_identities':True,'unchanged_daemon_generations':True}
result['helper_limits']='Helpers other than the bridge are included in the aggregate CPU/memory budget; no separate per-helper bound is specified.'
result['aggregate_timer_status']='pending helper timer coverage; daemon sum alone is not aggregate acceptance'
checks=[]
for p in result['processes']:
 if p['role'] in ('executor_daemon','coordinator_daemon'):limits=(.5,40)
 elif p['role']=='native_bridge':limits=(.1,16)
 else:continue
 checks.append({'role':p['role'],'cpu_pass':p['cpu_percent_one_core']<=limits[0],'memory_pass':p['memory_mib']<=limits[1]})
result['cpu_memory_checks']=checks
result['cpu_memory_pass']=(all(c['cpu_pass'] and c['memory_pass'] for c in checks) and result['aggregate_cpu_percent']<=1.1 and result['aggregate_memory_mib']<=96)
save('result.json',result);print(json.dumps(result),flush=True)
if not result['cpu_memory_pass']:raise RuntimeError('installed idle CPU/memory budget exceeded')

# Extra observation outside the measured 300-second interval: let every bounded
# native bridge request (<=30 s) settle, then reject any delayed teardown.
print('idle interval complete; settling bounded bridge requests',flush=True)
time.sleep(35)
settled=snapshot();save('settled.json',settled)
for side,identity in [('linux','start_ticks'),('windows','start_time')]:
 if {(p['pid'],p[identity]) for p in after[side]}!={(p['pid'],p[identity]) for p in settled[side]}:
  raise RuntimeError('process identity changed after the interval boundary')
 if settled['doctor'][side]['daemon']['daemon_generation']!=after['doctor'][side]['daemon']['daemon_generation']:
  raise RuntimeError('daemon generation changed while settling')
if settled['job_counts']!=after['job_counts']:
 raise RuntimeError('Jobs were submitted while settling')
for name in ['transport','backoff']:
 if timer(settled['doctor']['linux'])[name]!=timer(after['doctor']['linux'])[name]:
  raise RuntimeError('delayed bridge deadline/reconnect after interval boundary')
save('settlement.json',{'passed':True,'duration_seconds':settled['monotonic']-after['monotonic'],'unchanged_process_identities':True,'unchanged_daemon_generations':True,'unchanged_job_history':True,'delayed_transport_or_backoff_expirations':0})
print('boundary settlement passed',flush=True)
