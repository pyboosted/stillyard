import json,os,subprocess,time
from pathlib import Path
root=Path(__file__).parent
ps='/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
def snapshot():
 records=[]
 for p in Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   cg=(p/'cgroup').read_text()
   if 'stillyard.service/' not in cg and p.name not in ('3989858','3989859'):continue
   stat=(p/'stat').read_text().rsplit(')',1)[1].split()
   status=dict((line.split(':',1)[0],line.split(':',1)[1].strip()) for line in (p/'status').read_text().splitlines() if ':' in line)
   threads={}
   for t in (p/'task').iterdir():
    state=dict(line.split(':',1) for line in (t/'status').read_text().splitlines() if ':' in line)
    threads[t.name]={'voluntary':int(state['voluntary_ctxt_switches']),'involuntary':int(state['nonvoluntary_ctxt_switches'])}
   records.append({'pid':int(p.name),'name':status['Name'],'start_ticks':int(stat[19]),'cpu_ticks':int(stat[11])+int(stat[12]),'rss_anon_kib':int(status.get('RssAnon','0 kB').split()[0]),'threads':threads})
  except (FileNotFoundError,ProcessLookupError):pass
 command="@(Get-Process stillyard -ErrorAction Stop; Get-Process -Id 32592 -ErrorAction Stop) | Select-Object Id,ProcessName,@{n='CPU';e={$_.TotalProcessorTime.TotalSeconds}},@{n='PrivateBytes';e={$_.PrivateMemorySize64}},@{n='StartTime';e={$_.StartTime.ToUniversalTime().ToString('o')}} | ConvertTo-Json -Depth 3"
 native=json.loads(subprocess.check_output([ps,'-NoProfile','-Command',command],timeout=15))
 return {'unix_ns':time.time_ns(),'monotonic':time.monotonic(),'linux':records,'windows':native}
before=snapshot();(root/'before.json').write_text(json.dumps(before,indent=2));print('idle sampling started',flush=True)
time.sleep(300)
after=snapshot();(root/'after.json').write_text(json.dumps(after,indent=2))
seconds=after['monotonic']-before['monotonic'];result={'duration_seconds':seconds,'processes':[],'timer_wakes':'not measured: context-switch deltas are upper-bound diagnostics only'}
for side,key,cpu,scale,mem in [('linux','pid','cpu_ticks',os.sysconf('SC_CLK_TCK'),'rss_anon_kib'),('windows','Id','CPU',1,'PrivateBytes')]:
 prior={p[key]:p for p in before[side]}
 for p in after[side]:
  b=prior.pop(p[key],None)
  if b is None:raise RuntimeError('process changed during idle measurement')
  item={'side':side,'pid':p[key],'cpu_percent_one_core':100*(p[cpu]-b[cpu])/scale/seconds,'memory_mib':max(p[mem],b[mem])/(1024 if side=='linux' else 1048576)}
  if side=='linux':item['voluntary_switches']=sum(v['voluntary']-b['threads'][k]['voluntary'] for k,v in p['threads'].items())
  result['processes'].append(item)
 if prior:raise RuntimeError('process disappeared during idle measurement')
result['aggregate_cpu_percent']=sum(p['cpu_percent_one_core'] for p in result['processes']);result['aggregate_memory_mib']=sum(p['memory_mib'] for p in result['processes'])
(root/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
