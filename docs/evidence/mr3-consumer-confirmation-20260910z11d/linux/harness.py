import hashlib,json,subprocess,time,uuid,shutil
from pathlib import Path
root=Path('/home/pythonic/Development/stillyard');base=Path('/home/pythonic/Development/stillyard-consumers-20260910z11d');base.mkdir(exist_ok=False)
old=Path('/home/pythonic/Development/stillyard-consumers-20260910z3');linux_source=Path('/home/pythonic/Development/stillyard-mr3-membership-20260910z11d');windows_source=Path('/mnt/c/Development/stillyard-mr3-membership-20260910z11d');scratch=Path('/home/pythonic/Development/stillyard-wc3-scratch-20260910z11d');manifest=Path('/mnt/c/Development/stillyard-mr3-membership-evidence-20260910z11d/source.json');m=json.loads(manifest.read_text());wcli='/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe';lcli=str(Path.home()/'.local/share/stillyard/bin/stillyard')
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def query(cli,*args):return json.loads(subprocess.check_output([cli,*args],timeout=15))
def winpath(p):return subprocess.check_output(['wslpath','-w',str(p)],text=True).strip()
for s in (linux_source,windows_source,scratch):assert all(hashlib.sha256((s/n).read_bytes()).hexdigest()==v for n,v in m['files'].items())
for consumer in ('wc2','wc3','wc4'):
 prior=next(old.glob(consumer+'-round4-*'));d=base/consumer;d.mkdir();shutil.copyfile(root/'scripts/machine-resource-consumer.py',d/'consumer.py');save(d/'source.json',m)
 spec=json.loads((prior/'spec.json').read_text().replace(str(prior),str(d)).replace('wc3-round4','wc3-z11d-confirmation').replace('/home/pythonic/Development/stillyard-mr3-quiet-retry-20260910z3',str(linux_source)))
 for label in spec['labels']:
  if label['key']=='round':label['value']='z11d-confirmation'
 if consumer=='wc2':
  brief='Read-only consumer review of the installed Stillyard native bootstrap companion and its acceptance control. Check the current-primary role and finite-timeout validation before spawn; Linux and native success conjunction; ordinary descendant containment; narrow public mailbox export. Give concrete evidence-backed findings only. Do not assume unprovided code or invoke tools. This is a live W-C2 consumer run, not a declaration of full MR3 acceptance.\n'
  for name in ('src/bootstrap_controller.rs','tests/support/cross_os_reset.rs'):
   brief+='\nFILE '+name+'\n'+(linux_source/name).read_text()
  (d/'brief.txt').write_text(brief)
 if consumer=='wc3':
  spec['args'][1] += '\nFor each Bash invocation set timeout=600000 ms. Wait for completion before the second call and before returning. If Bash still returns a background task ID, use TaskOutput with block=true and timeout=300000 until it completes. Never replace waiting with a shell polling command.'
  spec['args'][spec['args'].index('--tools')+1]='Bash,TaskOutput'
  spec['args'][spec['args'].index('--allowedTools')+1]+=',TaskOutput'
  spec['postconditions']=[{'executable':'/usr/bin/python3','args':[str(d/'consumer.py'),'validate-managed-build','--cli',lcli,'--evidence-directory',str(d/'child-evidence')]}]
  child=json.loads((prior/'child.json').read_text().replace('/home/pythonic/Development/stillyard-wc3-scratch-20260910z3',str(scratch)));save(d/'child.json',child)
 save(d/'spec.json',spec);save(d/'intent.json',{'idempotency_key':str(uuid.uuid4())})
shutil.copyfile(__file__,base/'harness.py')
save(base/'plan.json',{'source_files_sha256':m['files_sha256'],'purpose':'one final-installed z11d confirmation round after three accepted z3 rounds','measurement':'256 SHA256 passes over verified nonempty source; finite positive cpu and wall durations','expected_machine_cargo_slots':1,'scratch_cache':'reused current scratch cache, moved from z8 through unsubmitted d; no copy'})
print('PREPARED',base,flush=True)
pair={side:query(cli,'daemon-status') for side,cli in [('windows',wcli),('linux',lcli)]};save(base/'pair-before.json',pair)
assert all(not x['running_jobs'] and not x['queued_jobs'] for x in pair.values());assert pair['windows']['capacities']['cargo_slots']==1
processes=[]
def spawn(label,cmd,directory):
 directory.mkdir(parents=True,exist_ok=True)
 with (directory/'launcher.stdout').open('wb') as o,(directory/'launcher.stderr').open('wb') as e:p=subprocess.Popen(cmd,stdout=o,stderr=e)
 processes.append((label,p));return p
native=Path('/mnt/c/Development/stillyard-mr3-consumer-confirmation-evidence-20260910z11d');native.mkdir(exist_ok=False)
spawn('W-C1-Windows',['powershell.exe','-NoProfile','-ExecutionPolicy','Bypass','-File',winpath(windows_source/'scripts/run-stillyard-job.ps1'),'test','-RepositoryRoot',winpath(windows_source),'-EvidenceDirectory',winpath(native)],native)
# Start the actual agent early enough for its child to wait on Windows Cargo.
for consumer in ('wc3','wc2','wc4'):
 d=base/consumer;key=json.loads((d/'intent.json').read_text())['idempotency_key'];spawn(consumer,[lcli,'--endpoint',pair['linux']['endpoint'],'ensure','--spec',str(d/'spec.json'),'--idempotency-key',key,'--result-file',str(d/'receipt.json'),'--wait','--deadline-seconds','1200'],d)
spawn('W-C1-Linux',['python3',str(root/'scripts/run-wsl-job.py'),'test','--repository-root',str(linux_source),'--source-manifest',str(manifest),'--evidence-directory',str(base/'linux-gate')],base/'linux-gate')
end=time.monotonic()+1300
while any(p.poll() is None for _,p in processes):
 if time.monotonic()>end:raise RuntimeError('consumer controller deadline; receipts retained, Jobs not resubmitted')
 child_receipts=list((base/'wc3/child-evidence').glob('*.receipt.json'))
 if child_receipts and not (base/'managed-wait-during-windows.json').exists():
  child=json.loads(child_receipts[0].read_text())['receipt']['accepted']['job_id'];status=query(lcli,'status',child)
  native_receipts=list(native.glob('*receipt.json'))
  if native_receipts:
   wjob=json.loads(native_receipts[0].read_text(encoding='utf-8-sig'))['receipt']['accepted']['job_id'];wstatus=query(wcli,'status',wjob)
   if status['state']=='pending' and wstatus['state']=='active':save(base/'managed-wait-during-windows.json',{'child':status,'windows':wstatus})
 time.sleep(1)
save(base/'client-exits.json',{name:p.returncode for name,p in processes})
receipts=[('windows',p) for p in native.glob('*receipt.json')]+[('linux',p) for p in base.rglob('*receipt.json')]
seen=set();jobs=[]
for side,p in receipts:
 receipt=json.loads(p.read_text(encoding='utf-8-sig'));j=receipt['receipt']['accepted']['job_id']
 if j in seen:continue
 seen.add(j);cli=wcli if side=='windows' else lcli;status=query(cli,'status',j);save(base/(j.split('~')[1]+'.status.json'),status);jobs.append({'side':side,'job_id':j,'outcome':status['outcome'],'receipt':str(p)})
 assert status['state']=='final' and status['outcome']=='succeeded',(j,status['outcome'])
callfiles=list((base/'wc3/child-evidence').glob('call-*.json'));calls=[json.loads(p.read_text()) for p in callfiles];assert len(calls)==2 and all(c['completed'] and c['exit_code']==0 for c in calls);assert len({c['idempotency_key'] for c in calls})==1
assert (base/'managed-wait-during-windows.json').exists(),'no actual managed wait overlapping Windows was captured'
measurement=json.loads((base/'wc4/measurement.json').read_text());assert measurement['rounds']==256 and measurement['source_files_sha256']==m['files_sha256']
save(base/'result.json',{'jobs':jobs,'managed_calls':len(calls),'one_child':True,'measurement':measurement,'source_files_sha256':m['files_sha256']})
print('CONSUMERS SUCCEEDED',json.dumps(jobs),flush=True)
