import json,pathlib,subprocess,time,uuid
root=pathlib.Path(__file__).parent
native=pathlib.Path('/mnt/c/Development/stillyard-mr3-priority-evidence-20260910z3b');native.mkdir(exist_ok=True)
lin='/home/pythonic/.local/share/stillyard/bin/stillyard';win='/mnt/c/Users/User/AppData/Local/stillyard/Stillyard/bin/stillyard.exe'
def save(name,v):(root/name).write_text(json.dumps(v,indent=2))
def query(cli,*args):return json.loads(subprocess.check_output([cli,*args],timeout=15))
def wp(p):return subprocess.check_output(['wslpath','-w',str(p)],text=True).strip()
def submit(name,windows,priority,cargo,seconds=0):
 directory=(native if windows else root)/name;directory.mkdir(mode=0o700,exist_ok=True)
 code="import time;print('"+name+"',flush=True);time.sleep("+str(seconds)+")"
 s={'spec_version':4,'priority':priority,'executable':r'C:\Users\User\AppData\Local\Programs\Python\Python313\python.exe' if windows else '/usr/bin/python3','args':['-c',code],'working_directory':wp(directory) if windows else str(directory),'timeout_seconds':120,'resources':{'cargo_slots':cargo} if cargo else {},'labels':[{'key':'project','value':'stillyard'},{'key':'gate','value':'installed-priority-'+name}]}
 (directory/'spec.json').write_text(json.dumps(s,indent=2));key=json.loads((directory/'intent.json').read_text())['idempotency_key'] if (directory/'intent.json').exists() else str(uuid.uuid4());(directory/'intent.json').write_text(json.dumps({'idempotency_key':key}));cli=win if windows else lin
 subprocess.run([cli,'--endpoint',pair['windows' if windows else 'linux']['endpoint'],'ensure','--spec',wp(directory/'spec.json') if windows else str(directory/'spec.json'),'--idempotency-key',key,'--result-file',wp(directory/'receipt.json') if windows else str(directory/'receipt.json'),'--deadline-seconds','30'],stdout=subprocess.DEVNULL,check=True)
 j=json.loads((directory/'receipt.json').read_text(encoding='utf-8-sig'))['receipt']['accepted']['job_id'];print(name,j,flush=True);return cli,j,directory
def status(job):return query(job[0],'status',job[1])
def wait(job,pred,seconds):
 end=time.monotonic()+seconds
 while True:
  s=status(job)
  if pred(s):return s
  if time.monotonic()>end:raise RuntimeError('status deadline '+job[1])
  time.sleep(.1)
pair={'windows':query(win,'daemon-status'),'linux':query(lin,'daemon-status')};save('pair-before.json',pair)
if pair['windows']['capacities']['cargo_slots']!=1:raise RuntimeError('one machine token required')
holder=submit('holder',True,0,1,75);wait(holder,lambda s:s['state']=='active',10)
blocked=submit('older-blocked',False,1,1);since=time.monotonic();wait(blocked,lambda s:s['state']=='pending',10)
free_win=submit('free-windows',True,0,0);free_lin=submit('free-linux',False,0,0)
a=wait(free_win,lambda s:s['state']=='final',10);b=wait(free_lin,lambda s:s['state']=='final',10)
assert a['outcome']==b['outcome']=='succeeded' and status(blocked)['state']=='pending' and status(holder)['state']=='active'
save('no-head-blocking.json',{'holder':status(holder),'blocked':status(blocked),'free_windows':a,'free_linux':b})
time.sleep(max(0,65-(time.monotonic()-since)))
aged=status(blocked);save('aged.json',aged);assert aged['state']=='pending' and aged['effective_priority']>=2,aged.get('effective_priority')
new=submit('newer-priority-2',True,2,1)
for job in [holder,blocked,free_win,free_lin,new]:
 final=wait(job,lambda s:s['state']=='final',120);save(job[2].name+'-final.json',final);assert final['outcome']=='succeeded'
old_start=status(blocked)['attempts'][0]['invocations'][0]['started_unix_millis'];new_start=status(new)['attempts'][0]['invocations'][0]['started_unix_millis'];assert old_start<new_start
save('verdict.json',{'passed':True,'older_job':blocked[1],'newer_job':new[1],'older_start':old_start,'newer_start':new_start,'aged_priority':aged['effective_priority'],'jobs':{j[2].name:j[1] for j in [holder,blocked,free_win,free_lin,new]}});print('priority/aging/no-head-blocking passed',flush=True)
