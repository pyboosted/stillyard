import os,subprocess,json
from pathlib import Path
assert os.environ.get("STILLYARD_JOB_ID")
path="/usr/bin/printf"
normal=subprocess.run([path,"scheduled coreutils control\\n"],capture_output=True,text=True)
code="import os; fd=os.open('/usr/bin/printf',os.O_RDONLY); os.execve(fd,['/usr/bin/printf','scheduled coreutils control\\\\n'],dict(os.environ))"
fd=subprocess.run(['/usr/bin/python3','-c',code],capture_output=True,text=True)
result={'job':os.environ['STILLYARD_JOB_ID'],'normal_path':{'exit':normal.returncode,'stdout':normal.stdout,'stderr':normal.stderr},'fd_exec_same_argv0':{'exit':fd.returncode,'stdout':fd.stdout,'stderr':fd.stderr}}
Path('result.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
assert normal.returncode==0 and normal.stdout.strip()=='scheduled coreutils control'
assert fd.returncode!=0 and "unknown program" in fd.stderr
