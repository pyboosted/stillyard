import os,sys,subprocess,json
from pathlib import Path
from common import process,require,running_names,fixed_path
require(process(os.getpid())['in_job'],'control must be contained')
with subprocess.Popen([sys.executable,'-c','import os; os._exit(259)']) as child:
    require(child.wait(timeout=10)==259,'exit-259 control failed')
    try:process(child.pid)
    except RuntimeError:pass
    else:raise RuntimeError('dead process with exit 259 was accepted as live')
for raw in (b'',b'\xff\xfe','\ufeff\r\n'.encode('utf-16le')):
    require(running_names(raw)==[],'empty quiet listing misparsed')
require(running_names('\ufeffUbuntu-SSD\r\n'.encode('utf-16le'))==['Ubuntu-SSD'],'nonempty quiet listing misparsed')
fixed_path(Path(__file__))
try:fixed_path(r'\\wsl.localhost\Ubuntu-SSD\tmp\evidence')
except RuntimeError:pass
else:raise RuntimeError('WSL share accepted')
print(json.dumps({'exit_259_dead_process_rejected':True,'quiet_empty_and_bom_controls':True,'local_path_control':True,'wsl_share_rejected':True}))
