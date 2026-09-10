import subprocess,json,winreg,os
from pathlib import Path
p=Path(r'C:\Development\stillyard-mr3-test-distro-20260910a');name='Stillyard-MR3-Test'
wsl=r'C:\Windows\System32\wsl.exe'
raw=subprocess.check_output([wsl,'--list','--quiet'],timeout=30)
known=[s.strip() for s in raw.decode('utf-16le').replace('\ufeff','').splitlines() if s.strip()]
assert name not in known,'refusing existing distribution'
assert os.environ.get('STILLYARD_JOB_ID'),'requires native default Job'
location=p/'distro';assert not location.exists(),'refusing existing import location'
result=subprocess.run([wsl,'--import',name,str(location),str(p/'ubuntu-26.04.1-wsl-amd64.wsl'),'--version','2'],capture_output=True,timeout=180)
(p/'import-output.json').write_text(json.dumps({'job':os.environ['STILLYARD_JOB_ID'],'exit_code':result.returncode,'stdout_hex':result.stdout.hex(),'stderr_hex':result.stderr.hex()},indent=2))
result.check_returncode()
rows=[]
with winreg.OpenKey(winreg.HKEY_CURRENT_USER,r'Software\Microsoft\Windows\CurrentVersion\Lxss') as base:
 default=winreg.QueryValueEx(base,'DefaultDistribution')[0]
 for i in range(winreg.QueryInfoKey(base)[0]):
  guid=winreg.EnumKey(base,i)
  with winreg.OpenKey(base,guid) as key:
   rows.append({'guid':guid,'name':winreg.QueryValueEx(key,'DistributionName')[0],'base_path':winreg.QueryValueEx(key,'BasePath')[0],'default':guid==default})
selected=next(r for r in rows if r['name']==name)
assert Path(selected['base_path'])==location
assert next(r for r in rows if r['default'])['name']=='Ubuntu-SSD'
(p/'owned-distro.json').write_text(json.dumps({'created_by_job':os.environ['STILLYARD_JOB_ID'],'target':selected,'registered':rows,'purpose':'disposable MR-3 distro termination acceptance; never whole-VM shutdown'},indent=2))
print('Imported own disposable distro; default remains Ubuntu-SSD',flush=True)
