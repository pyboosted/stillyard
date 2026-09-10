import json,subprocess,winreg,os,time
from pathlib import Path
p=Path(r'C:\Development\stillyard-mr3-test-distro-20260910a')
name='Stillyard-MR3-Test';guid='{1cf1e672-7ffd-4a75-a2b5-c6dc6b127450}'
with winreg.OpenKey(winreg.HKEY_CURRENT_USER,r'Software\Microsoft\Windows\CurrentVersion\Lxss') as base:
 assert winreg.QueryValueEx(base,'DefaultDistribution')[0]!=guid
 with winreg.OpenKey(base,guid) as key:
  assert winreg.QueryValueEx(key,'DistributionName')[0]==name
  assert Path(winreg.QueryValueEx(key,'BasePath')[0])==p/'distro'
assert (p/'distro/ext4.vhdx').stat().st_size==12582912,'partial image changed; investigate before cleanup'
assert not (p/'owned-distro.json').exists(),'import completed; do not adopt failed-import cleanup'
result={'job':os.environ['STILLYARD_JOB_ID'],'target':name,'guid':guid,'operation':'unregister own failed import only'}
start=time.monotonic()
try:
 r=subprocess.run([r'C:\Windows\System32\wsl.exe','--unregister',name],capture_output=True,timeout=30)
 result.update(exit_code=r.returncode,stdout_hex=r.stdout.hex(),stderr_hex=r.stderr.hex())
except subprocess.TimeoutExpired as e:
 result.update(timeout=True,stdout_hex=(e.stdout or b'').hex(),stderr_hex=(e.stderr or b'').hex())
finally:
 result['elapsed_seconds']=time.monotonic()-start
 (p/'failed-import-cleanup-result.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result),flush=True)
raise SystemExit(1 if result.get('timeout') else result['exit_code'])
