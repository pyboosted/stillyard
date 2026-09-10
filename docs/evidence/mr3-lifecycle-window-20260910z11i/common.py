import ctypes, hashlib, json, os, time
from ctypes import wintypes as w
from pathlib import Path

WCLI = Path(r'C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe')
WSL = Path(r'C:\Windows\System32\wsl.exe')
LROOT = '/home/pythonic/.local/share/stillyard'
LCLI = LROOT + '/bin/stillyard'
HELPER = LROOT + '/libexec/wsl-service.py'

def require(ok, message):
    if not ok:
        raise RuntimeError(message)

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def save(path, value):
    path=Path(path); temporary=path.with_name(path.name+'.tmp')
    with temporary.open('w',encoding='utf-8') as stream:
        json.dump(value,stream,indent=2);stream.flush();os.fsync(stream.fileno())
    # MOVEFILE_WRITE_THROUGH waits for the NTFS rename to complete.
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.MoveFileExW.argtypes=[w.LPCWSTR,w.LPCWSTR,w.DWORD]
    kernel.MoveFileExW.restype=w.BOOL
    if not kernel.MoveFileExW(str(temporary),str(path),0x1|0x8):
        raise ctypes.WinError(ctypes.get_last_error())

def process(pid):
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.argtypes=[w.DWORD,w.BOOL,w.DWORD];kernel.OpenProcess.restype=w.HANDLE
    kernel.CloseHandle.argtypes=[w.HANDLE]
    kernel.WaitForSingleObject.argtypes=[w.HANDLE,w.DWORD];kernel.WaitForSingleObject.restype=w.DWORD
    kernel.IsProcessInJob.argtypes=[w.HANDLE,w.HANDLE,ctypes.POINTER(w.BOOL)]
    kernel.GetProcessTimes.argtypes=[w.HANDLE]+[ctypes.POINTER(w.FILETIME)]*4
    kernel.QueryFullProcessImageNameW.argtypes=[w.HANDLE,w.DWORD,w.LPWSTR,ctypes.POINTER(w.DWORD)]
    handle=kernel.OpenProcess(0x1000|0x100000,False,pid)
    require(handle,'cannot pin native process')
    try:
        member=w.BOOL();times=[w.FILETIME() for _ in range(4)]
        buffer=ctypes.create_unicode_buffer(32768);length=w.DWORD(len(buffer))
        for ok in [kernel.IsProcessInJob(handle,None,ctypes.byref(member)),
                   kernel.GetProcessTimes(handle,*[ctypes.byref(t) for t in times]),
                   kernel.QueryFullProcessImageNameW(handle,0,buffer,ctypes.byref(length))]:
            require(ok,'cannot inspect native process identity')
        require(kernel.WaitForSingleObject(handle,0)==0x102,'native process is no longer live or cannot be waited')
        return {'pid':pid,'creation_time':(times[0].dwHighDateTime<<32)|times[0].dwLowDateTime,
                'image':buffer.value,'in_job':bool(member.value)}
    finally:
        kernel.CloseHandle(handle)

def wait_json(path, deadline):
    while True:
        failure=Path(path).with_name('failure.json')
        require(not failure.exists(),'external exchange failed: '+str(failure))
        if Path(path).exists():
            return json.loads(Path(path).read_text())
        require(time.monotonic()<deadline,'timed out waiting for '+str(path))
        time.sleep(.5)

def fixed_path(path):
    path=Path(path)
    require(len(path.drive)==2 and path.drive[1]==':','requires a local drive, not UNC/WSL/network paths')
    path=path.resolve()
    require(len(path.drive)==2 and path.drive[1]==':','resolved path is not a local drive')
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.GetDriveTypeW.argtypes=[w.LPCWSTR];kernel.GetDriveTypeW.restype=w.UINT
    require(kernel.GetDriveTypeW(path.anchor)==3,'requires a fixed local drive')
    return path

def running_names(data):
    decoded=data.decode('utf-16le').replace('\x00','').replace('\ufeff','')
    return sorted(line.strip() for line in decoded.splitlines() if line.strip())
