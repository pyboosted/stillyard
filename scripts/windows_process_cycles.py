"""Read native process lifetime cycle totals without sampling only live threads."""
import ctypes
from ctypes import wintypes as w
import json
import sys


def process_cycles(pid):
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    kernel.QueryProcessCycleTime.argtypes = [w.HANDLE, ctypes.POINTER(ctypes.c_ulonglong)]
    kernel.QueryProcessCycleTime.restype = w.BOOL
    kernel.GetProcessTimes.argtypes = [w.HANDLE] + [ctypes.POINTER(w.FILETIME)] * 4
    kernel.GetProcessTimes.restype = w.BOOL
    kernel.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    kernel.WaitForSingleObject.restype = w.DWORD
    # QUERY_LIMITED_INFORMATION | SYNCHRONIZE. Never request mutation rights.
    handle = kernel.OpenProcess(0x1000 | 0x100000, False, pid)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        cycles = ctypes.c_ulonglong()
        times = [w.FILETIME() for _ in range(4)]
        if not kernel.QueryProcessCycleTime(handle, ctypes.byref(cycles)):
            raise ctypes.WinError(ctypes.get_last_error())
        if not kernel.GetProcessTimes(handle, *[ctypes.byref(t) for t in times]):
            raise ctypes.WinError(ctypes.get_last_error())
        if kernel.WaitForSingleObject(handle, 0) != 0x102:
            raise RuntimeError('observed native process exited or cannot be waited')
        return {'pid': pid, 'cycles': cycles.value,
                'creation_filetime': (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime}
    finally:
        kernel.CloseHandle(handle)


if __name__ == '__main__':
    print(json.dumps([process_cycles(int(pid)) for pid in json.loads(sys.argv[1])]))
