import ctypes,os,json,threading,time,ast
from ctypes import wintypes as w
from pathlib import Path
from windows_process_cycles import process_cycles
if not os.environ.get('STILLYARD_JOB_ID'):raise RuntimeError('requires scheduled native Job')
ast.parse(Path('observe-installed-idle.py').read_text())
k=ctypes.WinDLL('kernel32',use_last_error=True)
k.GetCurrentThread.argtypes=[];k.GetCurrentThread.restype=w.HANDLE
k.QueryThreadCycleTime.argtypes=[w.HANDLE,ctypes.POINTER(ctypes.c_ulonglong)];k.QueryThreadCycleTime.restype=w.BOOL
def thread_cycles():
    value=ctypes.c_ulonglong()
    if not k.QueryThreadCycleTime(k.GetCurrentThread(),ctypes.byref(value)):raise ctypes.WinError(ctypes.get_last_error())
    return value.value
result={}
def work():
    before=thread_cycles();end=time.thread_time()+.3;value=0
    while time.thread_time()<end:
        for i in range(10000):value+=i
    result['worker_cycles']=thread_cycles()-before
before=process_cycles(os.getpid());worker=threading.Thread(target=work);worker.start();worker.join(timeout=10)
if worker.is_alive():raise RuntimeError('bounded worker failed to finish')
after=process_cycles(os.getpid());delta=after['cycles']-before['cycles']
if not result.get('worker_cycles') or delta<result['worker_cycles']:raise RuntimeError('exited worker cycles missing from process lifetime total')
print(json.dumps({'job_id':os.environ['STILLYARD_JOB_ID'],'before':before,'after':after,'exited_worker_cycles':result['worker_cycles'],'process_delta_cycles':delta,'exited_thread_included':True,'observer_native_parse_passed':True}))
