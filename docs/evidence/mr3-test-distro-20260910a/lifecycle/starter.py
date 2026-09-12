"""Run from an ordinary native Windows session, outside every Stillyard Job.

Waits for the approved controller request, then owns the permanent WSL keepalive.
Does not stop a distro, kill a helper, or escape a Windows Job Object.
"""
import argparse, json, os, subprocess, sys, time
from pathlib import Path
from common import *

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--exchange-directory',type=Path,required=True)
    p.add_argument('--approval-file',type=Path,required=True)
    args=p.parse_args()
    require(os.name=='nt' and not os.environ.get('STILLYARD_JOB_ID'),'requires an ordinary native Windows session')
    identity=process(os.getpid());require(not identity['in_job'],'external starter must not belong to a Windows Job')
    out=fixed_path(args.exchange_directory);out.mkdir(parents=True,exist_ok=False)
    approval=digest(fixed_path(args.approval_file))
    request_deadline=time.monotonic()+1800
    save(out/'ready.json',{'identity':identity,'approval_sha256':approval,'starter_sha256':digest(__file__),'request_deadline_monotonic':request_deadline})
    try:
        request=wait_json(out/'start-request.json',request_deadline)
        require(request['approval_sha256']==approval,'approval changed')
        require(request['operation']=='terminate','unexpected operation')
        # Native controller has recorded completion/absence before this request.
        with (out/'keepalive.stdout').open('wb') as stdout,(out/'keepalive.stderr').open('wb') as stderr:
            keeper=subprocess.Popen([str(WSL),'-d','Stillyard-MR3-Test','-u','pythonic','--exec',
                '/usr/bin/python3',HELPER,'keepalive','--root',LROOT],stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr)
        native=process(keeper.pid)
        if native['in_job']:
            save(out/'rejected-child.json',native)
            keeper.terminate();keeper.wait(timeout=30)
            raise RuntimeError('new keepalive unexpectedly belongs to a Windows Job')
        save(out/'launched.json',{'request':request,'identity':native,'starter':identity})
        # Never detach or terminate this child on controller completion.
        code=keeper.wait()
        save(out/'exited.json',{'exit_code':code,'unix_ns':time.time_ns()})
        return code
    except BaseException as error:
        save(out/'failure.json',{'error':str(error),'unix_ns':time.time_ns()})
        raise

if __name__=='__main__':sys.exit(main())
