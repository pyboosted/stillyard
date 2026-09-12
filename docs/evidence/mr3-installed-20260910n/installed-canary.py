import json, os, struct, subprocess, uuid
from pathlib import Path
cli = Path(r"C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe")
version = subprocess.check_output([str(cli), "--version"], text=True).strip()
assert version == "stillyard 0.1.0-alpha.19", version
context = json.loads(subprocess.check_output([str(cli), "context", "--json", "--deadline-seconds", "5"]))
assert context["parent"]["job_id"] == os.environ["STILLYARD_JOB_ID"], context
endpoint = os.environ["STILLYARD_ENDPOINT"]
commands = [{"kind":"authority_status"}, {"kind":"participant","domain":str(uuid.uuid4())}, {"kind":"authority_status"}]
frames=[]
requests=[]
for command in commands:
    request={"version":1,"protocol_version":24,"request_id":str(uuid.uuid4()),"deadline_millis":5000,"command":command}
    requests.append(request)
    raw=json.dumps(request).encode()
    frames.append(struct.pack("<I",len(raw))+raw)
bridge=subprocess.Popen([str(cli),"--endpoint",endpoint,"machine","bridge"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
out,err=bridge.communicate(b"".join(frames),timeout=20)
assert bridge.returncode == 0,err.decode(errors="replace")
replies=[]
for request in requests:
    assert len(out)>=4
    n=struct.unpack("<I",out[:4])[0]
    assert 0<n<=1048576 and len(out)>=4+n
    reply=json.loads(out[4:4+n]);out=out[4+n:]
    assert reply["request_id"]==request["request_id"] and reply["protocol_version"]==24
    replies.append(reply["outcome"])
assert not out
assert [r["kind"] for r in replies]==["authority","error","authority"],replies
assert replies[0]["authority"]["epoch"]==replies[2]["authority"]["epoch"]
print(json.dumps({"version":version,"parent":context["parent"],"bridge_requests":[r["request_id"] for r in requests],"bridge_outcomes":[r["kind"] for r in replies],"authority_epoch":replies[0]["authority"]["epoch"],"result":"installed_native_canary_and_bridge_pass"}))
