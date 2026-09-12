import json,os,socket,struct,subprocess,uuid
from pathlib import Path
out=Path(__file__).parent
cli='/home/pythonic/.local/share/stillyard/bin/stillyard'
endpoint=os.environ['STILLYARD_ENDPOINT'];job=os.environ['STILLYARD_JOB_ID']
role=os.environ['STILLYARD_ROLE'];assert role in ('postcondition','probe')
status=json.loads(subprocess.check_output([cli,'--endpoint',endpoint,'status',job,'--deadline-seconds','10']))
assert status['job_id']==job and status['state'] in ('active','pending')
# Raw wire request deliberately bypasses client/environment hints. Server must
# authenticate the peer using kernel containment and refuse unmanaged authority.
def rpc(value):
 payload=json.dumps(value).encode()
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(10);s.connect(endpoint);s.sendall(struct.pack('<I',len(payload))+payload)
  def read(n):
   data=b''
   while len(data)<n:
    part=s.recv(n-len(data));assert part;data+=part
   return data
  size=struct.unpack('<I',read(4))[0];assert size<16777216
  return json.loads(read(size))
for k in list(os.environ):
 if k.startswith('STILLYARD_'):del os.environ[k]
responses=[]
for request in [{'operation':'submission_context','claimed_parent':None}, {'operation':'submit','idempotency_key':str(uuid.uuid4()),'payload_hash':'unused-before-authority','spec':{'spec_version':4,'executable':'/usr/bin/false','args':[],'working_directory':str(out)},'stdin':None,'expected_store_uuid':job.split('~')[0],'expected_parent':None,'wait_for_completion':False}]:
 response=rpc(request);responses.append(response)
 assert response['result']=='error' and response['code']=='rejected' and 'containing primary' in response['message'],response
(out/(role+'-result.json')).write_text(json.dumps({'authenticated_status_job':job,'non_primary_requests':responses,'environment_hints_removed':True},indent=2))
