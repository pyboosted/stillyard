import json, subprocess
from pathlib import Path
cli = Path(r"C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe")
version = subprocess.check_output([str(cli), "--version"], text=True).strip()
assert version == "stillyard 0.1.0-alpha.17", version
context = json.loads(subprocess.check_output([str(cli), "context", "--json", "--deadline-seconds", "5"]))
assert context["parent"] is not None, context
assert context["parent"]["job_id"].startswith("01a05f1f-858c-7880-8c15-d55875da9e6b~"), context
print(json.dumps({"version": version, "parent": context["parent"], "result": "installed_native_canary_pass"}))
