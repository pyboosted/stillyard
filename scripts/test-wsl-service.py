#!/usr/bin/env python3
"""Supervisor signal/restart controls inside a system Job.

Regular-file cgroup doubles isolate signal/fd behavior; actual cgroup retention
is a separate installed daemon-crash acceptance, never inferred from this test.
"""
import json
import os
from pathlib import Path
import select
import subprocess
import tempfile
import time
import unittest


class Supervisor(unittest.TestCase):
    def test_restarts_child_and_stops_without_leaving_a_process(self):
        self.run_case(False)

    def test_recovers_inspection_failure_without_losing_supervisor(self):
        self.run_case(True)

    def run_case(self, initial_failure):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            for name in ["manager", "executors"]:
                (root / name).mkdir()
            if not initial_failure:
                (root / "manager/cgroup.events").write_text("populated 0\nfrozen 0\n")
            marker = root / "births.json"
            child = root / "fake-daemon"
            child.write_text("#!/usr/bin/python3\nimport json,os,pathlib,time\n"
                             f"p=pathlib.Path({str(marker)!r})\n"
                             "births=json.loads(p.read_text()) if p.exists() else []\n"
                             "births.append(os.getpid());p.write_text(json.dumps(births))\n"
                             "time.sleep(.05 if len(births)==1 else 30)\n")
            child.chmod(0o700)
            helper = Path(__file__).with_name("wsl-service.py").resolve()
            code = ("import importlib.util,pathlib; "
                    f"s=importlib.util.spec_from_file_location('service',{str(helper)!r}); "
                    "m=importlib.util.module_from_spec(s);s.loader.exec_module(m); "
                    f"m.supervise(pathlib.Path({str(root / 'executors')!r}),pathlib.Path({str(child)!r}))")
            process = subprocess.Popen(["/usr/bin/python3", "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                if initial_failure:
                    time.sleep(.2)
                    self.assertIsNone(process.poll(), "inspection error terminated lifetime owner")
                    self.assertFalse(marker.exists())
                    (root / "manager/cgroup.events").write_text("populated 0\nfrozen 0\n")
                end = time.monotonic() + 10
                while True:
                    try:
                        births = json.loads(marker.read_text())
                    except (FileNotFoundError, json.JSONDecodeError):
                        births = []
                    if len(births) == 2:
                        break
                    if process.poll() is not None or time.monotonic() >= end:
                        self.fail("supervisor did not restart its first child")
                    time.sleep(.01)
                self.assertNotEqual(births[0], births[1])
                fd = os.pidfd_open(births[1])
                try:
                    process.terminate()
                    out, err = process.communicate(timeout=10)
                    self.assertEqual(process.returncode, 0, err.decode())
                    poll = select.poll(); poll.register(fd, select.POLLIN)
                    self.assertTrue(poll.poll(1000), "child survived service stop")
                finally:
                    os.close(fd)
                events = [json.loads(line) for line in out.decode().splitlines()]
                self.assertEqual(sum(e["kind"] == "wsl_daemon_started" for e in events), 2)
                self.assertTrue(events[-1]["service_stopping"])
                if initial_failure:
                    self.assertTrue(any(e["kind"] == "wsl_supervisor_retry" for e in events))
                self.assertEqual(list((root / "executors").iterdir()), [], "supervisor touched executor state")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
