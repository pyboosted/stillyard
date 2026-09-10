#!/usr/bin/env python3
"""Bounded MR-0 interop negative control; run as a Windows Stillyard Job.

No Cargo. A pass proves only the ordinary PE-execution barrier for this profile,
not durable admission, cgroup cleanup, or resistance to a malicious host owner.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def attempt(executable, marker):
    try:
        result = subprocess.run(
            [executable, "/d", "/c", "echo stillyard-interop-control>" + marker],
            stdin=subprocess.DEVNULL, capture_output=True, timeout=5,
        )
        return {"exit_code": result.returncode,
                "stdout": result.stdout.decode(errors="replace"),
                "stderr": result.stderr.decode(errors="replace")}
    except OSError as error:
        return {"exec_errno": error.errno, "error": str(error)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--inside", action="store_true")
    parser.add_argument("--windows-marker")
    args = parser.parse_args()
    directory = args.directory.resolve()
    cmd = "/mnt/c/Windows/System32/cmd.exe"
    if args.inside:
        evidence = {
            "pe": attempt(cmd, args.windows_marker),
            "init_is_regular": Path("/init").is_file(),
            "init_is_character_device": Path("/init").is_char_device(),
            "pid_namespace": os.readlink("/proc/self/ns/pid"),
            "mount_namespace": os.readlink("/proc/self/ns/mnt"),
            "interop_environment_retained": bool(os.environ.get("WSL_INTEROP")),
        }
        print(json.dumps(evidence))
        return 0 if evidence["pe"].get("exec_errno") in (13, 8) else 1
    directory.mkdir(parents=True, exist_ok=False)
    win_directory = subprocess.check_output(["wslpath", "-w", str(directory)], text=True).strip()
    control_marker = directory / "outside.txt"
    if " " in win_directory:
        raise RuntimeError("The bounded cmd control requires an evidence path without spaces")
    control = attempt(cmd, win_directory + '\\outside.txt')
    (directory / "outside-attempt.json").write_text(json.dumps(control, indent=2) + "\n")
    if control.get("exit_code") != 0 or not control_marker.exists():
        raise RuntimeError("Outside control did not exercise functioning WSL interop")
    # Keep WSL_INTEROP deliberately: the OS boundary must work independently of
    # clearing an environment variable. No global binfmt/WSL setting is changed.
    result = subprocess.run([
        "/usr/bin/bwrap", "--ro-bind", "/", "/", "--dev", "/dev",
        "--proc", "/proc", "--unshare-user", "--unshare-pid", "--unshare-uts",
        "--unshare-cgroup", "--new-session", "--die-with-parent",
        "--ro-bind", "/dev/null", "/init", "--tmpfs", "/run",
        "--", sys.executable, str(Path(__file__).resolve()), str(directory), "--inside",
        "--windows-marker", win_directory + '\\inside-must-not-exist.txt',
    ], capture_output=True, timeout=10)
    report = {"outside": control, "inside_exit": result.returncode,
              "inside": json.loads(result.stdout) if result.stdout else None,
              "inside_stderr": result.stderr.decode(errors="replace"),
              "pid_namespace": os.readlink("/proc/self/ns/pid"),
              "mount_namespace": os.readlink("/proc/self/ns/mnt"),
              "scope": "ordinary PE execution barrier only"}
    (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
