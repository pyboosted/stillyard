#!/usr/bin/env python3
"""Identify the actual bytes in a build snapshot before submitting a system Job.

Unlike prepare-source-snapshot.py this also supports an existing development
snapshot after scheduled formatting. Output must be outside that snapshot.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    root, output = args.root.resolve(), args.output.resolve()
    if output == root or root in output.parents:
        parser.error("manifest output must be outside the snapshot")
    hashes = {}
    for directory, children, files in os.walk(root):
        relative = Path(directory).relative_to(root)
        children[:] = sorted(name for name in children
                             if name not in {"target", ".git", "__pycache__"}
                             and (relative / name).as_posix() != "docs/evidence")
        for name in sorted(files):
            path = Path(directory) / name
            if path.is_symlink():
                parser.error("snapshot contains a symlink: " + str(path))
            hashes[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    origin = json.loads((root / "source-manifest.json").read_text())
    for name, digest in hashes.items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError("snapshot changed during identity capture: " + name)
    manifest = {"format": 1, "snapshot": str(root), "commit": origin["commit"],
                "files": hashes, "files_sha256": hashlib.sha256(
                    json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        stream.write(json.dumps(manifest, indent=2) + "\n")
    print(manifest["files_sha256"])


if __name__ == "__main__":
    main()
