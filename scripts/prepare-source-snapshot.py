#!/usr/bin/env python3
"""Copy an identified checkout snapshot without modifying another checkout.

Includes tracked and non-ignored untracked files, except docs/evidence (outputs).
The destination must not exist and must be outside the source checkout.
No build tools are invoked. Build the snapshot through run-stillyard-job.ps1.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    destination = args.destination.resolve()
    if destination == root or root in destination.parents:
        parser.error("destination must be outside the checkout")
    commit = git(root, "rev-parse", "HEAD").decode().strip()
    diff = git(root, "diff", "HEAD", "--binary")
    paths = sorted(set(git(root, "ls-files", "-z", "--cached", "--others",
                           "--exclude-standard").decode().split("\0")) - {""})
    # Reject unsafe inputs before creating the destination. Git symlinks are not
    # flattened: that could silently copy files outside the source snapshot.
    selected = []
    for name in paths:
        if name.startswith("docs/evidence/"):
            continue
        path = root / name
        if path.is_symlink():
            parser.error(f"symlink needs explicit snapshot policy: {name}")
        if path.exists():
            if not path.is_file():
                parser.error(f"non-file needs explicit snapshot policy: {name}")
            selected.append((name, path))
    destination.mkdir(parents=True, exist_ok=False)
    hashes = {}
    for name, path in selected:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        hashes[name] = hashlib.sha256(target.read_bytes()).hexdigest()
    if (git(root, "rev-parse", "HEAD").decode().strip() != commit
            or git(root, "diff", "HEAD", "--binary") != diff
            or any(hashlib.sha256(path.read_bytes()).hexdigest() != hashes[name]
                   for name, path in selected)):
        raise RuntimeError("Source changed while copying; discard this snapshot and retry")
    manifest = {
        "format": 1,
        "source": str(root),
        "commit": commit,
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
        "files": hashes,
        "files_sha256": hashlib.sha256(json.dumps(hashes, sort_keys=True,
                                                 separators=(",", ":")).encode()).hexdigest(),
    }
    output = destination / "source-manifest.json"
    output.write_text(json.dumps(manifest, indent=2) + "\n")
    print(output)
    print(manifest["files_sha256"])


if __name__ == "__main__":
    main()
