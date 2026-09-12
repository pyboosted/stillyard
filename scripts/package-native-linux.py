#!/usr/bin/env python3
"""Package an identified prebuilt native candidate; never invokes Cargo."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--candidate-sha256', required=True)
    parser.add_argument('--build-origin', required=True)
    parser.add_argument('--output-directory', type=Path, required=True)
    args = parser.parse_args()
    kernel = Path('/proc/sys/kernel/osrelease').read_text().lower()
    if 'microsoft' in kernel or 'wsl' in kernel:
        parser.error('native distribution packages require native build provenance, outside WSL')
    root = Path(__file__).resolve().parent.parent
    candidate = args.candidate.resolve(strict=True)
    with candidate.open('rb') as stream:
        image_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
    if image_hash != args.candidate_sha256:
        parser.error('candidate differs from its retained build digest')
    revision = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
    inputs = ['scripts/install-native-linux.py', 'scripts/native-linux-service.py',
              'scripts/wsl-service.py', 'scripts/probe-native-linux.py',
              'docs/native-linux-operation.md', 'LICENSE-APACHE', 'LICENSE-MIT']
    if subprocess.check_output(['git', '-C', str(root), 'status', '--porcelain', '--', *inputs]):
        parser.error('package inputs must match the recorded source revision')
    name = 'stillyard-native-linux-' + platform.machine() + '-' + revision[:12]
    output = args.output_directory.resolve()
    output.mkdir(parents=True, exist_ok=True)
    bundle = output / name
    bundle.mkdir(mode=0o700)
    (bundle / 'bin').mkdir()
    shutil.copy2(candidate, bundle / 'bin/stillyard')
    for name_in in inputs:
        source = root / name_in
        if source.is_symlink() or not source.is_file():
            raise RuntimeError('package input is not a regular tracked file: ' + name_in)
        target = bundle / name_in
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    instructions = '''# Native Linux first-install bundle

Reference profile: native Ubuntu 24.04 x86-64, systemd >=254, Python >=3.11,
bubblewrap, delegated cgroup v2 and a private local durable filesystem.
The host prerequisite probe must pass. WSL and existing installations are refused.

Run `python3 install.py --evidence-directory /absolute/path/to/evidence` to
prepare the installation plan. Add `--apply` to install. Optional budgets:
`--ram-mb 4096 --cargo-slots 2`. All packaged files are checked against the
manifest before installation. The archive checksum and build origin identify
the candidate; they are not a package signature.

The installed binary is `~/.local/share/stillyard/bin/stillyard` unless
XDG_DATA_HOME is set. See docs/native-linux-operation.md for operation and limits.
This bundle does not upgrade or replace an existing Store or service.
'''
    (bundle / 'README.md').write_text(instructions)
    wrapper = '''#!/usr/bin/env python3
import argparse,hashlib,json,os,sys
from pathlib import Path
parser=argparse.ArgumentParser(description='Install the exact native candidate recorded in this bundle')
parser.add_argument('--evidence-directory',required=True)
parser.add_argument('--ram-mb',type=int,default=4096)
parser.add_argument('--cargo-slots',type=int,default=2)
parser.add_argument('--apply',action='store_true')
args=parser.parse_args()
root=Path(__file__).resolve().parent
manifest=json.loads((root/'manifest.json').read_text())
for name,digest in manifest['files'].items():
    path=root/name
    if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
        raise SystemExit('Bundle file differs from manifest: '+name)
os.execv(sys.executable,[sys.executable,str(root/'scripts/install-native-linux.py'),
    '--candidate',str(root/'bin/stillyard'),'--candidate-sha256',manifest['candidate_sha256'],
    '--build-origin',manifest['build_origin'],'--source-root',str(root),
    '--evidence-directory',args.evidence_directory,'--ram-mb',str(args.ram_mb),
    '--cargo-slots',str(args.cargo_slots),*(['--apply'] if args.apply else [])])
'''
    (bundle / 'install.py').write_text(wrapper)
    files = {str(p.relative_to(bundle)): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(bundle.rglob('*')) if p.is_file()}
    manifest = {'format': 1, 'source_revision': revision, 'build_origin': args.build_origin,
                'candidate_sha256': image_hash, 'architecture': platform.machine(),
                'build_kernel': kernel.strip(), 'libc': platform.libc_ver(), 'files': files}
    (bundle / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    archive = output / (name + '.tar.gz')
    with tarfile.open(archive, 'x:gz') as stream:
        stream.add(bundle, arcname=name)
    with archive.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    (output / (archive.name + '.sha256')).write_text(digest + '  ' + archive.name + '\n')
    print(archive, digest, flush=True)


if __name__ == '__main__':
    main()
