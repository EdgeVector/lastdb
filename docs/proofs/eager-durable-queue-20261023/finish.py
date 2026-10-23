#!/usr/bin/env python3
"""Build and capture the final candidate in one host compiler-exclusion window."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

PROOF = Path(__file__).resolve().parent
ROOT = PROOF.parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scratch', type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get('FOLD_HOST_CARGO_LOCK_HELD') != '1':
        raise RuntimeError('hold the host compiler exclusion for build and captures')
    scratch = args.scratch.resolve(strict=True)
    identity = json.loads((PROOF / 'identity.json').read_text())
    if str(scratch) != identity['scratch'] or scratch.stat().st_uid != os.getuid():
        raise RuntimeError('scratch ownership differs from the capture receipt')
    changed = subprocess.check_output(['git', 'diff', 'HEAD', '--', 'fold_db', 'vendor', 'lastdb_node'],
                                      cwd=ROOT, text=True)
    if changed:
        raise RuntimeError('commit product source before the final build')
    subprocess.run(['cargo', 'build', '-p', 'lastdb_node', '--bin', 'lastdbd',
                    '--profile', 'dev-release', '--locked'], cwd=ROOT, check=True)
    binary = ROOT / 'target/dev-release/lastdbd'
    latest = hashlib.sha256(binary.read_bytes()).hexdigest()
    if latest == identity['candidate_binary_sha256']:
        raise RuntimeError('final program is unchanged from the first candidate')
    shutil.copyfile(scratch / 'candidate', scratch / 'initial')
    shutil.copyfile(binary, scratch / 'candidate')
    (scratch / 'candidate').chmod(0o755)
    (PROOF / 'final-identity.json').write_text(json.dumps({
        'source': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'binary_sha256': latest, 'baseline_source': identity['baseline_source'],
        'baseline_binary_sha256': identity['baseline_binary_sha256'],
        'compiler_exclusion': True, 'other_host_work_controlled': False,
        'load_before': os.getloadavg()}, indent=2) + '\n')
    subprocess.run([sys.executable, str(PROOF / 'run.py'), '--scratch', str(scratch), '--mode', 'final'],
                   cwd=ROOT, check=True)


if __name__ == '__main__':
    main()
