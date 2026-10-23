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


def build():
    subprocess.run(['cargo', 'build', '-p', 'lastdb_node', '--bin', 'lastdbd',
                    '--profile', 'dev-release', '--locked'], cwd=ROOT, check=True)
    return ROOT / 'target/dev-release/lastdbd'


def baseline(source, scratch):
    branch = subprocess.check_output(['git', 'branch', '--show-current'], cwd=ROOT, text=True).strip()
    if not branch or subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True):
        raise RuntimeError('baseline build requires a clean owned branch')
    pinned = subprocess.check_output(['git', 'rev-parse', source], cwd=ROOT, text=True).strip()
    try:
        subprocess.run(['git', 'switch', '--detach', pinned], cwd=ROOT, check=True)
        binary = build()
        shutil.copyfile(scratch / 'before', scratch / 'before-bcd')
        shutil.copyfile(binary, scratch / 'before')
        (scratch / 'before').chmod(0o755)
        digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    finally:
        subprocess.run(['git', 'switch', branch], cwd=ROOT, check=True)
    return pinned, digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--mode', choices=('final', 'metadata', 'groups', 'counts', 'barrier'), default='final')
    parser.add_argument('--baseline-source')
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
    baseline_source = identity['baseline_source']
    baseline_digest = identity['baseline_binary_sha256']
    if args.mode in ('counts', 'barrier'):
        previous = json.loads((PROOF / 'groups-identity.json').read_text())
        baseline_source = previous['baseline_source']
        baseline_digest = previous['baseline_binary_sha256']
        if baseline_digest != hashlib.sha256((scratch / 'before').read_bytes()).hexdigest():
            raise RuntimeError('pinned current-main baseline changed')
    if args.baseline_source:
        baseline_source, baseline_digest = baseline(args.baseline_source, scratch)
    binary = build()
    raw = binary.read_bytes()
    latest = hashlib.sha256(raw).hexdigest()
    markers = {label: label.encode() in raw for label in (
        'failed compaction must be discarded',
        'immutable dependency facts changed during compaction')}
    if args.mode in ('groups', 'counts', 'barrier'):
        markers['compaction_unchanged_groups'] = b'compaction_unchanged_groups' in raw
    if args.mode in ('counts', 'barrier'):
        markers['compaction_reference_deltas'] = b'compaction_reference_deltas' in raw
    if args.mode == 'barrier':
        markers['private_file_preparations'] = b'private_file_preparations' in raw
    del raw
    if not all(markers.values()):
        raise RuntimeError('compiled storage implementation markers differ')
    if latest == hashlib.sha256((scratch / 'candidate').read_bytes()).hexdigest():
        raise RuntimeError('final program is unchanged from the first candidate')
    shutil.copyfile(scratch / 'candidate', scratch / ('initial' if args.mode == 'final' else ('frontier' if args.mode == 'metadata' else ('groups' if args.mode == 'counts' else ('counts' if args.mode == 'barrier' else 'metadata')))))
    shutil.copyfile(binary, scratch / 'candidate')
    (scratch / 'candidate').chmod(0o755)
    (PROOF / (args.mode + '-identity.json')).write_text(json.dumps({
        'source': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'binary_sha256': latest, 'implementation_markers': markers,
        'baseline_source': baseline_source,
        'baseline_binary_sha256': baseline_digest,
        'compiler_exclusion': True, 'other_host_work_controlled': False,
        'load_before': os.getloadavg()}, indent=2) + '\n')
    subprocess.run([sys.executable, str(PROOF / 'run.py'), '--scratch', str(scratch), '--mode', args.mode],
                   cwd=ROOT, check=True)
    (PROOF / (args.mode + '-completed.json')).write_text(
        json.dumps({'ok': True, 'binary_sha256': latest}) + '\n')


if __name__ == '__main__':
    main()
