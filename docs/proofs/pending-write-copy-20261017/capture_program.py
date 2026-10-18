#!/usr/bin/env python3
"""Capture an exact built Mini in a fresh owned synthetic root without a node."""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
BASE = Path('/Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016')
sys.path.insert(0, str(BASE / 'docs/proofs/schema-preload-product-20261017'))
import measure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--source-identity', type=Path, required=True)
    parser.add_argument('--label', choices=['before', 'after'], required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    args = parser.parse_args()
    manifest_path = args.source_identity.resolve(strict=True)
    manifest_hash = measure.profile.file_hash(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    checked = 0
    for name, expected in manifest['source_files'].items():
        source = (BASE / name).resolve(strict=True)
        if BASE not in source.parents:
            raise RuntimeError('A source identity escapes this worktree')
        if measure.profile.file_hash(source) != expected['sha256']:
            raise RuntimeError('The source identity differs before capture: ' + name)
        checked += 1
    if measure.profile.file_hash(args.binary) != args.expected_sha256:
        raise RuntimeError('The built executable differs before capture')
    run = measure.Run(SimpleNamespace(binary=args.binary, root=None,
        receipt_dir=args.receipt_dir, variant=args.label + '-receipt-fixed-capture',
        source_commit=str(manifest_path), records_per_hash=256, hashes=2,
        body_bytes=1024, seed_batch=64, read_rounds=2, cap=10000,
        counts=[16, 64, 256], concurrent_width=16, extra_env={}, reads_only=True))
    if measure.profile.file_hash(run.binary) != args.expected_sha256:
        raise RuntimeError('The private executable differs after capture')
    if measure.profile.file_hash(manifest_path) != manifest_hash:
        raise RuntimeError('The source manifest changed during capture')
    measure.profile.save(run.receipt_dir / 'private-binary.json', {
        'root': str(run.root), 'private_binary': str(run.binary),
        'sha256': args.expected_sha256, 'owner_uid': os.getuid(),
        'node_started': False, 'source_identity': str(manifest_path),
        'source_identity_sha256': manifest_hash, 'source_files_verified': checked,
        'retained_until': 'Both benchmark programs complete and exact cleanup receives review.'})
    for name, source in {'capture_program.py': Path(__file__),
            'measure.py': Path(measure.__file__),
            'range_profile.py': measure.SHARED / 'range_profile.py',
            'ownership.py': measure.SHARED / 'ownership.py'}.items():
        shutil.copy2(source, run.receipt_dir / name)
    run.finish(True, None)
    print(json.dumps({'private_binary': str(run.binary), 'sha256': args.expected_sha256,
                      'source_files_verified': checked, 'node_started': False}), flush=True)


if __name__ == '__main__':
    main()
