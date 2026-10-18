#!/usr/bin/env python3
"""Verify retained receipt bytes and the exact cleanup outcomes."""
import argparse
import hashlib
import json
from pathlib import Path


def file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seal', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--retained-baseline', type=Path)
    args = parser.parse_args()
    seal = json.loads(args.seal.read_text())
    roots, checked = [], 0
    for run in seal['runs']:
        directory = Path(run['receipt_directory']).resolve(strict=True)
        for name, expected in run['files'].items():
            path = directory / name
            if path.stat().st_size != expected['bytes'] or file_hash(path) != expected['sha256']:
                raise RuntimeError('The preserved receipt differs after cleanup: ' + str(path))
            checked += 1
        root = Path(run['retained_root']['root'])
        outcome = json.loads((directory / 'cleanup.json').read_text())
        if root.exists() or root.is_symlink() or outcome.get('exit_code') != 0 or outcome.get('root_exists_after') is not False:
            raise RuntimeError('An exact supported reclaim does not remove its released root')
        if outcome.get('root') != str(root):
            raise RuntimeError('The exact cleanup root differs from the receipt seal')
        roots.append(str(root))
    baseline = None
    if args.retained_baseline is not None:
        baseline = json.loads(args.retained_baseline.read_text())
        if file_hash(Path(baseline['binary'])) != baseline['binary_sha256']:
            raise RuntimeError('The retained baseline executable identity differs')
        baseline = {'root': baseline['root'], 'binary_sha256': baseline['binary_sha256'],
                    'retained': True}
    result = {'seal': str(args.seal.resolve()), 'seal_sha256': file_hash(args.seal),
        'all_original_receipt_bytes_match': True, 'checked_receipt_files': checked,
        'all_exact_run_roots_removed': True, 'removed_roots': roots,
        'normal_node_sessions': seal['node_sessions'], 'retained_baseline': baseline}
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
