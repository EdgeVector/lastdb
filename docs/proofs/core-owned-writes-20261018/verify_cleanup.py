#!/usr/bin/env python3
"""Verify original receipt hashes and exact supported cleanup without a product call."""
import argparse
import json
from pathlib import Path

from seal_runs import digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seal', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--retained-program-records', type=Path, nargs='*', default=[])
    args = parser.parse_args()
    seal, roots, checked = json.loads(args.seal.read_text()), [], 0
    for run in seal['runs']:
        directory = Path(run['receipt_directory'])
        for name, expected in run['files'].items():
            path = directory / name
            if path.stat().st_size != expected['bytes'] or digest(path) != expected['sha256']:
                raise RuntimeError('An original receipt differs after cleanup: ' + str(path))
            checked += 1
        root = Path(run['retained_root']['root'])
        result = json.loads((directory / 'cleanup.json').read_text())
        if (root.exists() or root.is_symlink() or result.get('root') != str(root)
                or result.get('exit_code') != 0 or result.get('root_exists_after') is not False):
            raise RuntimeError('The supported exact cleanup does not remove the released root')
        roots.append(str(root))
    retained = []
    for path in args.retained_program_records:
        record = json.loads(path.read_text())
        if digest(Path(record['binary'])) != record['binary_sha256']:
            raise RuntimeError('A required retained program differs')
        retained.append({'root': record['root'], 'binary_sha256': record['binary_sha256'], 'retained': True})
    result = {'seal': str(args.seal.resolve()), 'seal_sha256': digest(args.seal),
        'all_original_receipt_bytes_match': True, 'checked_receipt_files': checked,
        'all_exact_run_roots_removed': True, 'removed_roots': roots,
        'normal_node_sessions': sum(code in (0, -15) for run in seal['runs'] for code in run['exit_codes']),
        'other_unforced_exit_codes': [code for run in seal['runs'] for code in run['exit_codes'] if code not in (0, -15)],
        'retained_programs': retained}
    with args.output.open('x') as stream:
        json.dump(result, stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({'roots_removed': len(roots), 'original_receipt_files_verified': checked}))


if __name__ == '__main__':
    main()
