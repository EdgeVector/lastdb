#!/usr/bin/env python3
"""Verify retained receipt bytes and exact authorized cleanup results."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    seals, checked, errors = [], 0, []
    for name in ('receipt-seal.json', 'response-receipt-seal.json', 'preparation-receipt-seal.json'):
        path = HERE / name
        seal = json.loads(path.read_text())
        pairs = []
        if 'runs' in seal:
            for run in seal['runs']:
                pairs += [(Path(run['receipt_directory']) / key, expected)
                          for key, expected in run['files'].items()]
        else:
            pairs = [(ROOT / key, expected) for key, expected in seal['files'].items()]
        for source, expected in pairs:
            if (not source.is_file() or source.stat().st_size != expected['bytes']
                    or digest(source) != expected['sha256']):
                errors.append(str(source))
            checked += 1
        seals.append({'path': str(path), 'sha256': digest(path), 'files_checked': len(pairs)})
    index = json.loads((HERE / 'cleanup-review-index.json').read_text())
    roots = []
    for item in index['roots']:
        directory = Path(item['preview']).parent
        outcome = directory / ('baseline-cleanup.json' if item['kind'] == 'program-only' else 'cleanup.json')
        result = json.loads(outcome.read_text())
        root = Path(item['root'])
        absent = not root.exists() and not root.is_symlink()
        if (not absent or result.get('exit_code') != 0 or result.get('root') != str(root)
                or result.get('root_exists_after') is not False):
            errors.append(str(root))
        roots.append({'label': item['label'], 'root': str(root), 'absent': absent,
                      'cleanup_exit_code': result['exit_code'], 'outcome': str(outcome)})
    value = {'passed': not errors, 'sealed_receipt_files_checked': checked,
        'all_receipt_sizes_and_hashes_match': not errors, 'seals': seals,
        'exact_roots_removed': len(roots), 'normal_node_sessions': index['node_sessions'],
        'preparation_node_sessions': 0, 'roots': roots, 'errors': errors}
    with (HERE / 'cleanup-verification.json').open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({key: value[key] for key in
        ('passed', 'sealed_receipt_files_checked', 'exact_roots_removed', 'normal_node_sessions')}))
    return 0 if not errors else 1


if __name__ == '__main__':
    raise SystemExit(main())
