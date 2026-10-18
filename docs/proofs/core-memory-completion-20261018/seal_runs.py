#!/usr/bin/env python3
"""Preserve exact finished-run receipts before supported scratch cleanup."""
import argparse
import collections
import hashlib
import json
from pathlib import Path


def file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def seal_run(directory):
    result = json.loads((directory / 'result.json').read_text())
    if result.get('complete') is not True:
        raise RuntimeError('A run does not complete its declared value checks: ' + str(directory))
    counts, starts, stops, gates, churn = collections.Counter(), [], [], [], []
    with (directory / 'receipts.jsonl').open() as stream:
        for line in stream:
            row = json.loads(line)
            operation = row.get('operation')
            counts[operation] += 1
            if operation == 'owned-node-start':
                starts.append(row['pid'])
            elif operation == 'owned-node-stop':
                stops.append(row)
            elif operation == 'memory-state-warm-storage-gate':
                gates.append(row['read_operations'])
            elif operation == 'completion-constant-membership':
                churn.append(row)
    normal = [row['pid'] for row in stops if row.get('forced_kill') is False
              and row.get('exit_code') in (0, -15)]
    if collections.Counter(normal) != collections.Counter(starts) or len(normal) != len(stops):
        raise RuntimeError('The run does not establish a normal stop for every owned node')
    if any(value != 0 for value in gates):
        raise RuntimeError('A retained zero-file-read gate fails')
    files = {path.name: {'bytes': path.stat().st_size, 'sha256': file_hash(path)}
             for path in sorted(directory.iterdir()) if path.is_file() and not path.is_symlink()}
    return {'receipt_directory': str(directory), 'result': result,
        'retained_root': json.loads((directory / 'retained-root.json').read_text()),
        'operation_counts': dict(counts), 'node_sessions': len(starts),
        'all_node_sessions_stop_normally': True, 'strict_file_read_gates': len(gates),
        'constant_membership': churn, 'files': files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-root', type=Path, required=True)
    parser.add_argument('--operational', type=Path, required=True)
    parser.add_argument('--label', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    paths = [args.operational.resolve(strict=True)]
    paths += [(args.receipt_root / f'{label}-r{number}').resolve(strict=True)
              for label in ('before-off', 'before-on', 'current-off', 'current-on')
              for number in (1, 2)]
    runs = [seal_run(path) for path in paths]
    result = {'label': args.label, 'scope': 'Exact run receipt bytes before cleanup; ordinary generated-data product checks, not a test suite.',
        'all_runs_complete': True, 'all_nodes_stop_normally': True,
        'run_count': len(runs), 'node_sessions': sum(row['node_sessions'] for row in runs),
        'runs': runs}
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'runs'}))


if __name__ == '__main__':
    main()
