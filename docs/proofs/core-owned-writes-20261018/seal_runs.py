#!/usr/bin/env python3
"""Preserve exact completed operational receipts before supported scratch cleanup."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directories', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    runs = []
    for supplied in args.directories:
        directory = supplied.resolve(strict=True)
        if not directory.is_relative_to(HERE) or len(directory.relative_to(HERE).parts) > 2:
            raise RuntimeError('Use an exact receipt directory within two levels of this proof')
        rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
        starts = [row for row in rows if row.get('operation') == 'owned-node-start']
        stops = [row for row in rows if row.get('operation') == 'owned-node-stop']
        if (not starts or len(starts) != len(stops)
                or {row['pid'] for row in starts} != {row['pid'] for row in stops}
                or any(row.get('forced_kill') is not False for row in stops)):
            raise RuntimeError('An owned operational process lacks a recorded unforced exit')
        relative = str(directory.relative_to(ROOT))
        regular = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others',
            '--exclude-standard', '--', relative], cwd=ROOT)
        ignored = subprocess.check_output(['git', 'ls-files', '-z', '--others', '--ignored',
            '--exclude-standard', '--', relative], cwd=ROOT)
        paths = sorted(set((regular + ignored).decode().split('\0')) - {''})
        files = {}
        for name in paths:
            path = ROOT / name
            if path.is_file() and not path.is_symlink() and not path.name.startswith('cleanup'):
                files[str(path.relative_to(directory))] = {'bytes': path.stat().st_size,
                                                          'sha256': digest(path)}
        runs.append({'receipt_directory': str(directory),
            'retained_root': json.loads((directory / 'retained-root.json').read_text()),
            'result': json.loads((directory / 'result.json').read_text()),
            'node_sessions': len(starts), 'all_nodes_stop_normally': all(
                row.get('exit_code') in (0, -15) for row in stops),
            'exit_codes': [row.get('exit_code') for row in stops], 'files': files})
    value = {'runs': runs, 'node_sessions': sum(row['node_sessions'] for row in runs),
        'all_run_processes_exit_unforced': True,
        'all_nodes_stop_normally': all(row['all_nodes_stop_normally'] for row in runs),
        'scope': 'Expected failed value gates remain failed. Startup exit1 is recorded separately from normal stops. This seal does not establish cleanup safety or change acceptance.'}
    with args.output.open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({'runs': len(runs), 'node_sessions': value['node_sessions'],
                      'all_nodes_stop_normally': value['all_nodes_stop_normally']}))


if __name__ == '__main__':
    main()
