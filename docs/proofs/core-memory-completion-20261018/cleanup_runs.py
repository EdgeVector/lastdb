#!/usr/bin/env python3
"""Preview or execute reviewed exact cleanup for the explicitly sealed runs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
HELPER = HERE.parent / 'core-memory-state-20261018/cleanup_owned.py'


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
    parser.add_argument('--execute-reviewed-previews', action='store_true')
    args = parser.parse_args()
    seal = json.loads(args.seal.read_text())
    if seal.get('all_runs_complete') is not True or seal.get('all_nodes_stop_normally') is not True:
        raise RuntimeError('The receipt seal does not establish completed released runs')
    outcomes = []
    for run in seal['runs']:
        directory = Path(run['receipt_directory']).resolve(strict=True)
        for name, expected in run['files'].items():
            path = directory / name
            if path.stat().st_size != expected['bytes'] or file_hash(path) != expected['sha256']:
                raise RuntimeError('A preserved run receipt differs from its seal: ' + str(path))
        command = [sys.executable, str(HELPER), '--receipt-dir', str(directory)]
        if args.execute_reviewed_previews:
            command.append('--execute-reviewed-preview')
        outcome = subprocess.run(command, capture_output=True, text=True, check=False)
        saved = {'receipt_directory': str(directory), 'command': command,
            'exit_code': outcome.returncode, 'stdout': outcome.stdout, 'stderr': outcome.stderr}
        outcomes.append(saved)
        args.output.write_text(json.dumps(outcomes, sort_keys=True, indent=2) + '\n')
        print(json.dumps(saved), flush=True)
        if outcome.returncode:
            return outcome.returncode
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
