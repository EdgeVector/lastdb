#!/usr/bin/env python3
"""Capture exact reused ordinary API inputs before the native cutover run."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    paths = [HERE / name for name in ('default_node.py', 'supplement.py',
                                     'run_clarity.py', 'freeze_driver.py')]
    previous = json.loads((HERE.parent / 'core-owned-writes-20261018' /
                           'driver-identity-v5.json').read_text())
    paths += [ROOT / name for name in previous['files']]
    paths.append(HERE.parent / 'core-owned-writes-20261018' / 'driver-identity-v5.json')
    return {str(path.relative_to(ROOT)): {'bytes': path.stat().st_size,
            'sha256': file_hash(path)} for path in paths}


def verify(path):
    saved = json.loads(path.read_text())
    if saved['files'] != inputs():
        raise RuntimeError('An ordinary API driver input differs from the frozen identity')
    return saved


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    saved = inputs()
    if saved != inputs():
        raise RuntimeError('The driver changes during capture')
    value = {'version': 1, 'captured_utc': datetime.now(timezone.utc).isoformat(),
        'files': saved, 'files_count': len(saved),
        'scope': 'Unchanged ordinary API cohorts plus native-default boot, CAS, repeated public writes, and unsupported-operation refusals. No benchmark.'}
    with args.output.open('x') as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write('\n')
    print(json.dumps({'identity': str(args.output), 'sha256': file_hash(args.output),
                      'files_count': len(saved)}))


if __name__ == '__main__':
    main()
