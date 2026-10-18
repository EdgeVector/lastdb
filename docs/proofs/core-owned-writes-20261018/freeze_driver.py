#!/usr/bin/env python3
"""Freeze exact ordinary API driver inputs before a product run."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
OWN = ('run_operations.py', 'logical.py', 'cohorts.py', 'protein.py', 'scoped.py',
       'replay.py', 'memory.py', 'freeze_driver.py')
SHARED = ('core-memory-completion-20261018/operations.py',
    'core-memory-completion-20261018/hash_all.py',
    'core-memory-state-20261018/operational.py', 'core-memory-state-20261018/api.py',
    'core-memory-state-20261018/driver.py', 'core-memory-state-20261018/workload.py',
    'schema-preload-product-20261017/measure.py',
    'graph-first-20261017/drivers/range_profile.py', 'graph-first-20261017/drivers/ownership.py')


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs():
    paths = [HERE / name for name in OWN] + [HERE.parent / name for name in SHARED]
    return {str(path.relative_to(ROOT)): {'bytes': path.stat().st_size,
            'sha256': file_hash(path)} for path in paths}


def verify(path):
    saved = json.loads(path.read_text())
    if saved['files'] != inputs():
        raise RuntimeError('An ordinary API driver input differs from its frozen identity')
    return saved


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    saved = inputs()
    if saved != inputs():
        raise RuntimeError('An ordinary API driver input changes during capture')
    value = {'version': 1, 'captured_utc': datetime.now(timezone.utc).isoformat(),
        'files': saved, 'files_count': len(saved),
        'scope': 'Normal generated-data API operations. The prior strict gates remain unchanged. New first-value zero-file rules are candidate requirements and reference observations. No benchmark operations change.'}
    with args.output.open('x') as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write('\n')
    print(json.dumps({'identity': str(args.output), 'files_count': len(saved),
                      'sha256': file_hash(args.output)}))


if __name__ == '__main__':
    main()
