#!/usr/bin/env python3
"""Use the reviewed program-only reclaim for one explicitly named candidate container."""
import argparse
import importlib.util
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / 'core-memory-state-20261018/cleanup_baseline.py'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--record', type=Path, required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--execute-reviewed-preview', action='store_true')
    args = parser.parse_args()
    record = args.record.resolve(strict=True)
    target = args.receipt_dir.resolve()
    if (record.parent != HERE or not record.name.startswith('candidate-discovery-')
            or not record.name.endswith('-root.json') or target.parent != HERE):
        raise RuntimeError('Use an exact candidate program record and receipt path in this proof')
    if args.execute_reviewed_preview:
        if (target / 'baseline-root.json').read_bytes() != record.read_bytes():
            raise RuntimeError('The reviewed program record differs')
    else:
        target.mkdir(exist_ok=False)
        shutil.copy2(record, target / 'baseline-root.json')
    spec = importlib.util.spec_from_file_location('core_owned_program_cleanup', PRIOR)
    previous = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(previous)
    previous.HERE = target
    sys.argv = [sys.argv[0]] + (['--execute-reviewed-preview'] if args.execute_reviewed_preview else [])
    return previous.main()


if __name__ == '__main__':
    raise SystemExit(main())
