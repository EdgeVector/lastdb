#!/usr/bin/env python3
"""Reuse receipt seals and exact program cleanup for the final merge proof."""
import argparse
import importlib.util
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, HERE.parent / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('seal', 'program'))
    args, remaining = parser.parse_known_args()
    if args.action == 'seal':
        module = load('merge_receipt_seal', 'core-owned-writes-20261018/seal_runs.py')
        module.HERE, module.ROOT = HERE, ROOT
    else:
        own = argparse.ArgumentParser()
        own.add_argument('--execute-reviewed-preview', action='store_true')
        options = own.parse_args(remaining)
        record, target = HERE / 'merge-program-root.json', HERE / 'merge-program-cleanup'
        if options.execute_reviewed_preview:
            if (target / 'baseline-root.json').read_bytes() != record.read_bytes():
                raise RuntimeError('The reviewed merge program record differs')
        else:
            target.mkdir(exist_ok=False)
            shutil.copy2(record, target / 'baseline-root.json')
        module = load('merge_program_cleanup', 'core-memory-state-20261018/cleanup_baseline.py')
        module.HERE = target
        remaining = ['--execute-reviewed-preview'] if options.execute_reviewed_preview else []
    sys.argv = [sys.argv[0], *remaining]
    return module.main()


if __name__ == '__main__':
    raise SystemExit(main())
