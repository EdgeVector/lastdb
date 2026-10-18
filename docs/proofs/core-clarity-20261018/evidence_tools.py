#!/usr/bin/env python3
"""Reuse reviewed receipt seals, bounded evidence index, and program cleanup."""
import argparse
import importlib.util
from pathlib import Path
import shutil
import subprocess
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
    parser.add_argument('action', choices=('seal', 'index', 'program'))
    args, remaining = parser.parse_known_args()
    if args.action == 'seal':
        module = load('clarity_seal', 'core-owned-writes-20261018/seal_runs.py')
        module.HERE, module.ROOT = HERE, ROOT
    elif args.action == 'index':
        module = load('clarity_index', 'core-memory-completion-20261018/evidence_index.py')
        module.HERE, module.ROOT = HERE, ROOT

        def listed(*flags):
            return subprocess.check_output(['git', 'ls-files', *flags, '--',
                str(HERE.relative_to(ROOT))], cwd=ROOT, text=True).splitlines()

        module.listed = listed
    else:
        own = argparse.ArgumentParser()
        own.add_argument('--record', type=Path, required=True)
        own.add_argument('--receipt-dir', type=Path, required=True)
        own.add_argument('--execute-reviewed-preview', action='store_true')
        options = own.parse_args(remaining)
        record, target = options.record.resolve(strict=True), options.receipt_dir.resolve()
        containers = {'program-root.json': 'program-cleanup',
                      'response-program-root.json': 'response-program-cleanup'}
        if record.parent != HERE or record.name not in containers or target != HERE / containers[record.name]:
            raise RuntimeError('Use this proof\'s exact program record and cleanup directory')
        if options.execute_reviewed_preview:
            if (target / 'baseline-root.json').read_bytes() != record.read_bytes():
                raise RuntimeError('The reviewed program record differs')
        else:
            target.mkdir(exist_ok=False)
            shutil.copy2(record, target / 'baseline-root.json')
        module = load('clarity_program_cleanup', 'core-memory-state-20261018/cleanup_baseline.py')
        module.HERE = target
        remaining = ['--execute-reviewed-preview'] if options.execute_reviewed_preview else []
    sys.argv = [sys.argv[0], *remaining]
    return module.main()


if __name__ == '__main__':
    raise SystemExit(main())
