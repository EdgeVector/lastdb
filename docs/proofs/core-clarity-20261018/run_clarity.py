#!/usr/bin/env python3
"""Reuse the normal generated API workflows with native default selection."""
import argparse
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'core-owned-writes-20261018'))
import run_operations as previous
sys.path.insert(0, str(HERE))
import default_node
import supplement
import importlib.util

spec = importlib.util.spec_from_file_location('clarity_freeze', HERE / 'freeze_driver.py')
freeze = importlib.util.module_from_spec(spec)
spec.loader.exec_module(freeze)


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--clarity-identity', type=Path, required=True)
    own, remaining = parser.parse_known_args()
    identity = freeze.verify(own.clarity_identity)
    previous.Node.start = default_node.start
    original_cohorts = previous.cohorts.run
    original_preserve = previous.preserve

    def cohorts(node):
        original_cohorts(node)
        supplement.run(node)

    def preserve(run, options, old_identity):
        original_preserve(run, options, old_identity)
        freeze.verify(own.clarity_identity)
        shutil.copy2(own.clarity_identity, run.receipt_dir / 'clarity-driver-identity.json')
        for name in identity['files']:
            target = run.receipt_dir / 'clarity-driver-inputs' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(freeze.ROOT / name, target)
        run.remember({'operation': 'clarity-driver-scope',
            'identity_sha256': freeze.file_hash(own.clarity_identity),
            'default_native_without_engine_flags': True,
            'benchmark_result': False,
            'limits': 'True internal duplicate/replay and empty transaction branches receive source review. Public calls allocate fresh author clocks; empty public arrays are rejected before core.'})

    previous.cohorts.run = cohorts
    previous.preserve = preserve
    sys.argv = [sys.argv[0], *remaining]
    result = previous.main()
    freeze.verify(own.clarity_identity)
    return result


if __name__ == '__main__':
    raise SystemExit(main())
