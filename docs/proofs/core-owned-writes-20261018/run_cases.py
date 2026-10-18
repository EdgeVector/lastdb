#!/usr/bin/env python3
"""Use the unchanged eight ordinary-API timing cases for the core-owned comparison."""
import argparse
import importlib.util
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / 'core-memory-state-20261018'
sys.path.insert(0, str(PRIOR))
SPEC = importlib.util.spec_from_file_location('core_owned_timing_cases', PRIOR / 'run_cases.py')
previous = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(previous)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-root', type=Path, required=True)
    own, remaining = parser.parse_known_args()
    target = own.receipt_root.resolve()
    target.mkdir(parents=False, exist_ok=False)
    for name in ('driver.py', 'api.py', 'workload.py'):
        shutil.copy2(PRIOR / name, target / name)
    previous.HERE = target
    order = ['current-on', 'before-on', 'current-off', 'before-off']
    previous.CASES = [(name, 1) for name in order]
    previous.CASES += [(name, 2) for name in reversed(order)]
    sys.argv = [sys.argv[0], *remaining]
    return previous.main()

if __name__ == '__main__':
    raise SystemExit(main())
