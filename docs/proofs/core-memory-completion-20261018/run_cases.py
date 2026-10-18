#!/usr/bin/env python3
"""Use the prior eight-case method and a separate new receipt directory."""
import argparse
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'core-memory-state-20261018'))
import run_cases as previous

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--receipt-root', type=Path)
    parser.add_argument('--current-first', action='store_true')
    own, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    target = HERE if own.receipt_root is None else own.receipt_root.resolve()
    if target != HERE:
        target.mkdir(parents=False, exist_ok=False)
        for name in ('driver.py', 'api.py', 'workload.py'):
            shutil.copy2(HERE.parent / 'core-memory-state-20261018' / name, target / name)
    previous.HERE = target
    if own.current_first:
        order = ['current-on', 'before-on', 'current-off', 'before-off']
        previous.CASES = [(name, 1) for name in order]
        previous.CASES += [(name, 2) for name in reversed(order)]
    return previous.main()

if __name__ == '__main__':
    raise SystemExit(main())
