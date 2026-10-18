#!/usr/bin/env python3
"""Use the reviewed exact cleanup workflow for this owned executable root."""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'core-memory-state-20261018'))
import cleanup_baseline

cleanup_baseline.HERE = HERE

if __name__ == '__main__':
    raise SystemExit(cleanup_baseline.main())
