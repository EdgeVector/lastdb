#!/usr/bin/env python3
"""Reuse the supported exact-path cleanup for this task's owned before program."""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'core-memory-state-20261018'))
import cleanup_baseline as previous

previous.HERE = HERE

if __name__ == '__main__':
    raise SystemExit(previous.main())
