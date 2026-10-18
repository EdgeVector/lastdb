#!/usr/bin/env python3
"""Reuse the reviewed exact-root normal-stop cleanup for this comparison's receipts."""
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / 'core-memory-state-20261018/cleanup_owned.py'
SPEC = importlib.util.spec_from_file_location('core_owned_cleanup', PRIOR)
previous = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(previous)

if __name__ == '__main__':
    raise SystemExit(previous.main())
