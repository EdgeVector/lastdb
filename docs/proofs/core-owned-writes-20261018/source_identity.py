#!/usr/bin/env python3
"""Use the reviewed full source inventory with this comparison's frozen reference."""
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / 'core-memory-completion-20261018/source_identity.py'
SPEC = importlib.util.spec_from_file_location('core_owned_source_identity', PRIOR)
previous = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(previous)
previous.HERE = HERE
previous.ROOT = HERE.parents[2]
previous.PRIOR = HERE / 'before-identity.json'
previous.MANIFEST = HERE / 'candidate-identity.json'

if __name__ == '__main__':
    previous.main()
