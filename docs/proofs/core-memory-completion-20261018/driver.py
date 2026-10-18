#!/usr/bin/env python3
"""Run the unchanged prior ordinary workload with this proof's receipt path."""
from pathlib import Path
import runpy
import sys

sys.dont_write_bytecode = True
PRIOR = Path(__file__).resolve().parent.parent / 'core-memory-state-20261018'
sys.path.insert(0, str(PRIOR))

if __name__ == '__main__':
    runpy.run_path(str(PRIOR / 'driver.py'), run_name='__main__')
