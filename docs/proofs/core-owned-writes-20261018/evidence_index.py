#!/usr/bin/env python3
"""Index only this comparison's exact local evidence with the existing ignore policy."""
import importlib.util
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
PRIOR = HERE.parent / 'core-memory-completion-20261018/evidence_index.py'
SPEC = importlib.util.spec_from_file_location('core_owned_evidence_index', PRIOR)
previous = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(previous)
previous.HERE, previous.ROOT = HERE, ROOT


def listed(*flags):
    result = subprocess.run(['git', 'ls-files', *flags, '--', str(HERE.relative_to(ROOT))],
                            cwd=ROOT, capture_output=True, text=True, check=True)
    return result.stdout.splitlines()


previous.listed = listed

if __name__ == '__main__':
    previous.main()
