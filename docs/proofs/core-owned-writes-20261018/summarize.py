#!/usr/bin/env python3
"""Use the unchanged receipt arithmetic and state the exact new comparison scope."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / 'core-memory-state-20261018/summarize.py'
SPEC = importlib.util.spec_from_file_location('core_owned_timing_summary', PRIOR)
previous = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(previous)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-root', type=Path, required=True)
    args = parser.parse_args()
    previous.HERE = args.receipt_root.resolve(strict=True)
    previous.main()
    path = previous.HERE / 'comparison.json'
    result = json.loads(path.read_text())
    result['reference_scope'] = ('Before is accepted completion a0ad6190 from merged main011b29f3. '
        'After is the independently frozen core-owned-writes candidate. This does not repeat the '
        'original engine, e10-to-v4, or v4-to-completion comparisons.')
    result['limits'] = [item.replace('The comparison isolates the new memory-state integration.',
        'The comparison measures the core-owned logical write migration.') for item in result['limits']]
    path.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')

if __name__ == '__main__':
    main()
