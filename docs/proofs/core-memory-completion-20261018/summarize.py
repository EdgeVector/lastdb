#!/usr/bin/env python3
"""Reuse prior receipt arithmetic with the completion comparison scope."""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'core-memory-state-20261018'))
import summarize as previous

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-root', type=Path, default=HERE)
    args = parser.parse_args()
    previous.HERE = args.receipt_root.resolve(strict=True)
    previous.main()
    path = previous.HERE / 'comparison.json'
    result = json.loads(path.read_text())
    result['reference_scope'] = 'Before is reviewed v4 bdb83b38; after is the completion build. This is a new comparison, not the original engine comparison.'
    result['limits'] = [item.replace(
        'The comparison isolates the new memory-state integration.',
        'The comparison measures the later completion fixes.') for item in result['limits']]
    path.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')


if __name__ == '__main__':
    main()
