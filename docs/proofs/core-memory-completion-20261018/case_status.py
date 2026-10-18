#!/usr/bin/env python3
"""Read the repeated bounded64 measurements from one finished case."""
import argparse
import json
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('receipt_directory', type=Path)
    args = parser.parse_args()
    rows = []
    with (args.receipt_directory / 'receipts.jsonl').open() as stream:
        for line in stream:
            if 'temperature-read' not in line:
                continue
            row = json.loads(line)
            if (row.get('operation') == 'temperature-read'
                    and row.get('geometry') == 'hashrange'
                    and row.get('shape') == 'bounded' and row.get('width') == 64
                    and row.get('phase') == 'repeated-same-keys'):
                rows.append(row)
    if len(rows) != 5:
        raise RuntimeError('The finished case lacks five repeated bounded64 measurements')
    print(json.dumps({'case': str(args.receipt_directory), 'count': len(rows),
        'median_ms': statistics.median(row['elapsed_nanos'] / 1e6 for row in rows),
        'samples_ms': [row['elapsed_nanos'] / 1e6 for row in rows],
        'file_read_calls': [sum(cost['read_operations'] for cost in row['costs']) for row in rows]}))


if __name__ == '__main__':
    main()
