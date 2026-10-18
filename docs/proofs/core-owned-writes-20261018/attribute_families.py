#!/usr/bin/env python3
"""Associate fixed raw-read family logs with preserved ordinary request time windows."""
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re

PATTERN = re.compile(r'^(\S+) DEBUG lastdb::native_read_family: .* family="([^"]+)" keys=(\d+)$')


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = [json.loads(line) for line in (args.receipt_dir / 'receipts.jsonl').read_text().splitlines()]
    windows = {row['request_id']: row for row in rows if row.get('operation') == 'diagnostic-call-window'}
    logs = []
    for number, line in enumerate((args.receipt_dir / 'node-c.log').read_text().splitlines(), 1):
        match = PATTERN.match(line)
        if match:
            logs.append({'utc': match[1], 'line': number,
                         'family': match[2], 'keys': int(match[3])})
    answers = []
    for row in rows:
        if row.get('operation') != 'core-owned-read':
            continue
        window = windows[row['request_id']]
        selected = [entry for entry in logs
                    if instant(window['started_utc']) <= instant(entry['utc']) <= instant(window['finished_utc'])]
        families = Counter()
        for entry in selected:
            families[entry['family']] += entry['keys']
        answers.append({'request_id': row['request_id'], 'phase': row['phase'],
            'schema': row['schema'], 'db_locator': row.get('db_locator'),
            'file_read_calls': row['file_read_calls'], 'window': window,
            'raw_requested_family_keys': dict(families), 'trace_entries': selected})
    value = {'receipt_directory': str(args.receipt_dir.resolve()), 'queries': answers,
        'scope': 'Time-window association of fixed family logs. Background calls can share a window. Raw requested keys do not equal file reads. The strict zero-file candidate receipt remains failed.'}
    with args.output.open('x') as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write('\n')
    print(json.dumps({'queries': len(answers), 'output': str(args.output)}))


if __name__ == '__main__':
    main()
