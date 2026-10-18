#!/usr/bin/env python3
"""List exact retained receipt roots without a data or workspace walk."""
import argparse
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    values = []
    for path in sorted(HERE.glob('*/retained-root.json')):
        retained = json.loads(path.read_text())
        directory = path.parent
        rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
        starts = [row for row in rows if row.get('operation') == 'owned-node-start']
        stops = [row for row in rows if row.get('operation') == 'owned-node-stop']
        root = Path(retained['root'])
        marker = root / '.synthetic-seed-owner.json'
        values.append({'receipt_directory': str(directory), 'root': str(root),
            'root_exists': root.exists(), 'result': json.loads((directory / 'result.json').read_text()),
            'start_count': len(starts), 'stop_count': len(stops),
            'normal_stop_count': sum(row.get('forced_kill') is False and row.get('exit_code') in (0, -15)
                                     for row in stops),
            'stop_exit_codes': [row.get('exit_code') for row in stops],
            'marker': json.loads(marker.read_text()) if marker.is_file() else None})
    with args.output.open('x') as stream:
        json.dump({'roots': values,
            'scope': 'Bounded one-level receipt inventory only. This is not a process, open-file, socket, or cleanup preflight.'},
            stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({'owned_run_roots': len(values), 'output': str(args.output)}))


if __name__ == '__main__':
    main()
