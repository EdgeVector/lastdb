#!/usr/bin/env python3
"""Execute the twelve reviewed final exact-root cleanup commands sequentially."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
BASE = Path('/Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--review-json', type=Path, required=True)
    args = parser.parse_args()
    review_path = args.review_json.resolve(strict=True)
    review = json.loads(review_path.read_text())
    rows = review['cases']
    if (len(rows) != 12 or len({row['root'] for row in rows}) != 12
            or review['case_root_count'] != 10 or review['capture_root_count'] != 2
            or not review['all_supported_previews_pass']):
        raise RuntimeError('The reviewed final cleanup inventory differs')
    output_path = review_path.parent / 'cleanup-execution.json'
    if output_path.exists():
        raise RuntimeError('The final cleanup execution receipt already exists')
    outcomes = []
    for row in rows:
        directory = (BASE / row['receipt_directory']).resolve(strict=True)
        if review_path.parent not in directory.parents:
            raise RuntimeError('A reviewed receipt directory escapes the final proof root')
        preflight = json.loads((directory / 'cleanup-preflight.json').read_text())
        if any(preflight[key] != row[key] for key in ('root', 'realpath', 'device', 'inode', 'marker')):
            raise RuntimeError('A preflight differs from the reviewed final inventory')
        helper = HERE / ('cleanup_capture.py' if row['node_started'] is False else 'cleanup_owned.py')
        command = [sys.executable, str(helper), '--receipt-dir', str(directory),
                   '--execute-reviewed-preview']
        result = subprocess.run(command, cwd=BASE, capture_output=True, text=True, check=False)
        outcome = {'root': row['root'], 'receipt_directory': str(directory), 'command': command,
                   'exit_code': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr}
        receipt_path = directory / 'cleanup.json'
        if receipt_path.is_file():
            outcome['cleanup_receipt'] = json.loads(receipt_path.read_text())
        outcomes.append(outcome)
        complete = len(outcomes) == 12 and all(item['exit_code'] == 0
            and item.get('cleanup_receipt', {}).get('root') == item['root']
            and item['cleanup_receipt'].get('root_exists_after') is False for item in outcomes)
        output_path.write_text(json.dumps({'review_json': str(review_path), 'complete': complete,
            'executed_count': len(outcomes), 'reviewed_count': 12, 'outcomes': outcomes},
            sort_keys=True, indent=2) + '\n')
        print(json.dumps({'root': row['root'], 'exit_code': result.returncode,
            'retained': outcome.get('cleanup_receipt', {}).get('root_exists_after')}), flush=True)
        if result.returncode != 0:
            return result.returncode
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
