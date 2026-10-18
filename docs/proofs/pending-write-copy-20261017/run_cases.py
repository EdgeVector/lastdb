#!/usr/bin/env python3
"""Run the approved eight ordinary Mini cases in an alternating order."""
import json
from pathlib import Path
import subprocess
import sys
import time

BASE = Path('/Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016')
HERE = Path(__file__).resolve().parent
BEFORE = Path('/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/sp.grne1ikz/program-lastdbd')
AFTER = BASE / 'target/release/lastdbd'
HASHES = {'before': '739ab5e80e0cd915e4c42c2a13cd0e168f22410ceed0df4581aa0e2e1f92ffbb',
          'after': 'c5e7b74b069e4e914f84bd57751984bfd2866597602516b74b2cdf8d376d169a'}
CASES = [('before', 'serial', 1), ('after', 'serial', 1),
         ('before', 'concurrent4', 1), ('after', 'concurrent4', 1),
         ('after', 'concurrent4', 2), ('before', 'concurrent4', 2),
         ('after', 'serial', 2), ('before', 'serial', 2)]


def main():
    outcomes = []
    for index, (variant, shape, number) in enumerate(CASES, 1):
        name = f'{variant}-{shape}-r{number}'
        command = [sys.executable, '-u', str(HERE / 'benchmark.py'),
            '--expected-sha256', HASHES[variant], '--shape', shape, '--label', variant,
            '--round', str(number), '--binary', str(BEFORE if variant == 'before' else AFTER),
            '--variant', name, '--receipt-dir', str(HERE / name),
            '--records-per-hash', '256', '--hashes', '2', '--body-bytes', '1024',
            '--cap', '10000', '--seed-batch', '64', '--read-rounds', '2',
            '--counts', '16,64,256', '--concurrent-width', '16',
            '--source-commit', variant + '-release-' + HASHES[variant][:12]]
        started = time.perf_counter_ns()
        result = subprocess.run(command, cwd=BASE, capture_output=True, text=True, check=False)
        (HERE / (name + '.stdout')).write_text(result.stdout)
        (HERE / (name + '.stderr')).write_text(result.stderr)
        row = {'case': index, 'name': name, 'exit_code': result.returncode,
               'supervisor_elapsed_seconds': (time.perf_counter_ns() - started) / 1e9,
               'command': command}
        outcome = HERE / name / 'benchmark-result.json'
        if outcome.is_file():
            saved = json.loads(outcome.read_text())
            row.update(complete=saved['complete'], error=saved['error'])
            if saved['result'] is not None:
                row.update(records_per_second=saved['result']['logical_records_per_second'],
                           max_observed_accepted_batches=saved['result']['max_observed_accepted_batches'],
                           mutation_error_count=saved['result']['mutation_error_count'])
        outcomes.append(row)
        (HERE / 'run-order.json').write_text(json.dumps(outcomes, sort_keys=True, indent=2) + '\n')
        print(json.dumps({key: value for key, value in row.items() if key != 'command'}), flush=True)
    return 0 if all(row['exit_code'] == 0 for row in outcomes) else 1


if __name__ == '__main__':
    raise SystemExit(main())
