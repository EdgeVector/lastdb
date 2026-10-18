#!/usr/bin/env python3
"""Summarize preserved ordinary product receipts without another product call."""
import argparse
import json
from pathlib import Path


def summarize(directory):
    rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
    seed = []
    for row in rows:
        if row.get('operation') == 'owned-node-stop':
            break
        seed.append(row)
    acknowledgements = [row for row in seed if row.get('operation') == 'mutation'
                        and row.get('phase') == 'seed']
    flushes = [row for row in seed if row.get('operation') == 'flush']
    statuses = [row for row in rows if row.get('operation') == 'status-observation']
    selected = {'first-exact-before', 'first-exact-after', 'first-range-before', 'first-range-after',
                'concurrent-first-before', 'concurrent-first-after', 'final-flushed', 'final'}
    return {
        'directory': str(directory),
        'inputs': json.loads((directory / 'inputs.json').read_text()),
        'summary': json.loads((directory / 'summary.json').read_text()),
        'seed_requests': {
            'acknowledged_records': sum(row['batch'] for row in acknowledgements),
            'mutation_batches': len(acknowledgements),
            'mutation_ack_sum_ms': sum(row['elapsed_nanos'] for row in acknowledgements) / 1e6,
            'explicit_flush_calls': len(flushes),
            'explicit_flush_sum_ms': sum(row['elapsed_nanos'] for row in flushes) / 1e6,
            'measurement': 'Sum of request durations in one run; not elapsed seed wall time or a throughput distribution'},
        'write_observations': [dict(row, elapsed_ms=row['elapsed_nanos'] / 1e6)
            for row in rows if row.get('operation') == 'mutation' and row.get('phase') != 'seed'],
        'disk_observations': [row for row in rows if row.get('operation') == 'disk-size-observation'],
        'max_sampled_process_memory_bytes': max((row['sampled_process_memory_bytes'] for row in statuses), default=None),
        'selected_statuses': [row for row in statuses if row['phase'] in selected],
        'query_observations': [row for row in rows if row.get('operation') == 'query-observation'],
        'refusals': [row for row in rows if row.get('operation') == 'refusal'],
        'query_count': sum(row.get('operation') == 'query' for row in rows),
        'stops': [row for row in rows if row.get('operation') == 'owned-node-stop']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='append', required=True, help='label=direct-run-directory-name')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    result = {}
    for argument in args.run:
        label, separator, name = argument.partition('=')
        directory = (base / name).resolve(strict=True)
        if not separator or not label or directory.parent != base:
            raise RuntimeError('Use a label and one direct preserved run directory')
        result[label] = summarize(directory)
    output = args.output.resolve()
    if output.parent != base:
        raise RuntimeError('Save the derived receipt in this proof directory')
    output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'output': str(output), 'runs': list(result)}))


if __name__ == '__main__':
    main()
