#!/usr/bin/env python3
"""Summarize retained ordinary responses without another product call."""
import json
import math
from pathlib import Path
import statistics

HERE = Path(__file__).resolve().parent


def sample_stats(values):
    ordered = sorted(values)
    return {'samples': len(values), 'median_ms': statistics.median(values),
            'min_ms': min(values), 'max_ms': max(values), 'values_ms': values,
            'observed_p95_ms': ordered[math.ceil(.95 * len(values)) - 1] if len(values) >= 5 else None}


def costs(row, name):
    values = [cost.get(name) for cost in row['costs']]
    return sum(values) if all(isinstance(value, (int, float)) for value in values) else None


def main():
    cases, read_groups, write_groups = [], {}, {}
    for label in ('before', 'current-off', 'current-on'):
        for number in (1, 2):
            directory = HERE / f'{label}-r{number}'
            result = json.loads((directory / 'result.json').read_text())
            rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
            cases.append({'label': label, 'round': number, 'result': result,
                'binary_sha256': json.loads((directory / 'inputs.json').read_text())['binary_sha256'],
                'receipt_directory': str(directory)})
            for row in rows:
                if row.get('operation') == 'temperature-read' and row['phase'] in (
                        'first-after-reopen', 'repeated-same-keys', 'other-keys-same-process'):
                    key = (label, row['geometry'], row['shape'], row['width'], row['phase'])
                    read_groups.setdefault(key, []).append(dict(row, round=number))
                if row.get('operation') == 'temperature-write':
                    key = (label, row['geometry'], row['condition'])
                    write_groups.setdefault(key, []).append(dict(row, round=number))
    reads, writes = [], []
    for (label, geometry, shape, width, phase), rows in read_groups.items():
        row = {'label': label, 'geometry': geometry, 'shape': shape, 'width': width,
            'phase': phase, 'endpoint': rows[0]['endpoint'],
            **sample_stats([sample['elapsed_nanos'] / 1e6 for sample in rows]),
            'rounds': [sample['round'] for sample in rows]}
        row['cost_sample_totals'] = {name: [costs(sample, name) for sample in rows] for name in (
            'read_operations', 'decrypted_bytes', 'logical_warm_hits', 'logical_warm_misses')}
        reads.append(row)
    for (label, geometry, condition), rows in write_groups.items():
        wall = sum(sample['wall_nanos_through_explicit_flush'] for sample in rows) / 1e9
        count = sum(sample['width'] for sample in rows)
        payload = sum(sample['serialized_field_payload_bytes'] for sample in rows)
        writes.append({'label': label, 'geometry': geometry, 'condition': condition,
            'ack': sample_stats([sample['ack_elapsed_nanos'] / 1e6 for sample in rows]),
            'through_flush': sample_stats([sample['wall_nanos_through_explicit_flush'] / 1e6 for sample in rows]),
            'returned_modes': [sample['returned_durability'] for sample in rows],
            'total_records': count, 'total_wall_seconds': wall,
            'records_per_second_total_work_over_total_wall': count / wall,
            'serialized_field_payload_MB_per_second': payload / 1e6 / wall,
            'native_observations': [{'round': sample['round'], 'repeat': sample['repeat'],
                'before': sample['native_before'], 'after_ack': sample['native_after_ack'],
                'after_flush': sample['native_after_flush']} for sample in rows]})
    summary = {'cases': cases, 'all_cases_complete': all(case['result']['complete'] for case in cases),
        'reads': reads, 'writes': writes, 'limits': [
            'Each first read has two observations, one from each fresh home. Five repeats per home supply ten repeated samples.',
            'Process reopen does not clear the OS file cache. The host has no exclusive reservation.',
            'Hash widths16/64 use a batch of independent queries; HashRange uses one keyed query. Width1 uses the same /api/query endpoint.',
            'No-prior-read and read-primed are caller conditions. Write preparation can fetch state in both conditions.',
            'Write wall time includes a native-status call between ack and final explicit flush.',
            'Returned queued mode or global pending counters alone do not establish memory-only execution.',
            'The original program lacks newer native warm/key/raw-record metrics; missing fields remain unavailable.',
            'No failed request is retried or hidden. Observed p95 is not a latency guarantee.']}
    (HERE / 'comparison.json').write_text(json.dumps(summary, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'all_cases_complete': summary['all_cases_complete'],
                      'read_groups': len(reads), 'write_groups': len(writes)}))


if __name__ == '__main__':
    main()
