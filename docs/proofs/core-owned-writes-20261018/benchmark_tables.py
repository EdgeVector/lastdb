#!/usr/bin/env python3
"""Pair every retained read and write group without another product call."""
import argparse
import json
from pathlib import Path


def change(before, current):
    return 100 * (current / before - 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-root', type=Path, required=True)
    args = parser.parse_args()
    data = json.loads((args.receipt_root / 'comparison.json').read_text())
    reads, writes = [], []
    for policy in ('off', 'on'):
        before = {tuple(row[key] for key in ('geometry', 'shape', 'width', 'phase')): row
                  for row in data['reads'] if row['label'] == 'before-' + policy}
        current = {tuple(row[key] for key in ('geometry', 'shape', 'width', 'phase')): row
                   for row in data['reads'] if row['label'] == 'current-' + policy}
        if before.keys() != current.keys():
            raise RuntimeError('The two programs lack matching read groups')
        for key in before:
            left, right = before[key], current[key]
            reads.append({'preload': policy, **dict(zip(('geometry', 'shape', 'width', 'phase'), key)),
                'before': left, 'current': right,
                'median_change_percent': change(left['median_ms'], right['median_ms'])})
        before = {tuple(row[key] for key in ('geometry', 'condition')): row
                  for row in data['writes'] if row['label'] == 'before-' + policy}
        current = {tuple(row[key] for key in ('geometry', 'condition')): row
                   for row in data['writes'] if row['label'] == 'current-' + policy}
        if before.keys() != current.keys():
            raise RuntimeError('The two programs lack matching write groups')
        for key in before:
            left, right = before[key], current[key]
            writes.append({'preload': policy, **dict(zip(('geometry', 'condition'), key)),
                'before': left, 'current': right,
                'ack_median_change_percent': change(left['ack']['median_ms'], right['ack']['median_ms']),
                'flush_median_change_percent': change(left['through_flush']['median_ms'], right['through_flush']['median_ms'])})
    result = {'reads': reads, 'writes': writes,
        'scope': data['reference_scope'], 'limits': data['limits']}
    (args.receipt_root / 'paired-tables.json').write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    text = ['# Matched measurements', '', data['reference_scope'], '',
        'Each latency is in milliseconds. A positive change means a longer latency.', '',
        '## Writes', '', '| Preload | Key form | Caller condition | Ack before | Ack after | Ack change | Through flush before | Through flush after | Flush change | Reply modes |',
        '|---|---|---|---:|---:|---:|---:|---:|---:|---|']
    for row in writes:
        left, right = row['before'], row['current']
        modes = '/'.join(sorted(set(left['returned_modes']))) + ' → ' + '/'.join(sorted(set(right['returned_modes'])))
        text.append(f"| {row['preload']} | {row['geometry']} | {row['condition']} | {left['ack']['median_ms']:.3f} | {right['ack']['median_ms']:.3f} | {row['ack_median_change_percent']:+.1f}% | {left['through_flush']['median_ms']:.3f} | {right['through_flush']['median_ms']:.3f} | {row['flush_median_change_percent']:+.1f}% | {modes} |")
    text += ['', 'New-key and no-prior-value-read groups have two samples. Read-primed updates have ten samples.',
        'A queued reply does not establish memory-only preparation or absence of a disk wait.', '',
        '## Reads', '', '| Preload | Key form | Shape | Width | Caller condition | Before | After | Change | Samples |',
        '|---|---|---|---:|---|---:|---:|---:|---:|']
    for row in reads:
        left, right = row['before'], row['current']
        text.append(f"| {row['preload']} | {row['geometry']} | {row['shape']} | {row['width']} | {row['phase']} | {left['median_ms']:.3f} | {right['median_ms']:.3f} | {row['median_change_percent']:+.1f}% | {left['samples']} |")
    text += ['', 'The paired JSON retains every sample, minimum, maximum, reported file cost, and write status observation.',
        'Process reopen does not clear the OS file cache. This is not a maximum throughput measurement.', '']
    (args.receipt_root / 'TABLES.md').write_text('\n'.join(text))
    print(json.dumps({'paired_read_groups': len(reads), 'paired_write_groups': len(writes)}))


if __name__ == '__main__':
    main()
