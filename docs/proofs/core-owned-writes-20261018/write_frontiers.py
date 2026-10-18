#!/usr/bin/env python3
"""Report actual ACK frontiers and allowance refusals from retained API receipts."""
import argparse
import hashlib
import json
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def status(native):
    model = native.get('core_model', {})
    return {
        key: native.get(key) for key in (
            'accepted_sequence', 'completed_sequence', 'accepted_batches',
            'core_warm_dirty', 'core_warm_held', 'writer_failed')
    } | {'core_model': model}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-root', type=Path, required=True)
    args = parser.parse_args()
    root = args.receipt_root.resolve(strict=True)
    rows, inputs = [], []
    for label in ('before-on', 'before-off', 'current-on', 'current-off'):
        for round_number in (1, 2):
            case = f'{label}-r{round_number}'
            path = root / case / 'receipts.jsonl'
            receipts = [json.loads(line) for line in path.read_text().splitlines()]
            requests = {row['request_id']: row for row in receipts
                        if row.get('operation') == 'request'}
            inputs.append({'path': str(path), 'sha256': digest(path),
                           'bytes': path.stat().st_size})
            for row in receipts:
                if row.get('operation') != 'temperature-write':
                    continue
                before = row['native_before']
                after = row['native_after_ack']
                first = before.get('core_model', {}).get('refused')
                last = after.get('core_model', {}).get('refused')
                request = requests[row['request_id']]
                result = request.get('result', {})
                rows.append({
                    'case': case, 'label': label, 'round': round_number,
                    **{key: row[key] for key in ('geometry', 'condition', 'repeat',
                                               'width', 'request_id')},
                    'ack_ms': row['ack_elapsed_nanos'] / 1_000_000,
                    'requested_mode': row['requested_durability'],
                    'returned_mode': row['returned_durability'],
                    'pending_at_ack': after.get('accepted_batches', 0) > 0,
                    'caught_up_at_ack': after['accepted_sequence'] == after['completed_sequence'],
                    'model_refused_delta': None if first is None or last is None else last - first,
                    'resident_stages_us': result.get('resident_stages_us'),
                    'native_before': status(before),
                    'native_after_ack': status(after),
                    'native_after_flush': status(row['native_after_flush']),
                })
    groups = []
    for label in ('before-on', 'before-off', 'current-on', 'current-off'):
        for geometry in ('hash', 'hashrange'):
            for condition in ('new-key-after-reopen', 'update-no-prior-value-read', 'update-read-primed'):
                selected = [row for row in rows if (row['label'], row['geometry'], row['condition'])
                            == (label, geometry, condition)]
                if not selected:
                    raise RuntimeError(f'Missing write group: {label}/{geometry}/{condition}')
                groups.append({'label': label, 'geometry': geometry, 'condition': condition,
                    'samples': len(selected),
                    'returned_modes': sorted({row['returned_mode'] for row in selected}),
                    'pending_at_ack': sum(row['pending_at_ack'] for row in selected),
                    'caught_up_at_ack': sum(row['caught_up_at_ack'] for row in selected),
                    'dirty_at_ack': sum(row['native_after_ack']['core_warm_dirty'] > 0 for row in selected),
                    'model_refused_observed': all(row['model_refused_delta'] is not None for row in selected),
                    'model_refused_delta': sum(row['model_refused_delta'] or 0 for row in selected)})
    result = {'inputs': inputs, 'groups': groups, 'writes': rows,
        'limits': ['Status follows the API reply and can observe a subsequent flush.',
                   'Caught-up status alone does not prove a disk-first reply.',
                   'A queued reply alone does not prove memory-only preparation.',
                   'Absent reference core_model counters are unavailable, not zero.']}
    (root / 'write-frontiers.json').write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    lines = ['# Actual write ACK observations', '',
        'This report derives from the unchanged ordinary API receipts. It adds no product calls.', '',
        '| Program and preload | Key form | Caller condition | Samples | Pending after ACK | Caught up after ACK | Dirty after ACK | Model refusals |',
        '|---|---|---|---:|---:|---:|---:|---:|']
    for group in groups:
        refused = str(group['model_refused_delta']) if group['model_refused_observed'] else 'unavailable'
        lines.append(f"| {group['label']} | {group['geometry']} | {group['condition']} | {group['samples']} | {group['pending_at_ack']} | {group['caught_up_at_ack']} | {group['dirty_at_ack']} | {refused} |")
    lines += ['', 'All reply modes in this run are queued. Status follows the reply and can observe a subsequent flush.',
        'Caught-up status alone does not prove a disk-first reply. A queued reply does not prove memory-only preparation.',
        'The JSON retains every request ID, response phase, allowance counter, and before/after status.', '']
    (root / 'WRITE-FRONTIERS.md').write_text('\n'.join(lines))
    print(json.dumps({'write_observations': len(rows), 'groups': len(groups),
        'current_refusals': sum(row['model_refused_delta'] or 0 for row in rows
                                if row['label'].startswith('current-'))}))


if __name__ == '__main__':
    main()
