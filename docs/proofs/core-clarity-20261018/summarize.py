#!/usr/bin/env python3
"""Summarize exact ordinary API receipts without another product operation."""
import argparse
import collections
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def identity(path):
    return {'path': str(path), 'bytes': path.stat().st_size,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def summarize(directory):
    result = json.loads((directory / 'result.json').read_text())
    rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
    starts = [row for row in rows if row.get('operation') == 'owned-node-start']
    stops = [row for row in rows if row.get('operation') == 'owned-node-stop']
    reads = [row for row in rows if row.get('operation') == 'core-owned-read']
    zero = [row for row in reads if row['require_zero_file_calls']]
    statuses = [row for row in rows if row.get('operation') == 'native-status']
    frontiers = [{'phase': row['phase'], **{name: row['native'][name] for name in
        ('accepted_sequence', 'completed_sequence', 'accepted_batches', 'core_warm_dirty')}}
        for row in statuses if 'after-ack-before' in row['phase']]
    initial = [row for row in frontiers if row['phase'] ==
               'new-complete-queued-batch-after-ack-before-first-value-read']
    default_ack = None if not initial else all(row['accepted_sequence'] > row['completed_sequence']
        and row['accepted_batches'] > 0 and row['core_warm_dirty'] > 0 for row in initial)
    normal = [row for row in stops if row.get('forced_kill') is False
              and row.get('exit_code') in (0, -15)]
    prefixes = [{name: row[name] for name in ('phase', 'records', 'file_read_calls',
        'require_zero_file_calls', 'all_values_match')} for row in rows
        if row.get('operation') == 'literal-encryption-prefix-read']
    purges = [{name: row[name] for name in ('phase', 'uuids', 'http_status',
        'source_reviewed_deletion', 'reclaimed_disk_bytes_proved')} for row in rows
        if row.get('operation') == 'warm-purge-public-uuid-absence']
    special = [row for row in rows if row.get('operation') in (
        'expected-operation-refusal', 'native-unsupported-filter-refusals',
        'native-as-of-query-and-count-refusals', 'empty-public-batch-no-write-frontier',
        'native-cloud-on-refused-before-intent-change', 'public-repeat-contract',
        'native-selector-remediation-complete')]
    driver = directory / 'response-driver-identity.json'
    if not driver.is_file():
        driver = directory / 'clarity-driver-identity.json'
    value = {'version': 1, 'result': result, 'benchmark_result': False,
        'receipt_directory': str(directory), 'raw_receipts': identity(directory / 'receipts.jsonl'),
        'source_identity': identity(directory / 'source-identity.json'),
        'driver_identity': identity(driver),
        'binary_sha256': json.loads((directory / 'driver-inputs.json').read_text())['binary_sha256'],
        'operation_counts': dict(collections.Counter(row.get('operation') for row in rows)),
        'keyed_value_checks': len(reads), 'required_zero_file_checks': len(zero),
        'zero_file_checks_pass': all(row['file_read_calls'] == 0 for row in zero),
        'default_memory_ack_pass': default_ack, 'actual_ack_frontiers': frontiers,
        'default_native_boots': sum(row.get('operation') == 'default-native-format' for row in rows),
        'all_processes_remove_engine_and_mode_flags': all(all(value is None for value in
            row['engine_selection_environment'].values()) for row in starts),
        'node_sessions': len(starts), 'all_nodes_stop_normally':
            collections.Counter(row['pid'] for row in starts) ==
            collections.Counter(row['pid'] for row in normal) and len(stops) == len(normal),
        'exit_codes': [row['exit_code'] for row in stops],
        'literal_prefix_checks': prefixes, 'warm_purge_uuid_absence': purges,
        'special_acceptance_receipts': special,
        'evidence_scope': 'Tracked acceptance summary. Exact raw JSONL stays local and ignored; the receipt seal records its bytes and SHA256.',
        'limits': ['Repeated public payloads allocate fresh clocks; no internal idempotency hit is claimed.',
            'The empty public array is refused before core; the internal empty transaction is source-only.',
            'UUID404 proves addressable absence, not reclaimed disk bytes or zero-file UUID reads.',
            'Unsupported embedded API and old-format boundaries receive source review; these workflows use normal node APIs.']}
    with (directory / 'acceptance.json').open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({'workflow': result['workflow'], 'complete': result['complete'],
        'value_checks': len(reads), 'required_zero_file_checks': len(zero),
        'sessions': len(starts), 'default_memory_ack_pass': default_ack}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directories', type=Path, nargs='+', required=True)
    args = parser.parse_args()
    for supplied in args.directories:
        directory = supplied.resolve(strict=True)
        if directory.parent != HERE:
            raise RuntimeError('Use an exact run directory in this proof')
        summarize(directory)


if __name__ == '__main__':
    main()
