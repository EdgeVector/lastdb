#!/usr/bin/env python3
"""Summarize retained ordinary API receipts without another product call."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

BODY_FIELDS = ('body_reserved_current_bytes', 'body_reserved_used_bytes',
               'body_reserved_retained_bytes', 'body_reserved_peak_bytes')
META_FIELDS = ('metadata_current_bytes', 'metadata_used_bytes',
               'metadata_retained_bytes', 'metadata_peak_bytes')


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def summarize(directory):
    result = json.loads((directory / 'result.json').read_text())
    counts, reads, writes, observations, frontiers, errors = Counter(), [], [], [], [], []
    with (directory / 'receipts.jsonl').open() as stream:
        for line in stream:
            row = json.loads(line)
            operation = row.get('operation', 'unknown')
            counts[operation] += 1
            if operation == 'core-owned-read':
                reads.append({key: row[key] for key in ('phase', 'geometry', 'bounded',
                    'require_zero_file_calls', 'candidate_write_through_rule',
                    'reference_observation_only', 'file_read_calls', 'request_id')})
            if operation == 'core-owned-write':
                writes.append({key: row[key] for key in ('phase', 'count', 'returned_mode', 'request_id')})
            if operation == 'core-owned-memory-observation':
                observations.append(row)
            if operation == 'native-status':
                native = row['native']
                frontiers.append({'phase': row['phase'], **{key: native.get(key) for key in (
                    'accepted_batches', 'accepted_sequence', 'completed_sequence',
                    'core_warm_dirty', 'core_warm_held', 'writer_failed')}})
            if operation in ('failure', 'refusal', 'transport-error', 'malformed-response'):
                errors.append({key: row.get(key) for key in (
                    'operation', 'path', 'request_id', 'http_status', 'error')})
    maxima = {}
    for key in BODY_FIELDS + META_FIELDS:
        values = [row['core_model'].get(key) for row in observations
                  if isinstance(row.get('core_model'), dict)]
        values = [value for value in values if isinstance(value, int)]
        maxima[key] = max(values) if values else None
    fields = BODY_FIELDS + META_FIELDS
    memory_rows = [{'phase': row['phase'],
        'active_client_read_calls': row['active_client_read_calls'],
        'core_model': None if not isinstance(row.get('core_model'), dict) else {
            key: row['core_model'].get(key) for key in fields},
        'deferred_persist_bytes': row['deferred_persist_bytes'],
        'deferred_persist_cap_bytes': row['deferred_persist_cap_bytes'],
        'sampler_timestamp': row['sampler_timestamp']} for row in observations]
    return {'receipt_directory': str(directory), 'result': result,
        'receipt_sha256': sha(directory / 'receipts.jsonl'), 'operation_counts': dict(counts),
        'write_route_reads': reads, 'writes': writes, 'native_frontiers': frontiers,
        'memory': {'direct_observed_maxima': maxima, 'observations': memory_rows,
            'scope': 'Direct core counters and published sampler differ. Client overlap cannot identify an exact internal read capture. Retained bytes can include private preparation.'},
        'errors': errors, 'benchmark_result': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipts', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    reports = [summarize(path.resolve(strict=True)) for path in args.receipts]
    with args.output.open('x') as stream:
        json.dump({'cohorts': reports, 'scope': 'Operational receipt summary, without timing claims.'},
                  stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({'cohorts': len(reports), 'output': str(args.output)}))


if __name__ == '__main__':
    main()
