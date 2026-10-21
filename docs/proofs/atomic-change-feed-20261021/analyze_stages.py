#!/usr/bin/env python3
"""Summarize captured pipeline clocks only after the paired comparison completes."""
import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics

HERE = Path(__file__).resolve().parent
ORDER = ['before-r1', 'after-r1', 'after-r2', 'before-r2']
STAGES = ('prepare', 'gate_wait', 'publish', 'ack')


def load(path):
    return json.loads(path.read_text())


def require(value, message):
    if not value:
        raise ValueError(message)


def identity(path):
    return {'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def distribution(values):
    values = sorted(values)
    require(bool(values), 'A timing distribution has no samples')
    return {'samples': len(values), 'sum_us': sum(values), 'mean_us': statistics.mean(values),
        'p50_us': statistics.median(values), 'p95_us': values[math.ceil(.95 * len(values)) - 1],
        'min_us': values[0], 'max_us': values[-1], 'negative_samples': sum(value < 0 for value in values),
        'zero_samples': sum(value == 0 for value in values)}


def request_timing(row, label):
    stages = row.get('result', {}).get('resident_stages_us')
    require(isinstance(stages, dict) and all(type(stages.get(name)) is int
            and stages[name] >= 0 for name in STAGES), 'A response lacks its four pipeline clocks')
    require(type(row.get('elapsed_ns')) is int
            and row['elapsed_ns'] == row['finish_ns'] - row['start_ns'] > 0,
            'An original HTTP timing is invalid')
    require(row['http_status'] == 200 and row['result'].get('durability') == 'durable',
            'A stage sample is not a durable successful response')
    return {name: row[name] for name in ('geometry', 'batch', 'revision', 'first', 'request_id')} | {
        'label': label, 'variant': label.split('-')[0], 'stages_us': stages,
        'http_elapsed_ns': row['elapsed_ns'], 'http_us': row['elapsed_ns'] / 1000,
        'http_minus_inner_receipt_us': row['elapsed_ns'] / 1000 - stages['ack'],
        'inner_receipt_minus_prepare_us': stages['ack'] - stages['prepare']}


def summarize(rows):
    mutations = sum(row['batch'] for row in rows)
    stage_values = {name: distribution([row['stages_us'][name] for row in rows]) for name in STAGES}
    for value in stage_values.values():
        value['mean_us_per_logical_mutation'] = value['sum_us'] / mutations
    return {'requests': len(rows), 'logical_mutations': mutations,
        'http_elapsed_us': distribution([row['http_us'] for row in rows]),
        'resident_stages_us': stage_values,
        'derived_differences_us': {name: distribution([row[name] for row in rows])
            for name in ('http_minus_inner_receipt_us', 'inner_receipt_minus_prepare_us')}}


def per_cell(rows, comparison):
    grouped = defaultdict(list)
    for row in rows:
        grouped[tuple(row[name] for name in ('label', 'geometry', 'batch', 'revision', 'first'))].append(row)
    cells = []
    for source in comparison['cells']:
        key = tuple(source[name] for name in ('label', 'geometry', 'batch', 'revision', 'first'))
        selected = grouped.pop(key, [])
        require(len(selected) == source['requests'] == 16, 'A completed cell lacks its original clocks')
        require(sorted(row['http_elapsed_ns'] for row in selected) == sorted(source['request_elapsed_ns']),
                'The stage response timings differ from the independently checked original requests')
        cells.append({name: source[name] for name in ('label', 'variant', 'geometry', 'batch', 'revision', 'first')}
            | summarize(selected) | {'counter_observations_at_status_endpoints': {
                name: source[name] for name in ('post_http_status', 'through_notice_drain')}})
    require(not grouped and len(cells) == 48, 'Stage cells differ from the independently checked matrix')
    return cells


def analyze(directory, comparison_path):
    # This gate precedes all per-run reads. Never inspect partial paired receipts.
    completed = load(directory / 'summary.json')
    require(completed.get('complete') is True and completed.get('accepted_performance') is True,
            'The complete paired run is required before stage analysis')
    comparison = load(comparison_path)
    require(comparison.get('complete') is True
            and comparison['frozen_command'] == load(directory / 'command.json'),
            'An independently checked comparison for this exact run is required')
    rows, receipts = [], {}
    for label in ORDER:
        path = directory / label / 'receipts.jsonl'
        receipts[label] = identity(path)
        with path.open() as source:
            for line in source:
                row = json.loads(line)
                if row.get('operation') == 'measured-write':
                    rows.append(request_timing(row, label))
    require(len(rows) == 768, 'The paired receipts lack their 768 HTTP response clocks')
    cells = per_cell(rows, comparison)
    table = []
    for geometry in ('hash', 'hashrange'):
        for batch in (1, 32, 128):
            selected = [row for row in rows if row['geometry'] == geometry and row['batch'] == batch]
            table.append({'geometry': geometry, 'batch': batch,
                **{variant: summarize([row for row in selected if row['variant'] == variant])
                   for variant in ('before', 'after')}})
    return {'complete': True, 'performance_scope': comparison['performance_scope'],
        'quiet_host_claim': comparison['quiet_host_claim'],
        'analysis_identity': identity(Path(__file__)), 'comparison_identity': identity(comparison_path),
        'receipt_identities': receipts,
        'source_binary_identities': comparison['frozen_command']['versions'],
        'table': table, 'cells': cells,
        'by_source': {variant: summarize([row for row in rows if row['variant'] == variant])
                      for variant in ('before', 'after')},
        'per_run': {label: summarize([row for row in rows if row['label'] == label]) for label in ORDER},
        'definitions': {
            'prepare': 'Elapsed schema delta preparation plus related molecule rebase/fold; includes awaited work, not CPU alone.',
            'ack': 'Inner create/update pipeline interval from start_time to build_commit_receipt; not HTTP acknowledgement time.',
            'gate_wait': 'The current native receipt explicitly sets this field to zero; no actual gate-wait measurement.',
            'publish': 'The current native receipt has no recording site for this field; zero does not prove no publication cost.',
            'http_minus_inner_receipt_us': 'Signed HTTP duration minus the inner receipt interval. It includes work outside that interval, not a measured fsync or transport phase.',
            'inner_receipt_minus_prepare_us': 'Signed inner receipt interval minus preparation. It includes other work inside that interval, not a measured disk, CPU or gate phase.',
            'percentiles': 'Nearest rank p95 over original samples; p50 is the median. Units are microseconds.'},
        'receipt_clock_scope': {
            'current_mini_create_update': 'commit_native_request awaits its owning transaction::request_scope, which waits for the queued disk sequence before build_commit_receipt stamps ack. The inspected Mini routes add no enclosing native transaction.',
            'inherited_native_scope': 'request_scope returns future.await when PendingBatch::current exists. An inner receipt can then precede an outer scope durable wait; the ack field has no universal disk-completion boundary.',
            'mixed_and_pure_erasure': 'Mixed requests retain the create/update clock before later erasure phases. Pure erasure receipts use default stage clocks. These forms are outside this paired create/update matrix.',
            'outside_the_clock': 'Earlier route and admission work, later capture/attribution work, receipt logs, response construction and transport can fall outside the inner receipt interval.',
            'disk_proof': 'The whole HTTP reply waits for the required local commit. Neither the inner clock nor HTTP-minus-inner time isolates physical file barrier cost.'},
        'limitations': [
            'Receipt clocks overlap; do not sum prepare, ack, gate_wait and publish into an HTTP-time partition.',
            'Derived differences are arithmetic residuals, not direct measurements of disk, CPU or gate waits.',
            'File-barrier durations cover status endpoints, can overlap threads and include later baseline notices; do not subtract them from request latency.',
            'Concurrent host jobs prevent isolated or causal speed claims.',
            'The driver did not capture detailed request_ops phase timings; absent phases remain unavailable, not zero.'],
        'additional_phase_fields_for_a_separate_operational_observation': [
            'queue_wait_us', 'admission_wait_us', 'parse_us', 'schema_resolve_us', 'validate_us',
            'cas_precondition_us', 'schema_load_us', 'restore_molecules_us', 'idempotency_check_us',
            'change_notice_prepare_us', 'sync_capture_us', 'flush_us', 'apply_us', 'response_envelope_us']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, default=HERE / 'paired-r1')
    parser.add_argument('--comparison', type=Path, default=HERE / 'comparison.json')
    parser.add_argument('--output', type=Path, default=HERE / 'stages.json')
    args = parser.parse_args()
    result = analyze(args.run.resolve(), args.comparison.resolve())
    with args.output.open('x') as output:
        json.dump(result, output, sort_keys=True, indent=2)
        output.write('\n')
    print(json.dumps({'complete': True, 'output': str(args.output), 'cells': len(result['cells']),
                      'performance_scope': result['performance_scope']}))


if __name__ == '__main__':
    main()
