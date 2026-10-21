#!/usr/bin/env python3
"""Recompute batch request rates and clock scopes from completed original receipts."""
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics

HERE = Path(__file__).resolve().parent
PROOF = HERE.parent / 'atomic-change-feed-20261021'
ORDER = ('before-r1', 'after-r1', 'after-r2', 'before-r2')
BARRIERS = ('data_file_calls', 'data_file_micros', 'sidecar_file_calls',
            'sidecar_file_micros', 'directory_calls', 'directory_micros')
COUNTERS = ('accepted_sequence', 'completed_sequence')
SCOPE = 'process_lifetime_all_laststore_homes'


def require(value, message):
    if not value:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def identity(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            value.update(block)
    return {'path': str(path), 'sha256': value.hexdigest(), 'bytes': path.stat().st_size}


def distribution(values):
    values = sorted(values)
    return {'samples': len(values), 'sum': sum(values), 'mean': statistics.mean(values),
            'p50': statistics.median(values), 'p95': values[math.ceil(.95 * len(values)) - 1],
            'min': values[0], 'max': values[-1]}


def coordinates(row):
    return tuple(row[name] for name in ('geometry', 'batch', 'revision', 'first'))


def delta(before, after):
    require(before['file_barriers']['scope'] == after['file_barriers']['scope'] == SCOPE,
            'Counter scopes differ')
    values = {name: after[name] - before[name] for name in COUNTERS}
    values.update({name: after['file_barriers'][name] - before['file_barriers'][name]
                   for name in BARRIERS})
    require(all(type(value) is int and value >= 0 for value in values.values()),
            'Counter values regress or change type')
    return values


def inspect_cell(label, cell, requests, compared, staged):
    requests = sorted(requests, key=lambda row: row['start_ns'])
    require(len(requests) == cell['requests'] == 16, 'Missing original requests')
    durations = [row['elapsed_ns'] for row in requests]
    require(all(row['finish_ns'] - row['start_ns'] == row['elapsed_ns'] > 0
                and row['http_status'] == 200 and row['result']['durability'] == 'durable'
                and row['result']['count'] == cell['batch'] for row in requests),
            'A measured response or duration differs')
    require(all(a['finish_ns'] <= b['start_ns'] for a, b in zip(requests, requests[1:])),
            'One-writer requests overlap')
    wall = requests[-1]['finish_ns'] - requests[0]['start_ns']
    gaps = sum(b['start_ns'] - a['finish_ns'] for a, b in zip(requests, requests[1:]))
    require(wall == sum(durations) + gaps == cell['wall_ns'] == compared['wall_ns'],
            'Wall time does not match exact request and gap boundaries')
    require(durations == compared['request_elapsed_ns'], 'Comparison request samples differ')
    http = distribution([value / 1e6 for value in durations])
    require(math.isclose(http['p50'], compared['request_latency_ms']['p50_ms'], rel_tol=1e-12)
            and math.isclose(http['p95'], compared['request_latency_ms']['p95_ms'], rel_tol=1e-12),
            'Comparison percentiles differ')
    stages = {name: [row['result']['resident_stages_us'][name] / 1000 for row in requests]
              for name in ('prepare', 'ack', 'gate_wait', 'publish')}
    require(all(type(value) in (int, float) and value >= 0
                for values in stages.values() for value in values), 'Invalid pipeline clock')
    for name, values in stages.items():
        require(math.isclose(sum(values) * 1000,
                            staged['resident_stages_us'][name]['sum_us'], abs_tol=1e-5),
                'Stage analysis differs from original responses')
    counters = {name: delta(cell['before'], endpoint) for name, endpoint in (
        ('post_http_status', cell['after']),
        ('through_notice_drain', cell['notices']['after_notice_drain']))}
    for endpoint, values in counters.items():
        expected = compared[endpoint]
        require(all(values[name] == expected[name] for name in COUNTERS)
                and all(values[name] == expected['file_barriers'][name] for name in BARRIERS),
                'Comparison status deltas differ')
    return {'label': label, 'variant': label.split('-')[0],
        **{name: cell[name] for name in ('geometry', 'batch', 'revision', 'first')},
        'calls': 16, 'records': 16 * cell['batch'], 'wall_ns': wall,
        'sum_http_ns': sum(durations), 'intercall_gap_ns': gaps,
        'http_ms_samples': [value / 1e6 for value in durations], 'stage_ms_samples': stages,
        'counters': counters, 'first_send_ns': requests[0]['start_ns'],
        'last_reply_ns': requests[-1]['finish_ns']}


def counter_summary(cells, endpoint, calls):
    total = {name: sum(cell['counters'][endpoint][name] for cell in cells)
             for name in (*COUNTERS, *BARRIERS)}
    return {'scope': SCOPE, 'total': total,
            'per_http_call': {name: value / calls for name, value in total.items()},
            'barrier_elapsed_ms_per_http_call': {
                kind: total[kind + '_micros'] / 1000 / calls
                for kind in ('data_file', 'sidecar_file', 'directory')},
            'mean_elapsed_ms_per_completed_barrier_attempt': {
                kind: total[kind + '_micros'] / 1000 / total[kind + '_calls']
                if total[kind + '_calls'] else None
                for kind in ('data_file', 'sidecar_file', 'directory')}}


def aggregate(cells):
    calls, records = sum(cell['calls'] for cell in cells), sum(cell['records'] for cell in cells)
    wall = sum(cell['wall_ns'] for cell in cells)
    http = sum(cell['sum_http_ns'] for cell in cells)
    gaps = sum(cell['intercall_gap_ns'] for cell in cells)
    clock = {name: distribution([value for cell in cells for value in cell['stage_ms_samples'][name]])
             for name in ('prepare', 'ack', 'gate_wait', 'publish')}
    samples = [value for cell in cells for value in cell['http_ms_samples']]
    residuals = {name: distribution([value for cell in cells for value in values(cell)])
        for name, values in {
            'http_minus_inner_ms': lambda cell: [a - b for a, b in zip(
                cell['http_ms_samples'], cell['stage_ms_samples']['ack'])],
            'inner_minus_prepare_ms': lambda cell: [a - b for a, b in zip(
                cell['stage_ms_samples']['ack'], cell['stage_ms_samples']['prepare'])]}.items()}
    return {'cells': len(cells), 'http_calls': calls, 'logical_records': records,
        'wall_seconds': wall / 1e9, 'sum_http_seconds': http / 1e9,
        'intercall_gap_ms': gaps / 1e6, 'intercall_gap_fraction': gaps / wall,
        'calls_per_second_wall': calls * 1e9 / wall,
        'calls_per_second_sum_http': calls * 1e9 / http,
        'records_per_second_wall': records * 1e9 / wall,
        'mean_ms_per_call_wall': wall / 1e6 / calls,
        'http_ms': distribution(samples), 'resident_stage_ms': clock,
        'arithmetic_differences_ms': residuals,
        'status_endpoints': {endpoint: counter_summary(cells, endpoint, calls)
                             for endpoint in ('post_http_status', 'through_notice_drain')}}


def inspect():
    comparison, stages = load(PROOF / 'comparison.json'), load(PROOF / 'stages.json')
    require(comparison['complete'] and stages['complete']
            and load(PROOF / 'paired-r1/summary.json')['complete'], 'Incomplete original run')
    require(comparison['performance_scope'] == stages['performance_scope'] == 'observed_shared_host'
            and comparison['quiet_host_claim'] is False, 'Unexpected host scope')
    command = load(PROOF / 'paired-r1/command.json')
    require(comparison['frozen_command'] == command, 'Frozen command differs')
    source_comparison = {(row['label'], coordinates(row)): row for row in comparison['cells']}
    source_stages = {(row['label'], coordinates(row)): row for row in stages['cells']}
    cells, evidence = [], {}
    for label in ORDER:
        directory = PROOF / 'paired-r1' / label
        path = directory / 'receipts.jsonl'
        evidence[label] = identity(path)
        require(evidence[label]['sha256'] == stages['receipt_identities'][label]['sha256'],
                'Original receipt identity differs from stage analysis')
        with path.open() as stream:
            rows = [json.loads(line) for line in stream]
        requests = [row for row in rows if row.get('operation') == 'measured-write']
        summaries = [row for row in rows if row.get('operation') == 'write-summary']
        require(len(requests) == 192 and len(summaries) == 12, 'Original matrix differs')
        for cell in summaries:
            key = (label, coordinates(cell))
            selected = [row for row in requests if coordinates(row) == coordinates(cell)]
            cells.append(inspect_cell(label, cell, selected, source_comparison[key], source_stages[key]))
    return comparison, stages, cells, evidence


def markdown(data):
    lines = ['The batch contains the stated record count. One HTTP call sends the whole batch.',
        'Each result combines 64 calls: two runs, two write types, and 16 calls per cell.',
        'Other disk jobs ran at the same time. These results do not show an isolated machine limit.', '',
        '| Key | Records/call | Version | Calls/s | Mean ms/call | p50 ms | p95 ms | Records/s |',
        '|---|---:|---|---:|---:|---:|---:|---:|']
    for row in data['groups']:
        for version in ('before', 'after'):
            value = row[version]
            lines.append(f'| {row["geometry"]} | {row["batch"]} | {version} | '
                f'{value["calls_per_second_wall"]:.3f} | {value["http_ms"]["mean"]:.2f} | '
                f'{value["http_ms"]["p50"]:.2f} | {value["http_ms"]["p95"]:.2f} | '
                f'{value["records_per_second_wall"]:.2f} |')
    lines.extend(['', 'The mean call time uses the sum of HTTP durations divided by the call count.',
        'The call rate uses the sum of cell durations. Each cell spans its first send through its last reply.',
        'The small difference consists of time between calls. Schema setup, read checks, and notice checks sit outside these cell durations.', '',
        '| Key | Records/call | Version | Prepare ms/call | Inner receipt ms/call | HTTP ms/call | Data barriers/call | Directory barriers/call |',
        '|---|---:|---|---:|---:|---:|---:|---:|'])
    for row in data['groups']:
        for version in ('before', 'after'):
            value = row[version]
            counter = value['status_endpoints']['post_http_status']['per_http_call']
            lines.append(f'| {row["geometry"]} | {row["batch"]} | {version} | '
                f'{value["resident_stage_ms"]["prepare"]["mean"]:.2f} | '
                f'{value["resident_stage_ms"]["ack"]["mean"]:.2f} | '
                f'{value["http_ms"]["mean"]:.2f} | {counter["data_file_calls"]:.2f} | '
                f'{counter["directory_calls"]:.2f} |')
    lines.extend(['', *data['limitations']])
    return '\n'.join(lines) + '\n'


def main():
    comparison, stages, cells, evidence = inspect()
    groups = []
    for geometry in ('hash', 'hashrange'):
        for batch in (1, 32, 128):
            group = {'geometry': geometry, 'batch': batch}
            for version in ('before', 'after'):
                selected = [cell for cell in cells if cell['geometry'] == geometry
                            and cell['batch'] == batch and cell['variant'] == version]
                value = aggregate(selected)
                expected = next(row for row in comparison['table'] if row['geometry'] == geometry
                                and row['batch'] == batch)[version]
                require(value['http_calls'] == 64 and value['logical_records'] == batch * 64
                        and math.isclose(value['records_per_second_wall'],
                                         expected['records_per_second'], rel_tol=1e-12),
                        'Grouped original record rate differs')
                value['by_write_type'] = {name: aggregate([cell for cell in selected if cell['revision'] == rev])
                                          for rev, name in ((0, 'create'), (1, 'update'))}
                group[version] = value
            groups.append(group)
    data = {'complete': True, 'computed_utc': datetime.now(timezone.utc).isoformat(),
        'performance_scope': 'observed_shared_host', 'quiet_claim': False,
        'evidence': {'comparison': identity(PROOF / 'comparison.json'),
                     'stages': identity(PROOF / 'stages.json'), 'receipts': evidence,
                     'command': identity(PROOF / 'paired-r1/command.json'),
                     'analysis': identity(Path(__file__).resolve())},
        'frozen_versions': comparison['frozen_command']['versions'], 'groups': groups,
        'by_source': {version: aggregate([cell for cell in cells if cell['variant'] == version])
                      for version in ('before', 'after')}, 'cells': cells,
        'definitions': {'percentiles': 'p50 median; p95 nearest rank over original HTTP calls.',
            'wall_seconds': 'Sum of four first-send to last-reply cell intervals per source and group.',
            'sum_http_seconds': 'Sum of original send to complete-response-byte HTTP durations.',
            'records': 'Logical create/update operations. Four fields and a 1 KiB body per record.',
            'counter_window': 'Before-cell status through post-HTTP status; also through notice drain.',
            'barrier_scope': SCOPE},
        'clock_definitions': stages['definitions'], 'receipt_clock_scope': stages['receipt_clock_scope'],
        'limitations': ['These are shared-host observations, not an M5 or hardware-limit forecast.',
            'The combined notice candidate is the frozen after-v2 binary; this matrix does not benchmark later main integration changes.',
            'Prepare is elapsed work with awaits, not CPU time. The inner receipt clock is not the HTTP reply clock.',
            'Prepare overlaps the inner receipt clock. Clock differences do not measure a disk or queue wait.',
            'Gate-wait and publish fields report unmeasured zeros; they do not prove zero work.',
            'Barrier counters cover all LastStore homes in the node process and status endpoints outside the timed HTTP interval.',
            'Barrier durations count completed attempts, include failures, and can overlap threads. They cannot form a call-latency partition.',
            'Baseline notices can add work after the HTTP reply. Both post-status and notice-drain windows remain in the JSON.',
            'The complete HTTP reply follows the required durable local commit. No crash, power-loss, cloud or hardware-cold proof is claimed.']}
    with (HERE / 'existing_result.json').open('x') as stream:
        json.dump(data, stream, sort_keys=True, indent=2)
        stream.write('\n')
    with (HERE / 'existing_report.md').open('x') as stream:
        stream.write(markdown(data))
    print(json.dumps({'groups': len(groups), 'calls': sum(cell['calls'] for cell in cells),
                      'complete': True, 'output': str(HERE / 'existing_result.json')}))


if __name__ == '__main__':
    main()
