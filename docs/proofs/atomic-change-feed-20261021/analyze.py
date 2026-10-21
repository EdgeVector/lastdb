#!/usr/bin/env python3
"""Independently recompute a completed ordinary-API ABBA comparison."""
import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics

HERE = Path(__file__).resolve().parent
ORDER = ['before-r1', 'after-r1', 'after-r2', 'before-r2']
BARRIERS = ['data_file_calls', 'data_file_micros', 'sidecar_file_calls',
            'sidecar_file_micros', 'directory_calls', 'directory_micros']
SEQUENCES = ['accepted_sequence', 'completed_sequence']
SCOPE = 'process_lifetime_all_laststore_homes'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def percentiles(values):
    values = sorted(values)
    require(bool(values), 'No request latency samples')
    return {'samples': len(values), 'p50_ms': statistics.median(values) / 1e6,
            'p95_ms': values[math.ceil(.95 * len(values)) - 1] / 1e6,
            'p95_method': 'nearest rank over original request elapsed_ns values'}


def key(row):
    return tuple(row[name] for name in ('geometry', 'batch', 'revision', 'first'))


def counter_delta(before, after):
    require(before['file_barriers']['scope'] == after['file_barriers']['scope'] == SCOPE,
            'Barrier scopes differ from the frozen process-wide scope')
    result = {}
    for name in SEQUENCES:
        require(type(before[name]) is int and type(after[name]) is int
                and after[name] >= before[name], 'A native sequence regresses')
        result[name] = after[name] - before[name]
    result['file_barriers'] = {'scope': SCOPE}
    for name in BARRIERS:
        left, right = before['file_barriers'][name], after['file_barriers'][name]
        require(type(left) is int and type(right) is int and right >= left,
                'A file-barrier counter regresses or changes type')
        result['file_barriers'][name] = right - left
    return result


def sum_counters(rows, name):
    result = {field: sum(row[name][field] for row in rows) for field in SEQUENCES}
    result['file_barriers'] = {'scope': SCOPE, **{field: sum(
        row[name]['file_barriers'][field] for row in rows) for field in BARRIERS}}
    requests = sum(row['requests'] for row in rows)
    result['per_http_request'] = {field: result[field] / requests for field in SEQUENCES}
    result['file_barriers_per_http_request'] = {field: result['file_barriers'][field] / requests
                                               for field in BARRIERS}
    return result


def clipped_host(run, start, end):
    evidence = run['quiet']
    overlaps = []
    for index, sample in enumerate(evidence.get('samples', [])):
        left, right = max(start, sample['start_ns']), min(end, sample['end_ns'])
        if right <= left:
            continue
        jobs = [{name: row.get(name) for name in ('pid', 'birth', 'uid', 'name', 'kind',
                'delta_bytes', 'errno')} for row in sample['processes']
                if not row['owned'] and (row['delta_bytes'] or row['kind'])]
        overlaps.append({'sample_index': index, 'overlap_start_ns': left,
            'overlap_end_ns': right, 'overlap_ns': right - left,
            'full_sample_ns': sample['end_ns'] - sample['start_ns'],
            'full_sample_foreign_delta_bytes': sample['foreign_delta_bytes'],
            'reasons': sample['reasons'], 'disk': sample['disk'], 'foreign_processes': jobs,
            'process_count': sample['process_count'], 'observable_count': sample['observable_count'],
            'errno_counts': sample['errno_counts']})
    return {'overlapping_samples': overlaps,
        'covered_ns': sum(row['overlap_ns'] for row in overlaps), 'write_wall_ns': end - start,
        'scope': 'Only sample intervals intersecting first HTTP send through final reply. '
                 'Process byte deltas and disk rates still describe their full sample intervals; '
                 'no fraction of their bytes is attributed to the clipped intersection.'}


def validate_request(row, cell, seen):
    result = row['result']
    ids = result.get('mutation_ids', [])
    require(row['http_status'] == 200 and row.get('decode_error') is None
        and not row.get('cell_transport_failed') and result.get('durability') == 'durable'
        and type(result.get('count')) is int and result['count'] == cell['batch'],
        'A measured HTTP response is not a complete durable acknowledgement')
    require(len(ids) == cell['batch'] and all(isinstance(value, str) and value for value in ids)
            and len(set(ids)) == len(ids) and not seen.intersection(ids),
            'Measured mutation identities are absent or repeated')
    require(row['elapsed_ns'] == row['finish_ns'] - row['start_ns'] and row['elapsed_ns'] > 0,
            'The recorded HTTP duration differs from its original boundaries')
    require(row['request'].get('mutations_count') == cell['batch'], 'Request count differs')
    seen.update(ids)


def analyze_cell(run, cell, requests, seen):
    require(len(requests) == cell['requests'] == 16, 'A cell lacks its 16 original HTTP calls')
    requests = sorted(requests, key=lambda row: row['start_ns'])
    for row in requests:
        validate_request(row, cell, seen)
    require(all(left['finish_ns'] <= right['start_ns'] for left, right in zip(requests, requests[1:])),
            'A one-writer cell has overlapping HTTP requests')
    start, end = requests[0]['start_ns'], requests[-1]['finish_ns']
    require(cell['quiet_certificate']['start_ns'] <= start < end
            <= cell['quiet_certificate']['end_ns'], 'Host and HTTP clock boundaries disagree')
    require(cell['wall_ns'] == end - start and cell['records'] == cell['batch'] * 16,
            'A cell wall duration or mutation count differs from its original requests')
    rate = cell['records'] * 1e9 / (end - start)
    latency = percentiles([row['elapsed_ns'] for row in requests])
    require(math.isclose(rate, cell['records_per_second'], rel_tol=1e-12)
            and math.isclose(latency['p50_ms'], cell['median_ms'], rel_tol=1e-12)
            and math.isclose(latency['p95_ms'], cell['p95_ms'], rel_tol=1e-12),
            'A recorded rate or percentile differs from original request boundaries')
    for native in (cell['before'], cell['after'], cell['notices']['after_notice_drain']):
        require(native['writer_failed'] is False
                and native['completed_sequence'] <= native['accepted_sequence'], 'Invalid native status')
    drained = cell['notices']['after_notice_drain']
    require(drained['accepted_sequence'] == drained['completed_sequence']
            and drained['accepted_batch'] is False, 'Notices leave outstanding native queued work')
    counters = {name: counter_delta(cell['before'], endpoint) for name, endpoint in (
        ('post_http_status', cell['after']), ('through_notice_drain', drained))}
    for name, field in [('post_http_status', 'barrier_delta_until_post_write_status'),
                        ('through_notice_drain', 'barrier_delta_until_notice_drain')]:
        require(counters[name]['file_barriers'] == cell[field], 'Recorded barrier deltas disagree')
    require(cell['notices']['all_notice_ids_and_coordinates_match'] is True
            and set(cell['notices']['required_ids']) == {
                identity for row in requests for identity in row['result']['mutation_ids']},
            'Incomplete notice proof')
    if run['label'].startswith('after'):
        require(cell['notices']['immediate_presence'] is True, 'Candidate notice needs a later pass')
    return {'label': run['label'], 'variant': run['label'].split('-')[0],
        **{name: cell[name] for name in ('geometry', 'batch', 'revision', 'first', 'writers',
                                       'body_bytes', 'requests', 'records', 'wall_ns')},
        'records_per_second': rate, 'request_latency_ms': latency,
        'request_elapsed_ns': [row['elapsed_ns'] for row in requests], **counters,
        'notices': {name: cell['notices'][name] for name in ('immediate_presence', 'passes',
            'observation_start_after_last_reply_ns', 'observed_complete_after_last_reply_ns')},
        'host_overlap': clipped_host(run, start, end), 'consistency_checks_pass': True}


def inspect_run(directory, label, frozen):
    run = load(directory / label / 'result.json')
    require(run['label'] == label and run['timing_complete'] and run['reopen_complete'],
            'A timing or normal-reopen run did not complete')
    receipts = [json.loads(line) for line in (directory / label / 'receipts.jsonl').read_text().splitlines()]
    recorded = [row for row in receipts if row.get('operation') == 'write-summary']
    requests = [row for row in receipts if row.get('operation') == 'measured-write']
    require(len(run['writes']) == len(recorded) == 12 and len(requests) == 192,
            'A run lacks its complete matrix or original request receipts')
    require(len({key(row) for row in run['writes']}) == 12, 'Repeated cell coordinates')
    seen, cells = set(), []
    for cell in run['writes']:
        copies = [row for row in recorded if key(row) == key(cell)]
        require(len(copies) == 1 and all(copies[0].get(name) == value for name, value in cell.items()),
                'Result and immutable receipt write summaries differ')
        selected = [row for row in requests if key(row) == key(cell)]
        cells.append(analyze_cell(run, cell, selected, seen))
    expected = {(geometry, batch, revision) for geometry in ('hash', 'hashrange')
                for batch in (1, 32, 128) for revision in (0, 1)}
    require({(row['geometry'], row['batch'], row['revision']) for row in cells} == expected,
            'The measured matrix differs')
    reopen = [row for row in receipts if row.get('operation') == 'all-notices-after-normal-reopen']
    require(len(reopen) == 1 and reopen[0].get('all_notice_ids_match') is True
            and set(reopen[0]['expected_ids']) == seen, 'Reopened notices differ from measured IDs')
    reads = [row for row in receipts if row.get('operation') == 'all-key-readback']
    require(len(reads) == 18 and all(row.get('all_fields_and_membership_match') is True
        and row['expected_fields'] == frozen['fields'] for row in reads), 'Incomplete value readback proof')
    starts = [row for row in receipts if row.get('operation') == 'owned-node-start']
    stops = [row for row in receipts if row.get('operation') == 'owned-node-stop']
    require(len(starts) == len(stops) == 2 and all(row.get('forced_kill') is False
        for row in stops), 'A run lacks its two normal node stops')
    summary = load(directory / label / 'summary.json')
    require(summary.get('complete') is True, 'The preserved run outcome is not complete')
    inputs = load(directory / label / 'inputs.json')
    version = frozen['versions'][label.split('-')[0]]
    require(inputs['binary_sha256'] == version['binary']['sha256']
            and inputs['source_identity'] == version['source'], 'Per-run binary/source identity differs')
    require(all(row['exit_code'] in (0, -15) for row in stops), 'A node stop has an unexpected exit code')
    return cells, {'label': label, 'timed_http_calls': len(requests), 'mutation_ids': len(seen),
        'all_key_readback_checks': len(reads), 'normal_node_sessions': len(stops),
        'node_exit_codes': [row['exit_code'] for row in stops],
        'host_samples': len(run['quiet'].get('samples', [])),
        'host_windows': len(run['quiet'].get('windows', []))}


def aggregate(rows):
    requests = sum(row['requests'] for row in rows)
    wall, records = sum(row['wall_ns'] for row in rows), sum(row['records'] for row in rows)
    return {'cells': len(rows), 'http_requests': requests, 'records': records, 'wall_ns': wall,
        'records_per_second': records * 1e9 / wall,
        'request_latency_ms': percentiles([value for row in rows for value in row['request_elapsed_ns']]),
        'cell_rate_range': [min(row['records_per_second'] for row in rows),
                            max(row['records_per_second'] for row in rows)],
        'post_http_status': sum_counters(rows, 'post_http_status'),
        'through_notice_drain': sum_counters(rows, 'through_notice_drain'),
        'first_pagination_pass_complete_cells': sum(row['notices']['immediate_presence'] for row in rows)}


def group_result(rows):
    result = aggregate(rows)
    result['per_run'] = {label: aggregate([row for row in rows if row['label'] == label])
                         for label in ORDER if any(row['label'] == label for row in rows)}
    result['by_revision'] = {str(revision): aggregate([row for row in rows if row['revision'] == revision])
                              for revision in (0, 1)}
    return result


def host_totals(cells):
    samples = {}
    for cell in cells:
        for row in cell['host_overlap']['overlapping_samples']:
            identity = (cell['label'], row['sample_index'])
            if identity not in samples:
                samples[identity] = dict(row, overlap_ns=0)
            samples[identity]['overlap_ns'] += row['overlap_ns']
    reasons = defaultdict(int)
    processes = {}
    for row in samples.values():
        for reason in row['reasons']:
            reasons[reason.split(':', 1)[0]] += 1
        for process in row['foreign_processes']:
            identity = (process['pid'], process['birth'])
            found = processes.setdefault(identity, dict(process, intersecting_samples=0,
                full_intersecting_interval_bytes=0))
            found['intersecting_samples'] += 1
            found['full_intersecting_interval_bytes'] += process['delta_bytes']
    return {'unique_intersecting_sample_intervals': len(samples),
        'covered_write_ns': sum(row['overlap_ns'] for row in samples.values()),
        'write_wall_ns': sum(row['wall_ns'] for row in cells),
        'reasons_by_intersecting_interval': dict(reasons),
        'full_intersecting_interval_foreign_bytes': sum(row['full_sample_foreign_delta_bytes']
                                                       for row in samples.values()),
        'foreign_processes': sorted(processes.values(),
            key=lambda row: row['full_intersecting_interval_bytes'], reverse=True),
        'byte_scope': 'Whole sampled intervals that overlap writes; bytes are not clipped or request-attributed.'}


def analyze(directory):
    # Read nothing from an incomplete run. In particular, do not inspect partial receipts.
    completed = load(directory / 'summary.json')
    require(completed.get('complete') is True and completed.get('accepted_performance') is True,
            'The full comparison is not complete and accepted under its declared recipe')
    frozen = load(directory / 'command.json')
    require(frozen['order'] == ORDER and frozen['requested_durability'] == 'durable'
            and len(frozen['fields']) == 4 and not frozen['uncontrolled'], 'Unexpected comparison recipe')
    require(completed['performance_scope'] == frozen['performance_scope'], 'Host scopes differ')
    require(completed['accepted_quiet_performance'] is (not frozen['allow_concurrent']),
            'The quiet-host claim differs from the declared observation mode')
    identities = dict(frozen['helpers'])
    identities.update({name: version['source'] for name, version in frozen['versions'].items()})
    for name, identity in identities.items():
        require(digest(Path(identity['path'])) == identity['sha256'], f'Frozen source/helper changes: {name}')
    cells, checks = [], []
    for label in ORDER:
        part, check = inspect_run(directory, label, frozen)
        cells.extend(part)
        checks.append(check)
    require(sum(row['requests'] for row in cells) == 768
            and sum(row['records'] for row in cells) == 41216, 'The complete sample totals differ')
    table = []
    for geometry in ('hash', 'hashrange'):
        for batch in (1, 32, 128):
            selected = [row for row in cells if row['geometry'] == geometry and row['batch'] == batch]
            values = {name: group_result([row for row in selected if row['variant'] == name])
                      for name in ('before', 'after')}
            require(all(value['http_requests'] == 64 for value in values.values()), 'A pooled cell lacks 64 calls')
            table.append({'geometry': geometry, 'batch': batch, 'writers': 1, 'body_bytes': 1024,
                **values, 'observed_after_vs_before_percent':
                100 * (values['after']['records_per_second'] / values['before']['records_per_second'] - 1)})
    return {'complete': True, 'performance_scope': frozen['performance_scope'],
        'quiet_host_claim': completed['accepted_quiet_performance'], 'frozen_command': frozen,
        'sample_totals': {'runs': len(checks), 'cells': len(cells), 'timed_http_calls': 768,
            'logical_mutations': 41216, 'readback_checks': sum(row['all_key_readback_checks'] for row in checks),
            'normal_node_sessions': sum(row['normal_node_sessions'] for row in checks)},
        'per_run_checks': checks, 'table': table,
        'by_source': {name: group_result([row for row in cells if row['variant'] == name])
                      for name in ('before', 'after')},
        'host_overlaps_by_source': {name: host_totals([row for row in cells if row['variant'] == name])
                                    for name in ('before', 'after')},
        'cells': cells, 'limitations': [
            'ABBA reduces order bias; concurrent host jobs prevent an isolated or causal speed claim.',
            'The rate is summed logical mutations divided by summed nonoverlapping HTTP wall windows.',
            'Barrier deltas cover before-status through post-HTTP status or notice drain, not exact request boundaries.',
            'Barrier counters include every LastStore home and background clock/metadata work in this node process.',
            'Barrier durations sum attempts, can overlap threads, and are not request latency; atomic samples are independent.',
            'Accepted/completed sequences count queued native requests, not every raw-store commit or file barrier.',
            'Baseline notice work can continue past the data ack; the notice-drain deltas include that later work.',
            'A first pagination pass can itself wait for disk/locks; its presence does not prove baseline notice durability at ack.',
            'Host overlap uses clipped time intervals; byte counts and device rates still describe each full sampled interval.',
            'EPERM system processes and short-lived jobs remain unobserved; no absolute zero-I/O claim.',
            'Compact receipts preserve original timings, IDs, hashes and executed check results; they do not reconstruct all values.',
            'Normal reopen is process-cold, OS cache uncontrolled; no crash, power-loss, cloud or M5 throughput proof.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, default=HERE / 'paired-r1')
    parser.add_argument('--output', type=Path, default=HERE / 'comparison.json')
    args = parser.parse_args()
    result = analyze(args.run.resolve())
    with args.output.open('x') as output:
        json.dump(result, output, sort_keys=True, indent=2)
        output.write('\n')
    print(json.dumps({'complete': True, 'scope': result['performance_scope'],
        'sample_totals': result['sample_totals'], 'rates': [{name: row[name] for name in (
            'geometry', 'batch', 'observed_after_vs_before_percent')} | {
            source: row[source]['records_per_second'] for source in ('before', 'after')}
            for row in result['table']]}))


if __name__ == '__main__':
    main()
