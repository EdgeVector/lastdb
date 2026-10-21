#!/usr/bin/env python3
"""Analyze completed ordinary API profile receipts without another product run."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

HERE = Path(__file__).resolve().parent
RUN = HERE / 'profile-r2'
BARRIER_SCOPE = 'process_lifetime_all_laststore_homes'
ANSI = re.compile(r'\x1b\[[0-9;]*m')


def require(value, message):
    if not value:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def identity(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            digest.update(chunk)
    return {'path': str(path.resolve()), 'bytes': path.stat().st_size,
            'sha256': digest.hexdigest()}


def distribution(values):
    values = sorted(values)
    require(bool(values), 'A required measurement is absent')
    return {'samples': len(values), 'mean': statistics.mean(values),
            'p50': statistics.median(values),
            'p95': values[math.ceil(.95 * len(values)) - 1],
            'min': values[0], 'max': values[-1], 'sum': sum(values)}


def by_id(rows, operation):
    selected = [row for row in rows if row.get('operation') == operation]
    result = {row['request_id']: row for row in selected}
    require(len(result) == len(selected), f'Duplicate {operation} request IDs')
    return result


def native_delta(row):
    before, after = row['before'], row['after']
    require(before['file_barriers']['scope'] == after['file_barriers']['scope'] == BARRIER_SCOPE,
            'Barrier scopes differ')
    require(before['file_barriers'].keys() == after['file_barriers'].keys(),
            'Barrier fields differ')
    barriers = {name: after['file_barriers'][name] - value
                for name, value in before['file_barriers'].items() if name != 'scope'}
    sequences = {name: after[name] - before[name]
                 for name in ('accepted_sequence', 'completed_sequence')}
    require(all(type(value) is int and value >= 0
                for value in [*barriers.values(), *sequences.values()]), 'A counter regresses')
    for point in (before, after):
        require(point['accepted_sequence'] == point['completed_sequence']
                and point['accepted_batches'] == 0 and point['accepted_batch'] is False
                and point['writer_failed'] is False, 'A write leaves uncompleted work')
    return barriers, sequences


def gate_rows(log):
    found = []
    for number, line in enumerate(log.read_text().splitlines(), 1):
        text = ANSI.sub('', line)
        if 'native write gate released' in text:
            fields = {key: int(value) for key, value in re.findall(r'\b([a-z_]+)=(\d+)', text)}
            require({'sequence', 'hold_us', 'operations', 'tip_puts'} <= fields.keys(),
                    'A gate row lacks required counters')
            found.append(dict(fields, line=number, timestamp=text.split()[0], text=text))
    require(bool(found), 'No gate DEBUG evidence exists')
    return found


def phase_samples(rows, requested):
    found, observations = {}, []
    for row in rows:
        if row.get('operation') != 'fresh-request-phase-snapshot':
            continue
        wanted = set(row['requested_ids'])
        require(row['all_requested_phases_present'] is True
                and set(row['matched_ids']) == wanted, 'A phase snapshot is incomplete')
        matches = [sample for sample in row['request_ops_snapshot']['recent']
                   if sample.get('request_id') in wanted]
        require(len(matches) == len(wanted), 'Duplicate or absent phase samples')
        for sample in matches:
            key = sample['request_id']
            require(key in requested and key not in found, 'A phase request repeats or differs')
            require(isinstance(sample.get('phases'), dict) and bool(sample['phases'])
                    and sample['status'] == 200 and sample['resident_commits'] == 1,
                    'A request sample lacks a successful complete write')
            require(all(type(value) is int and value >= 0 for value in sample['phases'].values()),
                    'A phase has invalid units or sign')
            found[key] = sample
        observations.append({'requested_ids': sorted(wanted),
                             'status_sampled_at': row['status_sampled_at'],
                             'observations': row['observations']})
    require(set(found) == requested, 'Not every HTTP call has a phase sample')
    return found, observations


def aggregate(requests):
    phases = sorted({name for row in requests for name in row['phase_ms']})
    barriers = sorted(requests[0]['barrier_delta'])
    return {'http_calls': len(requests), 'logical_changes': sum(row['batch'] for row in requests),
        'http_ms': distribution([row['http_ms'] for row in requests]),
        'ack_ms': distribution([row['stages_ms']['ack'] for row in requests]),
        'prepare_ms': distribution([row['stages_ms']['prepare'] for row in requests]),
        'node_cpu_ms': distribution([row['cpu_ms']['total'] for row in requests]),
        'data_gate_hold_ms': distribution([row['data_gate']['hold_us'] / 1000 for row in requests]),
        'phase_ms': {name: {'observed_samples': sum(name in row['phase_ms'] for row in requests),
                           'total_samples': len(requests),
                           'observed': distribution([row['phase_ms'][name] for row in requests
                                                     if name in row['phase_ms']])}
                     for name in phases},
        'process_barriers': {'scope': BARRIER_SCOPE,
            'total': {name: sum(row['barrier_delta'][name] for row in requests) for name in barriers},
            'mean_per_call': {name: statistics.mean(row['barrier_delta'][name] for row in requests)
                              for name in barriers}},
        'native_sequence_delta': dict(Counter(str(row['sequence_delta']['accepted_sequence'])
                                              for row in requests)),
        'data_gate_operation_counts': dict(Counter(str(row['data_gate']['operations']) for row in requests)),
        'request_ids': [row['request_id'] for row in requests]}


def inspect():
    paths = {name: RUN / name for name in ('command.json', 'result.json', 'run/receipts.jsonl',
                                          'run/summary.json', 'run/inputs.json', 'run/retained-root.json')}
    command, result, summary = (load(paths[name]) for name in ('command.json', 'result.json', 'run/summary.json'))
    require(result['complete'] is True and result['error'] is None and summary['complete'] is True
            and summary['error'] is None and result['all_request_phase_snapshots_complete'] is True
            and result['normal_reopen_values_and_notices_match'] is True, 'The profile is incomplete')
    binary, source = command['binary'], command['source']
    require(identity(Path(binary['path'])) == binary, 'The release executable differs')
    require(identity(Path(source['path'])) == {key: source[key] for key in ('path', 'bytes', 'sha256')},
            'The source receipt differs')
    manifest = load(Path(source['path']))
    require(manifest['state'] == 'release_verified' and manifest['binary_sha256'] == binary['sha256']
            and manifest['source_inventory_sha256'] == source['source_inventory_sha256']
            and manifest['final_verification']['passed'] is True, 'The release binding is incomplete')
    for helper in command['helpers'].values():
        require(identity(Path(helper['path'])) == {key: helper[key] for key in ('path', 'bytes', 'sha256')},
                'A frozen profile helper differs')
    timebase = command['mach_timebase_info']
    require(all(type(timebase[name]) is int and timebase[name] > 0 for name in ('numer', 'denom')),
            'The Mach timebase is invalid')
    rows = [json.loads(line) for line in paths['run/receipts.jsonl'].read_text().splitlines()]
    require(len(rows) == summary['receipts'], 'The receipt count differs')
    http, native = by_id(rows, 'profiled-write-http'), by_id(rows, 'profiled-write-native')
    require(set(http) == set(native), 'HTTP and native observations differ')
    phases, observations = phase_samples(rows, set(http))
    starts = [row for row in rows if row.get('operation') == 'owned-node-start']
    stops = [row for row in rows if row.get('operation') == 'owned-node-stop']
    require(len(starts) == len(stops) == 2 and {row['pid'] for row in starts} == {row['pid'] for row in stops}
            and all(row['forced_kill'] is False and row['exit_code'] == 0 for row in stops),
            'The node lacks two ordinary stop receipts')
    require(all(row['cloud_activation'] is False and all(value is None for value in
                row['engine_selection_environment'].values()) for row in starts),
            'The run changes cloud or engine selection')
    retained = load(paths['run/retained-root.json'])
    require(result['root'] == summary['root'] == retained['root'] and retained['all_owned_nodes_stopped'] is True,
            'The retained home owner differs')
    log = Path(starts[0]['home']).parent / 'node.log'
    require(log.is_relative_to(Path(result['root'])), 'The log escapes its owned synthetic root')
    paths['owned_node_log'] = log
    gates = gate_rows(log)
    filters = [row for row in rows if row.get('operation') == 'profile-log-filter-after']
    require(len(filters) == 1 and filters[0]['log_filter']['log_filter']['installed'] is True
            and filters[0]['log_filter']['log_filter']['directive'] == command['runtime_log_filter'],
            'The gate log filter was not installed')
    requests, mutation_ids = [], set()
    for key, row in sorted(http.items(), key=lambda pair: pair[1]['start_ns']):
        require(row['http_status'] == 200 and row['finish_ns'] - row['start_ns'] == row['elapsed_ns'] > 0,
                'A request duration or HTTP status differs')
        response, phase = row['result'], phases[key]
        ids = response['mutation_ids']
        require(response['count'] == row['batch'] == len(ids) == phase['resident_operations']
                and response['durability'] == 'durable' and len(set(ids)) == len(ids)
                and not mutation_ids.intersection(ids), 'A write lacks its complete durable result')
        mutation_ids.update(ids)
        before, after = row['owned_node_usage_before'], row['owned_node_usage_after']
        require(before['birth'] == after['birth'], 'The CPU counter crosses a process identity')
        cpu = {name: after[name] - before[name] for name in ('user', 'system', 'reads', 'writes')}
        require(all(type(value) is int and value >= 0 for value in cpu.values()), 'A resource counter regresses')
        cpu_ms = {name: cpu[name] * timebase['numer'] / timebase['denom'] / 1e6
                  for name in ('user', 'system')}
        cpu_ms['total'] = cpu_ms['user'] + cpu_ms['system']
        barriers, sequences = native_delta(native[key])
        low, high = (native[key][point]['accepted_sequence'] for point in ('before', 'after'))
        bounded = [gate for gate in gates if low < gate['sequence'] <= high]
        matching = [gate for gate in bounded if gate['tip_puts'] == len(command['fields']) * row['batch']]
        require(len(matching) == 1 and matching[0]['sequence'] == high,
                'The native sequence range lacks one final data gate')
        requests.append({'request_id': key, 'geometry': row['geometry'], 'batch': row['batch'],
            'action': row['action'], 'schema': phase['schema'], 'start_ns': row['start_ns'],
            'finish_ns': row['finish_ns'], 'http_ms': row['elapsed_ns'] / 1e6,
            'stages_ms': {name: value / 1000 for name, value in response['resident_stages_us'].items()},
            'phase_ms': {name: value / 1000 for name, value in phase['phases'].items()},
            'cpu_raw_delta': cpu, 'cpu_ms': cpu_ms, 'barrier_delta': barriers,
            'sequence_before': low, 'sequence_after': high, 'sequence_delta': sequences,
            'data_gate': matching[0], 'other_gate_rows': [gate for gate in bounded if gate is not matching[0]]})
    samples = command['samples_per_verb_geometry_batch']
    counts = Counter((row['geometry'], row['batch'], row['action']) for row in requests)
    require(set(counts) == {(geometry, batch, action) for geometry in ('hash', 'hashrange')
                           for batch in (1, 32, 128) for action in ('create', 'update')}
            and set(counts.values()) == {samples}, 'The request matrix is incomplete')
    require(len(requests) == 12 * samples and len(mutation_ids) == result['accepted_mutations']
            == sum(row['batch'] for row in requests), 'The accepted change count differs')
    require([row['request_id'] for row in result['writes']] == [row['request_id'] for row in requests],
            'The final result loses or reorders writes')
    require(all(left['finish_ns'] <= right['start_ns'] for left, right in zip(requests, requests[1:])),
            'The single-caller requests overlap')
    checks = [row for row in rows if row.get('operation') == 'ordinary-notices-immediately-after-ack']
    require(len(checks) == 12 and all(row['all_notice_ids_coordinates_and_flags_match'] is True for row in checks)
            and {key for row in checks for key in row['required_ids']} == mutation_ids
            and sum(len(row['required_ids']) for row in checks) == len(mutation_ids),
            'The immediate notice checks are incomplete')
    reopen = [row for row in rows if row.get('operation') == 'ordinary-reopen-complete']
    reads = [row for row in rows if row.get('operation') == 'core-owned-read']
    require(len(reopen) == 1 and reopen[0]['all_values_and_notices_match'] is True
            and set(reopen[0]['required_ids']) == mutation_ids
            and all(row['all_fields_and_membership_match'] is True for row in reads),
            'The values or reopened notices lack a complete driver check')
    reopened_reads = [row for row in reads if row['phase'] == 'all-values-after-normal-reopen']
    require({row['geometry'] for row in reopened_reads} == {'hash', 'hashrange'}, 'A key type lacks reopen reads')
    groups, cells = [], []
    for geometry in ('hash', 'hashrange'):
        for batch in (1, 32, 128):
            selected = [row for row in requests if row['geometry'] == geometry and row['batch'] == batch]
            groups.append(dict(geometry=geometry, batch=batch, **aggregate(selected)))
            for action in ('create', 'update'):
                cells.append(dict(geometry=geometry, batch=batch, action=action,
                                  **aggregate([row for row in selected if row['action'] == action])))
    return {'complete': True, 'binary': binary, 'source': source, 'mach_timebase_info': timebase,
        'scope': command['scope'], 'matrix': {'http_calls': len(requests), 'changes': len(mutation_ids),
            'fields_per_record': len(command['fields']), 'body_bytes': command['body_bytes'],
            'samples_per_verb_geometry_batch': samples},
        'units_and_limits': {'http': 'Prepared JSON send through complete HTTP response bytes, milliseconds.',
            'cpu': command['cpu_scope'],
            'phases': 'Only observed phase fields enter each mean. Absent fields stay unreported. Apply is a residual, not a disk timer.',
            'barriers': 'All process LastStore work between status observations; attempts may overlap and include unrelated node tasks. Not a latency partition.',
            'gate': 'Sequence-bound final data preparation/acceptance hold. Excludes durable wait after gate release; no direct HTTP request ID exists in the log.',
            'percentiles': 'Nearest-rank p95; only six pooled or three per-action observations. The p95 is the observed maximum here.',
            'rates': 'No throughput rate is derived across diagnostic status, validation, or sampler pauses. Original ABBA receipts remain the rate source.',
            'comparison': 'Candidate-only small fresh database; no before/after gain, isolated-host, or hardware capacity claim.'},
        'validation': {'driver_completed': result['complete'], 'phase_snapshots': len(observations),
            'all_http_ids_have_exact_phase_and_native_samples': True,
            'all_http_ids_have_one_sequence_bound_data_gate': True,
            'immediate_notice_check_count': len(checks), 'normal_stop_receipts': stops,
            'normal_reopen_values_and_notices_driver_check': reopen[0]['all_values_and_notices_match'],
            'value_read_checks': len(reads), 'reopened_value_read_checks': len(reopened_reads),
            'raw_query_response_limit': 'Compact receipts keep response hashes and driver value checks; this analysis does not independently repeat the API calls.'},
        'evidence': {name: identity(path) for name, path in paths.items()},
        'analysis_helper': identity(Path(__file__)), 'request_phase_observations': observations,
        'groups': groups, 'cells': cells, 'requests': requests}


def markdown(data):
    lines = ['The current release uses a small fresh database. The run completes 36 calls and 1,932 changes.',
        'Each result combines three create calls and three update calls. Each record has four fields and a 1 KiB body.',
        'The host also runs other work. These results do not show a before/after gain or a machine limit.', '',
        '| Key | Records/call | Mean ms | p50 ms | p95 ms | Node CPU ms | Data gate ms |',
        '|---|---:|---:|---:|---:|---:|---:|']
    for row in data['groups']:
        lines.append('| {} | {} | {:.1f} | {:.1f} | {:.1f} | {:.1f} | {:.1f} |'.format(
            row['geometry'], row['batch'], row['http_ms']['mean'], row['http_ms']['p50'],
            row['http_ms']['p95'], row['node_cpu_ms']['mean'], row['data_gate_hold_ms']['mean']))
    lines += ['', 'The p95 is the largest observed call in each six-call result. The sample is small.',
        'CPU time includes all node threads and the sampler. The CPU interval extends slightly beyond the HTTP interval.',
        'The saved Mach timebase is {numer}/{denom} nanoseconds per raw tick.'.format(**data['mach_timebase_info']), '',
        '| Key | Records/call | Apply residual ms | Restore molecules ms | Schema load ms | Notice prepare ms |',
        '|---|---:|---:|---:|---:|---:|']
    for row in data['groups']:
        phase = row['phase_ms']
        def mean(name):
            return '{:.2f}'.format(phase[name]['observed']['mean']) if name in phase else 'unreported'
        lines.append('| {} | {} | {} | {} | {} | {} |'.format(row['geometry'], row['batch'],
            mean('apply_us'), mean('restore_molecules_us'), mean('schema_load_us'), mean('change_notice_prepare_us')))
    lines += ['', 'Apply is the remaining pipeline time after the named measurements. It includes unmeasured work and waits.',
        'Apply does not measure disk sync time. An absent phase stays unreported in the JSON result.',
        'The data gate covers private preparation and acceptance. The durable wait follows the gate release.', '',
        '| Key | Records/call | File barriers/call | Directory barriers/call | File barrier ms/call | Directory barrier ms/call |',
        '|---|---:|---:|---:|---:|---:|']
    for row in data['groups']:
        values = row['process_barriers']['mean_per_call']
        lines.append('| {} | {} | {:.1f} | {:.1f} | {:.1f} | {:.1f} |'.format(
            row['geometry'], row['batch'], values['data_file_calls'], values['directory_calls'],
            values['data_file_micros'] / 1000, values['directory_micros'] / 1000))
    lines += ['', 'These disk counters cover the node process between two status calls. Their intervals exceed the HTTP interval.',
        'Barrier attempts can overlap. Their summed times do not divide the request time into separate parts.',
        'Each request matches one final data gate through the native sequence range and its field count.',
        'A 128-record create carries 1,542 stored operations. A 128-record update carries 1,797 stored operations.',
        'The records, fields, counters, and change notices share that data commit. Some calls also carry a separate clock commit.', '',
        'The driver checks all values and notices after a normal reopen. Both node sessions stop without a forced kill.',
        'This analysis checks the complete receipt matrix, exact source and binary hashes, helper hashes, and phase coverage.',
        'The compact receipts retain the driver checks and response hashes. This analysis does not repeat the API calls.',
        'Use the original ABBA results for calls per second. This diagnostic includes status and validation pauses.', '',
        'The next profile should split the Apply residual into the clock wait, data commit, and post-commit work.',
        'Keep the disk-before-ack rule. Do not choose a code change from the Apply residual alone.', '',
        'Binary SHA-256: `{}`.'.format(data['binary']['sha256']),
        'Source receipt SHA-256: `{}`.'.format(data['source']['sha256']),
        'Receipt SHA-256: `{}`.'.format(data['evidence']['run/receipts.jsonl']['sha256']),
        'Full inputs, clock units, per-call results, and evidence hashes: [profile_result.json](profile_result.json).']
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Recompute and compare; do not change files')
    args = parser.parse_args()
    data = inspect()
    result, report = HERE / 'profile_result.json', HERE / 'profile_report.md'
    if args.check:
        require(load(result) == data and report.read_text() == markdown(data), 'A saved result differs')
    else:
        with result.open('x') as stream:
            json.dump(data, stream, sort_keys=True, indent=2)
            stream.write('\n')
        with report.open('x') as stream:
            stream.write(markdown(data))
    print(json.dumps({'complete': data['complete'], 'http_calls': data['matrix']['http_calls'],
                      'changes': data['matrix']['changes'], 'receipt_sha256': data['evidence']['run/receipts.jsonl']['sha256']}))


if __name__ == '__main__':
    main()
