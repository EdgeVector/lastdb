#!/usr/bin/env python3
"""Measure ordinary queued Mini batches through an explicit durable flush."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
BASE = Path('/Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016')
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE / 'docs/proofs/schema-preload-product-20261017'))
import measure

measure.SCHEMA = 'PendingWriteCopyProduct'


class Run(measure.Run):
    def __init__(self, args):
        super().__init__(args)
        self.accepted_pairs = set()

    def live(self, h, n):
        return (h, n) in self.accepted_pairs


def memory_sample(node, phase):
    result = subprocess.run(['ps', '-o', 'rss=', '-p', str(node.process.pid)],
                            capture_output=True, text=True, check=True)
    sample = {'phase': phase, 'monotonic_ns': time.perf_counter_ns(),
              'process_rss_bytes': int(result.stdout.strip()) * 1024,
              'scope': 'One sparse process memory sample; this does not bound the memory peak.'}
    node.remember(operation='sparse-process-memory-observation', **sample)
    return sample


def setup(node):
    node.start()
    node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
        'source': 'system_seed', 'name': measure.SCHEMA, 'schema_type': 'HashRange',
        'descriptive_name': 'Pending Write Copy Product Benchmark',
        'key': {'hash_field': 'bucket', 'range_field': 'id'},
        'fields': ['bucket', 'id', 'title', 'body'],
        'field_descriptions': {'bucket': 'generated record hash', 'id': 'generated range',
                               'title': 'generated title', 'body': 'generated body'}}})
    node.flush()
    node.status('setup-complete')


def prepared_requests(run):
    pairs = [(h, n) for h in range(run.args.hashes) for n in range(run.args.records_per_hash)]
    requests, payload_bytes = [], 0
    for first in range(0, len(pairs), 64):
        mutations = []
        for h, n in pairs[first:first + 64]:
            fields = dict(run.values(h, n), bucket=measure.bucket(h), id=measure.key(n))
            payload_bytes += len(json.dumps(fields, separators=(',', ':'), ensure_ascii=False).encode())
            mutations.append({'type': 'mutation', 'schema': measure.SCHEMA,
                'mutation_type': 'create', 'durability': 'queued',
                'key_value': {'hash': measure.bucket(h), 'range': measure.key(n)},
                'fields_and_values': fields})
        requests.append({'mutations': mutations})
    return requests, payload_bytes


def write_batch(node, number, request):
    started = time.perf_counter_ns()
    result, request_id, error, elapsed = None, None, None, None
    try:
        result, elapsed, _, request_id = node.call('/api/mutations/batch', request)
        if result.get('count') != 64 or result.get('durability') not in ('queued', 'durable'):
            raise RuntimeError('A measured batch lacks its complete accepted response')
        with node.run.receipt_lock:
            for mutation in request['mutations']:
                key = mutation['key_value']
                node.run.accepted_pairs.add((int(key['hash'].removeprefix('generated-')),
                                            int(key['range'].split('#')[1])))
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
    finished = time.perf_counter_ns()
    row = {'batch_number': number, 'requested_records': 64, 'accepted_records': 64 if error is None else 0,
           'request_id': request_id, 'error': error, 'request_sha256': measure.profile.digest(request),
           'started_monotonic_ns': started, 'finished_monotonic_ns': finished,
           'ack_elapsed_nanos': elapsed, 'attempt_elapsed_nanos': finished - started,
           'returned_durability': None if result is None else result.get('durability'),
           'returned_mode_limit': 'The response mode alone does not establish the internal deferred path.'}
    node.remember(operation='measured-batch-ack', **row)
    return row


def observe_queue(node, done, observations, memory, failures):
    deadline = time.monotonic() + 120
    sampled_backlog = False
    try:
        while not done.is_set() and time.monotonic() < deadline:
            result, elapsed, _, request_id = node.call('/api/db/native-status')
            native = result.get('data', result)['native_disk']
            if native['writer_failed']:
                raise RuntimeError('The observed native writer failed')
            row = {'monotonic_ns': time.perf_counter_ns(), 'request_id': request_id,
                   'accepted_batches': native['accepted_batches'],
                   'accepted_sequence': native['accepted_sequence'],
                   'completed_sequence': native['completed_sequence'],
                   'memory_write_records': native['memory_write_records'],
                   'elapsed_nanos': elapsed}
            observations.append(row)
            node.remember(operation='accepted-prefix-observation', **row)
            if not sampled_backlog and row['accepted_batches'] > 1:
                memory.append(memory_sample(node, 'observed-multiple-accepted-batches'))
                sampled_backlog = True
            done.wait(.02)
    except Exception as exc:
        failures.append(f'{type(exc).__name__}: {exc}')


def readback(node):
    pairs = [(h, n) for h in range(node.run.args.hashes) for n in range(node.run.args.records_per_hash)]
    for first in range(0, len(pairs), 64):
        node.query(pairs[first:first + 64], 'exact', 'normal-reopen-exact', first // 64)
    for h in range(node.run.args.hashes):
        node.query([(h, n) for n in range(node.run.args.records_per_hash)],
                   'range', 'normal-reopen-fixed-hash-range', h)


def workload(node, shape):
    requests, payload_bytes = prepared_requests(node.run)
    setup(node)
    memory = [memory_sample(node, 'before-measured-work')]
    done, observations, failures = threading.Event(), [], []
    monitor = threading.Thread(target=observe_queue, args=(node, done, observations, memory, failures))
    monitor.start()
    acknowledgements = []
    try:
        if shape == 'serial':
            acknowledgements = [write_batch(node, number, request)
                                for number, request in enumerate(requests)]
        else:
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(write_batch, node, number, request)
                           for number, request in enumerate(requests)]
                acknowledgements = [future.result() for future in futures]
        flush_started = time.perf_counter_ns()
        node.flush()
        finished = time.perf_counter_ns()
    finally:
        done.set()
        monitor.join(timeout=10)
    if monitor.is_alive() or failures:
        raise RuntimeError('The ordinary queue observer failed: ' + repr(failures))
    memory.append(memory_sample(node, 'after-final-explicit-flush'))
    native = node.status('after-final-explicit-flush')
    if native['accepted_batches'] or native['memory_write_records']:
        raise RuntimeError('The final explicit flush retains a visible raw write graph')
    first = min(row['started_monotonic_ns'] for row in acknowledgements)
    wall = (finished - first) / 1e9
    acks = sorted(row['ack_elapsed_nanos'] / 1e6 for row in acknowledgements if row['error'] is None)
    accepted = sum(row['accepted_records'] for row in acknowledgements)
    accepted_payload = sum(len(json.dumps(mutation['fields_and_values'], separators=(',', ':'), ensure_ascii=False).encode())
                           for row in acknowledgements if row['error'] is None
                           for mutation in requests[row['batch_number']]['mutations'])
    refusal_rows = [row for row in node.run.rows if row.get('operation') == 'refusal'
                    and row.get('path') == '/api/mutations/batch']
    result = {'shape': shape, 'requested_records': 512, 'accepted_records': accepted, 'batch_records': 64,
              'concurrent_clients': 1 if shape == 'serial' else 4,
              'measurement_start_monotonic_ns': first, 'measurement_end_monotonic_ns': finished,
              'wall_seconds_first_request_through_final_flush': wall,
              'final_flush_seconds': (finished - flush_started) / 1e9,
              'logical_records_per_second': accepted / wall,
              'requested_serialized_four_field_payload_bytes': payload_bytes,
              'accepted_serialized_four_field_payload_bytes': accepted_payload,
              'serialized_four_field_payload_MB_per_second': accepted_payload / 1e6 / wall,
              'body_payload_bytes': accepted * 1024, 'body_payload_MB_per_second': accepted * 1024 / 1e6 / wall,
              'mutation_error_count': sum(row['error'] is not None for row in acknowledgements),
              'mutation_refusal_count': len(refusal_rows),
              'mutation_refusal_http_statuses': [row['http_status'] for row in refusal_rows],
              'ack_samples': len(acks), 'ack_p50_ms': statistics.median(acks) if acks else None,
              'ack_p95_nearest_rank_ms': acks[math.ceil(.95 * len(acks)) - 1] if acks else None,
              'acks_ms': acks, 'returned_modes': [row['returned_durability'] for row in acknowledgements],
              'max_observed_accepted_batches': max((row['accepted_batches'] for row in observations), default=0),
              'max_observed_raw_graph_records': max((row['memory_write_records'] for row in observations), default=0),
              'multiple_accepted_requests_observed': any(row['accepted_batches'] > 1 for row in observations),
              'optimization_path_observed': any(row['accepted_batches'] > 1 for row in observations),
              'optimization_coverage_status': 'observed-multiple-visible-accepted-requests'
                  if any(row['accepted_batches'] > 1 for row in observations) else 'not-observed',
              'queue_observation_count': len(observations), 'memory_samples': memory,
              'limits': ['The native status observer adds client and node work in both variants.',
                         'The memory samples do not bound the memory peak.',
                         'The filesystem cache is uncontrolled; each case uses a fresh synthetic home.',
                         'Payload MB uses decimal 1,000,000 bytes and counts serialized fields_and_values only.',
                         'The ack distribution has eight batch requests per case.']}
    measure.profile.save(node.run.receipt_dir / 'benchmark-timing.json', result)
    node.remember(operation='benchmark-timing-result', **result)
    node.stop()
    node.start()
    readback(node)
    memory.append(memory_sample(node, 'after-normal-reopen-readback'))
    node.status('normal-reopen-complete')
    result['normal_reopen_all_512_requested_keys_checked'] = True
    result['normal_reopen_values_and_accepted_membership_match'] = True
    node.remember(operation='benchmark-result', **result)
    return result


def preserve(run, own):
    sources = {'benchmark.py': Path(__file__), 'measure.py': Path(measure.__file__),
               'range_profile.py': measure.SHARED / 'range_profile.py',
               'ownership.py': measure.SHARED / 'ownership.py'}
    identity = {}
    for name, source in sources.items():
        shutil.copy2(source, run.receipt_dir / name)
        identity[name] = {'source': str(source), 'sha256': measure.profile.file_hash(source)}
    measure.profile.save(run.receipt_dir / 'benchmark-inputs.json', {
        'variant': own.label, 'round': own.round, 'shape': own.shape,
        'expected_binary_sha256': own.expected_sha256, 'driver_and_helper_inputs': identity,
        'environment': {'LASTDB_RESIDENT_MODE': 'write', 'LASTDB_RESIDENT_PERSIST_MS': '500',
                        'LASTDB_RESIDENT_MAX_DEFERRED_BYTES': '134217728',
                        'LASTDB_RESIDENT_KEY_CAP': '10000', 'LASTDB_SCHEMA_PRELOAD_SCHEMAS': ''},
        'records': 512, 'hashes': 2, 'records_per_hash': 256, 'body_bytes': 1024,
        'fields': ['bucket', 'id', 'title', 'body'], 'batch_records': 64,
        'load_start': os.getloadavg(), 'measurement_scope': 'First measured create request through the final explicit flush; setup and reopen readback are excluded.',
        'baseline_source_reference': str(HERE / 'before-source.json')
            if own.label == 'before' else 'The parent preserves the replacement build source identity.'})


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--shape', choices=['serial', 'concurrent4'], required=True)
    parser.add_argument('--label', choices=['before', 'after'], required=True)
    parser.add_argument('--round', type=int, choices=[1, 2], required=True)
    own, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    args = measure.arguments()
    if args.hashes != 2 or args.records_per_hash != 256 or args.body_bytes != 1024 or args.cap != 10000:
        raise RuntimeError('Use the reviewed 512-record, four-field configuration')
    if measure.profile.file_hash(args.binary) != own.expected_sha256:
        raise RuntimeError('The requested binary identity changed before private capture')
    run, node, complete, error, result = Run(args), None, False, None, None
    names = {'LASTDB_RESIDENT_KEY_CAP': '10000', 'LASTDB_RESIDENT_PERSIST_MS': '500',
             'LASTDB_RESIDENT_MAX_DEFERRED_BYTES': '134217728', 'LASTDB_SCHEMA_PRELOAD_SCHEMAS': ''}
    previous = {name: os.environ.get(name) for name in names}
    try:
        if measure.profile.file_hash(run.binary) != own.expected_sha256:
            raise RuntimeError('The private executable differs')
        preserve(run, own)
        os.environ.update(names)
        node = measure.Node(run)
        result = workload(node, own.shape)
        complete = result['mutation_error_count'] == 0
        if not complete:
            error = 'The real workload returned mutation errors or refusals; no retry or configuration change was attempted.'
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        run.remember({'operation': 'failure', 'error': error})
    finally:
        if node is not None:
            try:
                node.stop()
            except Exception as exc:
                complete, error = False, f'{error or ""}; stop: {exc}'
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        run.finish(complete, error)
        if node is not None and (node.directory / 'node.log').is_file():
            shutil.copy2(node.directory / 'node.log', run.receipt_dir / 'node.log')
        measure.profile.save(run.receipt_dir / 'benchmark-result.json', {
            'complete': complete, 'error': error, 'variant': own.label, 'round': own.round,
            'functional_complete': complete,
            'optimization_path_observed': False if result is None else result['optimization_path_observed'],
            'result': result, 'load_end': os.getloadavg()})
    print(json.dumps({'complete': complete, 'error': error, 'root': str(run.root),
                      'receipt_dir': str(run.receipt_dir), 'result': result}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
