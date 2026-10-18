#!/usr/bin/env python3
"""Measure ordinary Mini requests over a new generated HashRange schema."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid

sys.dont_write_bytecode = True
SHARED = Path(__file__).resolve().parent.parent / 'graph-first-20261017/drivers'
sys.path.insert(0, str(SHARED))
import ownership
import range_profile as profile

SCHEMA = 'SchemaPreloadProduct'
FIELDS = ['title', 'body']


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--variant', required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--root', type=Path)
    parser.add_argument('--records-per-hash', type=int, default=64)
    parser.add_argument('--hashes', type=int, default=4)
    parser.add_argument('--read-rounds', type=int, default=5)
    parser.add_argument('--body-bytes', type=int, default=1024)
    parser.add_argument('--cap', type=int, default=10000)
    parser.add_argument('--seed-batch', type=int, default=64)
    parser.add_argument('--counts', default='16,64,256')
    parser.add_argument('--concurrent-width', type=int, default=16)
    parser.add_argument('--env', action='append', default=[])
    parser.add_argument('--source-commit', default='unknown')
    parser.add_argument('--reads-only', action='store_true',
                        help='Keep seed and cold/repeated reads; omit mutation and pressure phases')
    args = parser.parse_args()
    args.counts = [int(value) for value in args.counts.split(',')]
    args.counts = [value for value in args.counts if value <= args.records_per_hash]
    if not (16 <= args.records_per_hash <= 100000 and 1 <= args.hashes <= 16
            and 1 <= args.read_rounds <= 21 and 128 <= args.body_bytes <= 32768
            and 1 <= args.seed_batch <= 64 and args.cap >= args.seed_batch * 5
            and args.counts and all(1 <= value <= 256 for value in args.counts)
            and 1 <= args.concurrent_width <= min(16, args.records_per_hash)):
        parser.error('Use bounded records, body, read rounds, and seed batches that fit the path cap')
    args.extra_env = {}
    for entry in args.env:
        name, separator, value = entry.partition('=')
        if not separator or name not in ('LASTDB_SCHEMA_PRELOAD_SCHEMAS', 'LASTDB_SCHEMA_KEY_BYTES'):
            parser.error('Only the explicit schema preload policy and key byte allowance are accepted')
        args.extra_env[name] = value
    return args


def key(number):
    slug = f'record-{number:06d}'.ljust(40, 'x')
    return f'todo#{number:08d}#{slug}'


def bucket(number):
    return f'generated-{number:02d}'


def native_counters(native):
    selected = {name: value for name, value in native.items()
                if name.startswith(('core_warm_', 'schema_key_'))}
    schema = native.get('schema_keys')
    if isinstance(schema, dict):
        selected.update({'schema_key_' + name: value for name, value in schema.items()})
    return selected


class Run:
    def __init__(self, args):
        self.args, self.identity = args, uuid.uuid4().hex
        if args.root is None:
            self.root = Path(tempfile.mkdtemp(prefix='sp.')).resolve()
        else:
            self.root = args.root.resolve()
            self.root.mkdir(mode=0o700, exist_ok=False)
        ownership.mark_created_root(self.root)
        self.receipt_lock, self.rows = threading.Lock(), []
        self.revisions, self.absent = {}, set()
        self.receipt_dir = args.receipt_dir.resolve()
        self.receipt_dir.mkdir(parents=True, exist_ok=False)
        source = args.binary.resolve(strict=True)
        self.binary = self.root / 'program-lastdbd'
        shutil.copy2(source, self.binary)
        actual = profile.file_hash(self.binary)
        if actual != profile.file_hash(source):
            raise RuntimeError('The binary changed during its private copy')
        profile.save(self.root / 'inputs.json', {
            'started_utc': now(), 'run_id': self.identity, 'variant': args.variant,
            'schema': SCHEMA, 'source_commit': args.source_commit,
            'source_binary': str(source), 'binary_sha256': actual,
            'platform': platform.platform(), 'load_start': os.getloadavg(),
            'root': str(self.root), 'owner_uid': os.getuid(),
            'records_per_hash': args.records_per_hash, 'hashes': args.hashes,
            'total_seed_records': args.records_per_hash * args.hashes,
            'range_bytes': 54, 'range_recipe': 'todo#8-digit position#40-byte generated slug',
            'body_bytes': args.body_bytes, 'seed_batch': args.seed_batch,
            'read_rounds': args.read_rounds, 'path_cap': args.cap,
            'counts': args.counts, 'concurrent_queries': min(4, args.hashes),
            'concurrent_keys_per_query': args.concurrent_width,
            'candidate_policy': args.extra_env, 'reads_only': args.reads_only,
            'filesystem_cache': 'uncontrolled; normal process reopen does not flush the OS cache',
            'driver_sha256': profile.file_hash(Path(__file__)),
            'imports': {name: profile.file_hash(SHARED / name)
                        for name in ('range_profile.py', 'ownership.py')}})
        print(json.dumps({'root': str(self.root), 'receipt_dir': str(self.receipt_dir),
                          'binary_sha256': actual}), flush=True)

    def values(self, h, n):
        revision = self.revisions.get((h, n), 0)
        return {'title': f'title-{h}-{n}-revision-{revision}',
                'body': profile.body(h * 10000000 + n + revision * 1000000,
                                     self.args.body_bytes)}

    def live(self, h, n):
        return (h, n) not in self.absent

    def remember(self, row):
        with self.receipt_lock:
            self.rows.append(row)
            with (self.root / 'receipts.jsonl').open('a') as output:
                output.write(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n')

    def finish(self, complete, error):
        groups = {}
        for row in self.rows:
            if row.get('operation') == 'query':
                groups.setdefault((row['phase'], row['shape'], row['batch']), []).append(row)
        summaries = []
        for (phase, shape, batch), rows in groups.items():
            times = sorted(row['elapsed_nanos'] / 1e6 for row in rows)
            summaries.append({'phase': phase, 'shape': shape, 'batch': batch, 'samples': len(times),
                'median_ms': statistics.median(times),
                'p95_ms': times[math.ceil(.95 * len(times)) - 1] if len(times) >= 5 else None,
                'cost_medians': profile.medians([row['cost'] for row in rows])})
        profile.save(self.root / 'summary.json', {'complete': complete, 'error': error,
            'finished_utc': now(), 'load_end': os.getloadavg(), 'groups': summaries})
        for name in ('inputs.json', 'receipts.jsonl', 'summary.json', '.synthetic-seed-owner.json'):
            path = self.root / name
            if path.exists():
                shutil.copy2(path, self.receipt_dir / name)
        profile.save(self.receipt_dir / 'retained-root.json', {
            'root': str(self.root), 'retained_for': 'supported cleanup review after receipt preservation',
            'all_owned_nodes_stopped': all(row.get('forced_kill') is False
                for row in self.rows if row.get('operation') == 'owned-node-stop')})


class Node(profile.Node):
    def __init__(self, run):
        super().__init__(run, run.args.variant, run.binary, 'off')

    def call(self, path, value=None, cost=False):
        with self.request_lock:
            self.sequence += 1
            request_id = f'preload-{self.run.identity}-{self.variant}-{self.sequence}'
        headers = {'Content-Type': 'application/json', 'X-LastDB-Client': 'schema-preload-measurement',
                   'X-LastDB-Request-Id': request_id}
        if cost:
            headers['x-lastdb-read-receipt'] = 'true'
        if value is not None and 'SampleN' in value.get('filter', {}):
            headers['x-lastdb-allow-full-scan'] = 'true'
        connection = profile.UnixConnection(self.socket)
        started = time.perf_counter_ns()
        try:
            connection.request('GET' if value is None else 'POST', path,
                None if value is None else json.dumps(value, separators=(',', ':')), headers)
            response = connection.getresponse()
            result = json.loads(response.read())
            elapsed = time.perf_counter_ns() - started
            raw_receipt = response.getheader('x-lastdb-read-receipt')
            receipt = None if raw_receipt is None else json.loads(raw_receipt)
            if response.status != 200:
                self.remember(operation='refusal', request_id=request_id, path=path,
                              http_status=response.status, request=value, result=result)
                raise RuntimeError(f'{self.variant} {path}: HTTP {response.status}')
        finally:
            connection.close()
        self.remember(operation='request', request_id=request_id, path=path,
                      request=value, result=result, cost=receipt, elapsed_nanos=elapsed)
        return result, elapsed, receipt, request_id

    def status(self, phase):
        result, elapsed, _, request_id = self.call('/api/db/native-status')
        native = result.get('data', result)['native_disk']
        if native.get('writer_failed') is not False:
            raise RuntimeError('The owned writer reports failure')
        selected = native_counters(native)
        sampled = subprocess.run(['ps', '-o', 'rss=', '-p', str(self.process.pid)],
                                 capture_output=True, text=True, check=True)
        self.remember(operation='status-observation', phase=phase, request_id=request_id,
                      counters=selected, native=native, elapsed_nanos=elapsed,
                      sampled_process_memory_bytes=int(sampled.stdout.strip()) * 1024)
        return native

    def flush_phase(self, phase):
        self.flush()
        self.status(phase + '-flushed')

    def disk_size(self, phase):
        result = subprocess.run(['du', '-sk', str(self.home)], capture_output=True,
                                text=True, check=False)
        fields = result.stdout.split()
        allocated = int(fields[0]) * 1024 if result.returncode == 0 and fields and fields[0].isdigit() else None
        self.remember(operation='disk-size-observation', phase=phase, data_home=str(self.home),
            allocated_bytes=allocated, command=['du', '-sk', str(self.home)],
            exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr,
            scope='Owned generated data home only; excludes private executable and request receipts')

    def seed(self):
        self.start()
        schema = {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': SCHEMA,
            'descriptive_name': 'Schema Preload Product Measurement', 'schema_type': 'HashRange',
            'key': {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': ['bucket', 'id', 'title', 'body'],
            'field_descriptions': {'bucket': 'generated record hash', 'id': 'generated board range',
                                   'title': 'generated title', 'body': 'generated body'}}}
        self.call('/api/schemas/seed-system', schema)
        batch = []
        for h in range(self.run.args.hashes):
            for n in range(self.run.args.records_per_hash):
                batch.append((h, n))
                if len(batch) == self.run.args.seed_batch:
                    self.mutate(batch, 'seed', operation='create', durability='durable')
                    self.flush()
                    batch = []
            print(f'{self.variant}: seed hash {h + 1}/{self.run.args.hashes}', flush=True)
        if batch:
            self.mutate(batch, 'seed', operation='create', durability='durable')
            self.flush()
        self.status('seed-complete')
        self.disk_size('seed-complete')
        self.stop()

    def mutate(self, pairs, phase, operation='update', durability='queued', revision=None):
        pairs, mutations = list(pairs), []
        for h, n in pairs:
            current = self.run.revisions.get((h, n), 0) if revision is None else revision
            values = {'title': f'title-{h}-{n}-revision-{current}',
                'body': profile.body(h * 10000000 + n + current * 1000000, self.run.args.body_bytes)}
            fields = {} if operation == 'delete' else values
            if operation == 'create':
                fields = dict(fields, bucket=bucket(h), id=key(n))
            mutations.append({'type': 'mutation', 'schema': SCHEMA, 'mutation_type': operation,
                'durability': durability, 'key_value': {'hash': bucket(h), 'range': key(n)},
                'fields_and_values': fields})
        result, elapsed, _, request_id = self.call('/api/mutations/batch', {'mutations': mutations})
        if result.get('count') != len(pairs) or result.get('ok') is False:
            raise RuntimeError('The complete generated mutation batch was not acknowledged')
        actual = result.get('durability')
        if actual not in ('queued', 'durable') or durability == 'durable' and actual != 'durable':
            raise RuntimeError('The mutation durability differs from its request')
        for pair in pairs:
            if operation == 'delete':
                self.run.absent.add(pair)
            else:
                self.run.absent.discard(pair)
                if revision is not None:
                    self.run.revisions[pair] = revision
        self.remember(operation='mutation', phase=phase, mutation_type=operation,
            requested_durability=durability, actual_durability=actual, batch=len(pairs),
            elapsed_nanos=elapsed, request_id=request_id)

    def query(self, pairs, shape, phase, repeat=0):
        pairs = list(pairs)
        if shape == 'range':
            h = pairs[0][0]
            selector = {'HashRangeRange': {'hash': bucket(h), 'start': key(pairs[0][1]),
                                         'end': key(pairs[-1][1] + 1)}}
        else:
            selector = {'HashRangeKeys': [[bucket(h), key(n)] for h, n in pairs]}
        result, elapsed, cost, request_id = self.call('/api/query', {
            'schema_name': SCHEMA, 'fields': FIELDS, 'filter': selector, 'limit': len(pairs)}, True)
        wanted = {(bucket(h), key(n)): self.run.values(h, n) for h, n in pairs if self.run.live(h, n)}
        actual = {}
        for row in result.get('results', []):
            address = (row['key']['hash'], row['key']['range'])
            actual[address] = {field: row['fields'].get(field) for field in FIELDS}
            for field in FIELDS:
                metadata = row['metadata'][field]
                if not metadata.get('atom_uuid') or not metadata.get('molecule_uuid'):
                    raise RuntimeError('A returned field lacks its atom or molecule identity')
        if actual != wanted or len(result.get('results', [])) != len(wanted):
            raise RuntimeError('The generated values or live key membership differ')
        if shape == 'range' and [row['key']['range'] for row in result['results']] != sorted(r for _, r in wanted):
            raise RuntimeError('The returned range order differs')
        if result.get('returned_count') != len(wanted) or result.get('has_more') is not False:
            raise RuntimeError('The page count or exhaustion differs')
        if cost is None:
            raise RuntimeError('The requested per-call read receipt is absent')
        self.remember(operation='query', phase=phase, shape=shape, batch=len(pairs), repeat=repeat,
            request_id=request_id, elapsed_nanos=elapsed, cost=cost,
            response_sha256=profile.digest(result), expected_values_match=True,
            response_metadata_checked=True, returned_records=len(wanted))
        return result

    def observed(self, pairs, shape, phase, repeat=0):
        before = self.status(phase + '-before')
        result = self.query(pairs, shape, phase, repeat)
        after = self.status(phase + '-after')
        before_counters, after_counters = native_counters(before), native_counters(after)
        delta = {name: after_counters[name] - value for name, value in before_counters.items()
                 if isinstance(value, int) and isinstance(after_counters.get(name), int)}
        self.remember(operation='query-observation', phase=phase, shape=shape, counters_delta=delta,
            pending_before_and_after=(before['accepted_sequence'] > before['completed_sequence']
                and after['accepted_sequence'] > after['completed_sequence']))
        return result

    def sample(self):
        result, elapsed, cost, request_id = self.call('/api/query', {
            'schema_name': SCHEMA, 'fields': FIELDS, 'filter': {'SampleN': 10}, 'limit': 10}, True)
        rows = result.get('results', [])
        if len(rows) != 10 or len({(row['key']['hash'], row['key']['range']) for row in rows}) != 10:
            raise RuntimeError('The capped sample does not return ten distinct generated keys')
        for row in rows:
            h = int(row['key']['hash'].removeprefix('generated-'))
            n = int(row['key']['range'].split('#')[1])
            if not (0 <= h < self.run.args.hashes and 0 <= n < self.run.args.records_per_hash):
                raise RuntimeError('The sample returns a key outside its generated schema')
            if {field: row['fields'].get(field) for field in FIELDS} != self.run.values(h, n):
                raise RuntimeError('The sample value differs from its generated value')
        if cost is None:
            raise RuntimeError('The sample read receipt is absent')
        self.remember(operation='query', phase='capped-preview', shape='sample', batch=10,
            request_id=request_id, elapsed_nanos=elapsed, cost=cost,
            response_sha256=profile.digest(result), expected_values_match=True, returned_records=10)

    def move(self, old, new):
        fields = dict(self.run.values(*new), bucket=bucket(new[0]), id=key(new[1]))
        mutations = [
            {'type': 'mutation', 'schema': SCHEMA, 'mutation_type': 'delete', 'durability': 'durable',
             'key_value': {'hash': bucket(old[0]), 'range': key(old[1])}, 'fields_and_values': {}},
            {'type': 'mutation', 'schema': SCHEMA, 'mutation_type': 'create', 'durability': 'durable',
             'key_value': {'hash': bucket(new[0]), 'range': key(new[1])}, 'fields_and_values': fields}]
        result, elapsed, _, request_id = self.call('/api/mutations/batch', {'mutations': mutations})
        if result.get('count') != 2 or result.get('durability') != 'durable':
            raise RuntimeError('The key move lacks its complete durable batch ack')
        self.run.absent.add(old)
        self.run.absent.discard(new)
        self.remember(operation='mutation', phase='key-move', mutation_type='delete-and-create',
            requested_durability='durable', actual_durability='durable', batch=2,
            elapsed_nanos=elapsed, request_id=request_id)


def workload(node):
    args = node.run.args
    exact = [(h % args.hashes, h // args.hashes) for h in range(16)]
    window = [(0, n) for n in range(16)]
    node.seed()
    for shape, pairs in [('exact', exact), ('range', window)]:
        node.start()
        node.observed(pairs, shape, 'first-' + shape)
        for count in args.counts:
            selected = ([(h % args.hashes, h // args.hashes) for h in range(count)]
                        if shape == 'exact' else [(0, n) for n in range(count)])
            node.observed(selected, shape, 'prime-' + shape)
            for repeat in range(args.read_rounds):
                node.observed(selected, shape, 'repeated-' + shape, repeat)
        node.stop()
    node.start()
    node.status('concurrent-first-before')
    with ThreadPoolExecutor(max_workers=min(4, args.hashes)) as pool:
        futures = [pool.submit(node.query, [(h, n) for n in range(args.concurrent_width)], 'range',
                               'concurrent-first', h) for h in range(min(4, args.hashes))]
        for future in futures:
            future.result()
    node.status('concurrent-first-after')
    node.sample()
    if not args.reads_only:
        node.mutate([(0, n) for n in range(4)], 'related-update', revision=1)
        node.observed(window, 'range', 'related-update-ack')
        node.flush_phase('related-update')
        node.observed(window, 'range', 'related-update-flush')
        node.mutate([(0, n) for n in range(args.records_per_hash, args.records_per_hash + 4)],
                    'insert', operation='create')
        node.observed([(0, n) for n in range(args.records_per_hash, args.records_per_hash + 4)],
                      'range', 'insert-ack')
        node.mutate([(0, 2), (0, 3)], 'delete', operation='delete', durability='durable')
        node.observed(window, 'range', 'delete-ack')
        node.move((0, 4), (0, args.records_per_hash + 4))
        node.observed([(0, 4), (0, args.records_per_hash + 4)], 'exact', 'key-move-ack')
        node.flush_phase('before-pressure')
        for h in range(args.hashes):
            for first in range(0, min(args.records_per_hash, 96), 16):
                node.query([(h, n) for n in range(first, min(first + 16, args.records_per_hash))],
                           'exact', 'selected-value-walk', first)
        node.observed(window, 'range', 'after-selected-value-walk')
    node.flush_phase('final')
    node.stop()
    node.start()
    node.observed(window, 'range', 'normal-reopen')
    node.status('final')


def main():
    args = arguments()
    run, node, complete, error = Run(args), None, False, None
    names = {'LASTDB_RESIDENT_KEY_CAP': str(args.cap), **args.extra_env}
    previous = {name: os.environ.get(name) for name in names}
    try:
        os.environ.update(names)
        node = Node(run)
        workload(node)
        complete = True
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        run.remember({'operation': 'failure', 'error': error})
    finally:
        if node is not None:
            try:
                node.stop()
            except Exception as exc:
                complete, error = False, f'{error or ""}; stop: {exc}'
                run.remember({'operation': 'failure', 'error': error})
            if node.process is None:
                node.disk_size('end-after-stop')
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        run.finish(complete, error)
    print(json.dumps({'complete': complete, 'error': error, 'root': str(run.root),
                      'receipt_dir': str(run.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
