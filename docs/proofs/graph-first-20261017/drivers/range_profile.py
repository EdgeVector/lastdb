#!/usr/bin/env python3
"""Measure real exact and range requests against independently generated homes."""
import argparse
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import uuid

sys.dont_write_bytecode = True
from ownership import mark_created_root

SCHEMA = 'Layer2QueueBenchmark'
FIELDS = ['title', 'body']


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def file_hash(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def identifier(number):
    return f'record-{number:06d}'


def body(number, length):
    return ''.join(hashlib.sha256(f'Layer2Queue:{number}:0:{chunk}'.encode()).hexdigest()
                   for chunk in range((length + 63) // 64))[:length]


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=180)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


class Run:
    def __init__(self, args):
        self.args, self.identity = args, uuid.uuid4().hex
        self.root = args.output.resolve()
        self.root.mkdir(parents=True, mode=0o700, exist_ok=False)
        mark_created_root(self.root)
        self.receipt_lock = threading.Lock()
        self.rows = []
        self.expected = {number: {'title': f'title-{number}-revision-0',
                                  'body': body(number, args.body_bytes)}
                         for number in range(args.records)}

    def remember(self, row):
        with self.receipt_lock:
            self.rows.append(row)
            with (self.root / 'receipts.jsonl').open('a') as output:
                output.write(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n')

    def inputs(self, nodes):
        save(self.root / 'inputs.json', {
            'run_id': self.identity, 'started_utc': now(), 'platform': platform.platform(),
            'load_start': os.getloadavg(), 'filesystem_cache': 'unknown',
            'driver_sha256': file_hash(Path(__file__)), 'schema': SCHEMA,
            'records': self.args.records, 'body_bytes': self.args.body_bytes,
            'body_recipe': 'first N ASCII bytes of SHA256 hex chunks Layer2Queue:number:0:chunk',
            'seed_batch': 64, 'native': True, 'resident_mode': 'write', 'disk_cap_bytes': None,
            'shapes': self.args.shapes, 'counts': self.args.counts,
            'read_rounds': self.args.read_rounds, 'query_start': self.args.start,
            'expected_values_sha256': digest(self.expected),
            'binaries': {node.variant: {'path': str(node.binary), 'sha256': file_hash(node.binary),
                                      'diagnostics': node.diagnostics} for node in nodes}})

    def summarize(self):
        groups = {}
        for row in self.rows:
            if row['operation'] == 'query':
                key = (row['variant'], row['phase'], row['shape'], row['batch'])
                groups.setdefault(key, []).append(row)
        summaries = []
        for key, rows in groups.items():
            times = sorted(row['elapsed_nanos'] / 1e6 for row in rows)
            summary = dict(zip(['variant', 'phase', 'shape', 'batch'], key))
            summary.update(samples=len(times), median_ms=statistics.median(times),
                           p95_ms=times[math.ceil(.95 * len(times)) - 1] if len(times) >= 5 else None)
            summary['cost_medians'] = medians([row['cost'] for row in rows])
            stages = [row['cost'].get('native_cpu_diagnostics') for row in rows]
            if all(isinstance(stage, dict) for stage in stages):
                summary['native_cpu_stage_medians'] = {
                    name: medians([stage['stages'][name] for stage in stages])
                    for name in stages[0]['stages']}
            summaries.append(summary)
        save(self.root / 'summary.json', summaries)


class Node:
    def __init__(self, run, variant, binary, diagnostics):
        self.run, self.variant = run, variant
        self.binary, self.diagnostics = binary.resolve(strict=True), diagnostics
        self.directory = run.root / variant[0]
        self.directory.mkdir(mode=0o700)
        self.home = self.directory / 'h'
        self.home.mkdir(mode=0o700)
        self.user_home = self.directory / 'u'
        self.user_home.mkdir(mode=0o700)
        self.socket, self.full_socket = self.directory / 'q.sock', self.directory / 'f.sock'
        if len(str(self.full_socket).encode()) + 8 >= 103:
            raise ValueError('Use a shorter output path for the owned sockets')
        self.process, self.log, self.sequence = None, None, 0
        self.request_lock = threading.Lock()

    def call(self, path, value=None, cost=False):
        with self.request_lock:
            self.sequence += 1
            request_id = f'range-{self.run.identity}-{self.variant}-{self.sequence}'
        headers = {'Content-Type': 'application/json', 'X-LastDB-Client': 'layer2-range-profile',
                   'X-LastDB-Request-Id': request_id}
        if cost:
            headers['x-lastdb-read-receipt'] = 'true'
        raw = None if value is None else json.dumps(value, separators=(',', ':'))
        connection = UnixConnection(self.socket)
        started = time.perf_counter_ns()
        try:
            connection.request('GET' if value is None else 'POST', path, raw, headers)
            response = connection.getresponse()
            result = json.loads(response.read())
            elapsed = time.perf_counter_ns() - started
            if response.status != 200:
                self.remember(operation='refusal', request_id=request_id, path=path,
                              http_status=response.status, result=result)
                raise RuntimeError(f'{self.variant} {path}: HTTP {response.status}')
            receipt = response.getheader('x-lastdb-read-receipt')
            return result, elapsed, None if receipt is None else json.loads(receipt), request_id
        finally:
            connection.close()

    def remember(self, **row):
        self.run.remember(dict(variant=self.variant, diagnostics=self.diagnostics, **row))

    def start(self):
        if self.process is not None:
            raise RuntimeError('Owned node is already active')
        environment = dict(os.environ, LASTDB_ISOLATED_COPY='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
                           HOME=str(self.user_home),
                           LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                           LASTDB_RESIDENT_MODE='write', LASTDB_DISK_PACK='1', RUST_MIN_STACK='16777216')
        for name in ['LASTDB_DISK_CAP_BYTES', 'SENTRY_DSN', 'FOLD_SENTRY_DSN', 'RUST_LOG',
                     'LASTDB_NATIVE_READ_DIAGNOSTICS']:
            environment.pop(name, None)
        if self.diagnostics == 'on':
            environment['LASTDB_NATIVE_READ_DIAGNOSTICS'] = '1'
        self.log = (self.directory / 'node.log').open('a')
        self.process = subprocess.Popen([str(self.binary), '--data-dir', str(self.home),
                                         '--socket-path', str(self.socket),
                                         '--full-socket-path', str(self.full_socket)], env=environment,
                                        stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember(operation='owned-node-start', pid=self.process.pid, home=str(self.home))
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f'{self.variant} exits before health; inspect its node.log')
            if self.socket.exists():
                try:
                    result, _, _, _ = self.call('/health')
                    if result.get('status') == 'ok' or result.get('ok') is True:
                        return
                except (OSError, http.client.HTTPException):
                    pass
            time.sleep(.05)
        raise RuntimeError('Owned node health deadline')

    def stop(self):
        if self.process is None:
            return
        process, forced = self.process, False
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                code = process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                process.kill()
                code, forced = process.wait(timeout=15), True
        else:
            code = process.returncode
        self.remember(operation='owned-node-stop', pid=process.pid, exit_code=code, forced_kill=forced)
        self.process = None
        self.log.close()
        self.log = None
        if forced or code not in [0, -signal.SIGTERM]:
            raise RuntimeError(f'{self.variant} normal stop returned {code}')

    def flush(self):
        result, elapsed, _, request_id = self.call('/api/db/flush', {})
        data = result.get('data', result)
        receipt = data.get('native_flush', {})
        if data.get('durable') is not True or ('target_sequence' in receipt
                and receipt['completed_sequence'] < receipt['target_sequence']):
            raise RuntimeError('Explicit flush does not cover the accepted prefix')
        self.remember(operation='flush', request_id=request_id, elapsed_nanos=elapsed, result=result)

    def seed(self):
        self.start()
        schema = {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': SCHEMA, 'descriptive_name': 'Layer 2 Queue Benchmark',
            'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': ['bucket', 'id', 'title', 'body'],
            'field_descriptions': {'bucket': 'generated record hash', 'id': 'generated record range',
                                   'title': 'generated title', 'body': 'generated body'}}}
        result, elapsed, _, request_id = self.call('/api/schemas/seed-system', schema)
        self.remember(operation='schema-setup', request_id=request_id, elapsed_nanos=elapsed, result=result)
        for first in range(0, self.run.args.records, 64):
            numbers = range(first, min(first + 64, self.run.args.records))
            mutations = [{'type': 'mutation', 'schema': SCHEMA, 'mutation_type': 'create',
                          'key_value': {'hash': 'generated', 'range': identifier(number)},
                          'fields_and_values': dict(self.run.expected[number], bucket='generated',
                                                    id=identifier(number)), 'durability': 'durable'}
                         for number in numbers]
            result, elapsed, _, request_id = self.call('/api/mutations/batch', {'mutations': mutations})
            if result.get('count') != len(mutations) or result.get('durability') != 'durable':
                raise RuntimeError('Seed request lacks complete durable acknowledgement')
            self.remember(operation='seed', request_id=request_id, first=first, batch=len(mutations),
                          elapsed_nanos=elapsed, result=result)
        self.flush()
        self.stop()

    def query(self, numbers, shape, phase, repeat):
        selector = ({'HashRangeKeys': [['generated', identifier(number)] for number in numbers]}
                    if shape == 'exact' else {'HashRangeRange': {
                        'hash': 'generated', 'start': identifier(numbers[0]),
                        'end': identifier(numbers[-1] + 1)}})
        result, elapsed, cost, request_id = self.call('/api/query', {
            'schema_name': SCHEMA, 'fields': FIELDS, 'filter': selector, 'limit': len(numbers)}, True)
        try:
            validate_query(result, numbers, self.run.expected, shape)
        except (KeyError, TypeError, RuntimeError):
            save(self.directory / f'query-mismatch-{self.sequence}.json', {
                'request_id': request_id, 'shape': shape, 'numbers': numbers,
                'actual_response': result, 'cost': cost})
            raise
        if cost is None:
            raise RuntimeError('Requested read receipt is absent')
        if self.diagnostics == 'on' and not isinstance(cost.get('native_cpu_diagnostics'), dict):
            raise RuntimeError('The selected program does not expose the enabled CPU diagnostic')
        response_hash = digest(result)
        self.remember(operation='query', request_id=request_id, phase=phase, shape=shape,
                      batch=len(numbers), repeat=repeat, elapsed_nanos=elapsed, cost=cost,
                      response_sha256=response_hash, expected_values_match=True,
                      returned_records=len(result['results']))
        if phase == 'prime':
            save(self.directory / f'prime-{shape}-{len(numbers)}.json', result)
        return response_hash

    def forensics(self, name):
        result, _, _, request_id = self.call('/api/status?forensics=1')
        status = result.get('data', result).get('status', {})
        save(self.directory / f'ops-{name}.json', {
            'request_id': request_id, 'request_ops': status.get('request_ops'),
            'note': 'The published status sample can lag the last query.'})


def validate_query(result, numbers, expected, shape):
    records = result.get('results', [])
    wanted = {identifier(number): expected[number] for number in numbers}
    actual = {record['key']['range']: {field: record['fields'].get(field) for field in FIELDS}
              for record in records}
    if actual != wanted or len(records) != len(wanted):
        raise RuntimeError('Query differs from generated values or record count')
    for record in records:
        if record['key'].get('hash') != 'generated' or record['fields'].get('bucket') != 'generated':
            raise RuntimeError('Returned hash or required hash field differs')
        if 'id' in record['fields'] and record['fields']['id'] != record['key']['range']:
            raise RuntimeError('Returned range field differs from its key')
    flags = {'returned_count': len(numbers), 'has_more': False, 'next_cursor': None,
             'unresolved_rows': 0, 'offset': 0, 'tombstoned_rows': 0}
    if any(result.get(name) != value for name, value in flags.items()):
        raise RuntimeError('Query completeness or pagination metadata differs')
    if shape == 'range' and [record['key']['range'] for record in records] != sorted(wanted):
        raise RuntimeError('Range order differs')


def medians(rows):
    return {name: statistics.median(row[name] for row in rows)
            for name in rows[0] if all(isinstance(row.get(name), (int, float))
                                      and not isinstance(row[name], bool) for row in rows)}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def receipt_hashes(run, nodes):
    paths = [run.root / name for name in ['inputs.json', 'summary.json', 'receipts.jsonl', 'ownership.json']]
    for node in nodes:
        paths.extend(path for path in node.directory.iterdir() if path.is_file())
    return {str(path.relative_to(run.root)): file_hash(path) for path in paths if path.is_file()}


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['before', 'after', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ['before-diagnostics', 'after-diagnostics']:
        parser.add_argument('--' + name, choices=['on', 'off'], default='off')
    parser.add_argument('--body-bytes', type=int, default=1024)
    parser.add_argument('--records', type=int, default=320)
    parser.add_argument('--read-rounds', type=int, default=21)
    parser.add_argument('--start', type=int, default=32)
    parser.add_argument('--counts', default='1,16,64,256')
    parser.add_argument('--shapes', default='exact,range')
    args = parser.parse_args()
    args.counts, args.shapes = [int(count) for count in args.counts.split(',')], args.shapes.split(',')
    if (not args.counts or any(count < 1 or count > 256 for count in args.counts)
            or not args.shapes or any(shape not in ['exact', 'range'] for shape in args.shapes)
            or args.body_bytes < 0 or args.body_bytes > 16384 or args.read_rounds < 1
            or args.start < 0 or args.records < args.start + max(args.counts)):
        parser.error('Use valid shapes, counts 1..256, bodies 0..16384, and enough generated records')
    return args


def main():
    run, nodes, success, errors = Run(arguments()), [], False, []
    try:
        nodes = [Node(run, variant, getattr(run.args, variant), getattr(run.args, variant + '_diagnostics'))
                 for variant in ['before', 'after']]
        run.inputs(nodes)
        save(run.root / 'ownership.json', {'run_id': run.identity, 'uid': os.getuid(),
             'root': str(run.root), 'homes': [str(node.home) for node in nodes], 'origin': 'generated-empty'})
        for node in nodes:
            node.seed()
        for node in nodes:
            node.start()
        for shape in run.args.shapes:
            for count in run.args.counts:
                numbers = list(range(run.args.start, run.args.start + count))
                primes = {node.variant: node.query(numbers, shape, 'prime', 0) for node in nodes}
                for repeat in range(run.args.read_rounds):
                    for node in nodes if repeat % 2 == 0 else reversed(nodes):
                        if node.query(numbers, shape, 'steady', repeat) != primes[node.variant]:
                            raise RuntimeError('Repeated complete response differs from its prime')
                for node in nodes:
                    node.forensics(f'{shape}-{count}')
                print(f'{shape} {count}: {run.args.read_rounds} calls per program complete', flush=True)
        for node in nodes:
            node.flush()
            node.stop()
            node.start()
            for first in range(0, run.args.records, 256):
                node.query(list(range(first, min(first + 256, run.args.records))), 'exact', 'normal-reopen', first)
        success = True
    except Exception as error:
        errors.append(f'{type(error).__name__}: {error}')
    finally:
        for node in nodes:
            try:
                node.stop()
            except Exception as error:
                errors.append(f'{type(error).__name__}: {error}')
        run.summarize()
        save(run.root / 'completion.json', {'success': success and not errors, 'errors': errors,
             'finished_utc': now(), 'load_end': os.getloadavg(), 'query_count': sum(
                 row['operation'] == 'query' for row in run.rows), 'receipt_sha256': receipt_hashes(run, nodes)})
    if errors:
        raise RuntimeError('; '.join(errors))
    save(run.root / 'completed.json', {'finished_utc': now(), 'expected_values_match': True})
    print(str(run.root / 'summary.json'), flush=True)


if __name__ == '__main__':
    main()
