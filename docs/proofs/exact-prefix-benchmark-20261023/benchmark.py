#!/usr/bin/env python3
"""Bounded before/after Mini benchmark on a fresh, owned synthetic home."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output
import platform
import random
import statistics
import shutil
import subprocess
import time

sys.dont_write_bytecode = True
PROOFS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROOFS / 'snapshot-commits-20261022'))
from capture import Capture, UnixConnection, file_hash, save

FIELDS = ['bucket', 'id', 'title', 'body']
COUNT = 512
SCHEMAS = {'hash': 'ExactKeyBenchHash', 'range': 'ExactKeyBenchRange'}
SETTINGS = {'LASTDB_AUTO_MAINTENANCE': '0', 'LASTDB_RESIDENT_PERSIST_MS': '100',
            'LASTDB_UDS_WORKERS': '4', 'RUST_MIN_STACK': '16777216', 'RUST_LOG': 'warn'}


def key(n):
    return f'key-{n:06d}'


def values(n, revision):
    body = ''.join(hashlib.sha256(f'exact-key:{n}:{revision}:{part}'.encode()).hexdigest()
                   for part in range(16))
    return dict(bucket='measure', id=key(n), title=f'title-{n}-{revision}', body=body)


def coordinate(kind, n):
    return {'hash': key(n)} if kind == 'hash' else {'hash': 'measure', 'range': key(n)}


def mutation(kind, n, revision, mode='durable', action='update'):
    fields = values(n, revision)
    if action == 'update':
        fields = {name: fields[name] for name in ('title', 'body')}
    if action in ('delete', 'purge'):
        fields = {}
    return dict(type='mutation', schema=SCHEMAS[kind], mutation_type=action,
                durability=mode, key_value=coordinate(kind, n), fields_and_values=fields)


def query(kind, selector, limit=64):
    return dict(schema_name=SCHEMAS[kind], fields=FIELDS, filter=selector, limit=limit)


def stats(samples):
    ordered = sorted(samples)
    return {'requests': len(samples), 'median_ms': statistics.median(samples),
            'p95_ms': ordered[math.ceil(len(samples) * .95) - 1],
            'mean_ms': statistics.mean(samples), 'max_ms': max(samples),
            'first_request_ms': samples[0], 'total_ms': sum(samples)}


class Bench(Capture):
    def __init__(self, args):
        super().__init__(args)
        self.args = args
        self.samples, self.phases = [], {}
        self.revisions = {kind: [0] * COUNT for kind in SCHEMAS}
        self.deadline = time.monotonic() + 600
        self.free_bytes_before = shutil.disk_usage(self.root).free
        if self.free_bytes_before < 8 * 1024 ** 3:
            raise RuntimeError("Benchmark requires at least 8 GiB free before a run")
        self.settings = dict(SETTINGS)
        if args.config == 'plain':
            self.settings.update(LASTDB_HASH_KEY_ENCODING='plain', LASTDB_RANGE_KEY_ENCODING='plain')
        else:
            self.settings.update(LASTDB_HASH_KEY_ENCODING='blind_v1', LASTDB_RANGE_KEY_ENCODING='ope_v1')
        if args.config == 'preload':
            self.settings['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = SCHEMAS['range']
        save(self.output / 'inputs.json', {'source_revision': args.source_revision,
             'binary': str(self.binary), 'binary_sha256': file_hash(self.binary),
             'driver_sha256': file_hash(Path(__file__)), 'settings': self.settings,
             'config': args.config, 'records_per_schema': COUNT, 'schemas': SCHEMAS,
             'fields': FIELDS, 'body_bytes': 1024, 'concurrency': 1,
             'build_profile': 'release', 'rustc': args.rustc,
             'platform': platform.platform(), 'cpu_count': os.cpu_count(),
             'cpu_quota': Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
             'memory_limit': Path('/sys/fs/cgroup/memory.max').read_text().strip(),
             'clock': 'perf_counter_ns', 'transport': 'HTTP over owner Unix socket',
             'cache': 'OS cache uncontrolled; reopen-first-pass is not cold filesystem',
             'limits': {'seconds': 600, 'request_timeout_seconds': 30},
             'free_bytes_before': self.free_bytes_before,
             'data': 'Generated only, fresh owned home, no user data'})

    def start(self):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(('LASTDB', 'FOLD', 'SENTRY', 'OBS_SENTRY'))}
        env.update(self.settings, LASTDB_ISOLATED_COPY='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
                   LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home))
        self.log = (self.output / 'node.log').open('a')
        self.process = subprocess.Popen([str(self.binary), '--data-dir', str(self.home),
            '--socket-path', str(self.socket), '--full-socket-path', str(self.full_socket)],
            env=env, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember('owned-node-start', pid=self.process.pid)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('Owned daemon exited; inspect node.log')
            if self.socket.exists():
                try:
                    health = self.call('/health')
                    if health.get('status') == 'ok' or health.get('ok') is True:
                        return
                except (OSError, ConnectionError):
                    pass
            time.sleep(.05)
        raise RuntimeError('Health deadline exceeded')

    def request(self, path, value=None):
        if time.monotonic() > self.deadline:
            raise RuntimeError('Benchmark exceeded its ten-minute bound')
        body = None if value is None else json.dumps(value, separators=(',', ':')).encode()
        connection = UnixConnection(self.socket)
        connection.timeout = 30
        start = time.perf_counter_ns()
        try:
            connection.request('GET' if value is None else 'POST', path, body,
                {'Content-Type': 'application/json', 'X-LastDB-Client': 'exact-key-benchmark'})
            response = connection.getresponse()
            raw = response.read()
            elapsed = (time.perf_counter_ns() - start) / 1e6
            result = json.loads(raw)
            if response.status != 200:
                raise RuntimeError(f'{path}: HTTP {response.status}: {str(result)[:600]}')
            return result.get('data', result), elapsed
        finally:
            connection.close()

    def call(self, path, value=None, cost=False):
        return self.request(path, value)[0]

    def flush(self):
        result, elapsed = self.request('/api/db/flush', {})
        native = result.get('native_flush', {})
        if (result.get('durable') is not True or
                native.get('completed_sequence', -1) < native.get('target_sequence', 0)):
            raise RuntimeError('Explicit flush lacks complete durability')
        return elapsed

    def install(self):
        for kind, name in SCHEMAS.items():
            self.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
                'source': 'system_seed', 'name': name, 'descriptive_name': name,
                'schema_type': 'Hash' if kind == 'hash' else 'HashRange',
                'key': {'hash_field': 'id'} if kind == 'hash' else
                       {'hash_field': 'bucket', 'range_field': 'id'},
                'fields': FIELDS,
                'field_descriptions': {f: f'{name} independent {f}' for f in FIELDS}}})
            for start in range(0, COUNT, 32):
                self.write(kind, list(range(start, start + 32)), 0, 'durable', 'create')
        self.flush()

    def write(self, kind, numbers, revision, mode, action='update'):
        request = {'mutations': [mutation(kind, n, revision, mode, action) for n in numbers]}
        result, elapsed = self.request('/api/mutations/batch', request)
        if result.get('count') != len(numbers) or result.get('durability') not in ('queued', 'durable'):
            raise RuntimeError('Incomplete mutation acknowledgement')
        if mode == 'durable' and result['durability'] != 'durable':
            raise RuntimeError('Durable request was not durable')
        for n in numbers:
            self.revisions[kind][n] = None if action in ('delete', 'purge') else revision
        return result, elapsed

    def read(self, kind, selector, numbers, limit=64):
        result, elapsed = self.request('/api/query', query(kind, selector, limit))
        expected = {key(n): values(n, self.revisions[kind][n]) for n in set(numbers)
                    if n < COUNT and self.revisions[kind][n] is not None}
        actual = {}
        returned_keys = []
        for row in result.get('results', []):
            n = row['key']['hash' if kind == 'hash' else 'range']
            if n in actual:
                raise RuntimeError('Duplicate row in product response')
            actual[n] = row['fields']
            returned_keys.append(n)
        if (actual != expected or result.get('returned_count') != len(expected)
                or result.get('has_more') is not False):
            raise RuntimeError(f'Wrong membership/values: {kind} {selector}; got {len(actual)}, want {len(expected)}')
        if ('HashRangeRange' in selector or 'HashRangePrefix' in selector) and returned_keys != sorted(returned_keys):
            raise RuntimeError('Range order differs from API key order')
        return elapsed

    def reads(self, phase, kind, requests):
        samples = [self.read(kind, selector, numbers, limit) for selector, numbers, limit in requests]
        self.phases[phase] = stats(samples)
        print(f'{phase}: {self.phases[phase]["median_ms"]:.3f} ms median', flush=True)
        self.samples.extend({'phase': phase, 'ms': value} for value in samples)

    def writes(self, phase, kind, width, count, mode, revision):
        samples, acknowledgements = [], {}
        start = time.perf_counter_ns()
        for i in range(count):
            numbers = [(i * width + j) % COUNT for j in range(width)]
            result, elapsed = self.write(kind, numbers, revision + i, mode)
            acknowledgements[result['durability']] = acknowledgements.get(result['durability'], 0) + 1
            samples.append(elapsed)
        flush = self.flush()
        wall = (time.perf_counter_ns() - start) / 1e9
        self.phases[phase] = {**stats(samples), 'records_per_request': width,
            'acknowledgements': acknowledgements, 'final_flush_ms': flush,
            'wall_through_flush_seconds': wall, 'records_per_second_through_flush': width * count / wall}
        print(f'{phase}: {self.phases[phase]["median_ms"]:.3f} ms median', flush=True)
        self.samples.extend({'phase': phase, 'ms': value} for value in samples)

    def workloads(self):
        rng = random.Random(269)
        numbers = list(range(COUNT))
        rng.shuffle(numbers)
        for kind in SCHEMAS:
            requests = [({'HashKey': key(n)} if kind == 'hash' else
                         {'HashRangeKey': {'hash': 'measure', 'range': key(n)}}, [n], 1)
                        for n in numbers[:128]]
            self.reads(f'{kind}_reopen_first_pass', kind, requests)
            self.reads(f'{kind}_warm_point', kind, requests * 3)
            misses = [({'HashKey': key(n)} if kind == 'hash' else
                       {'HashRangeKey': {'hash': 'measure', 'range': key(n)}}, [n], 1)
                      for n in range(COUNT, COUNT + 64)]
            self.reads(f'{kind}_missing_point', kind, misses * 2)
        batches = [numbers[i:i+32] for i in range(0, COUNT, 32)]
        multi = [({'HashRangeKeys': [['measure', key(n)] for n in batch]}, batch, 32)
                 for batch in batches]
        for selector, batch, limit in multi:
            self.read('range', selector, batch, limit)
        self.reads('range_multiget32', 'range', multi * 6)
        self.reads('range_between64', 'range', [({'HashRangeRange': {
            'hash': 'measure', 'start': key(128), 'end': key(192)}}, list(range(128, 192)), 64)] * 96)
        self.reads('range_prefix10', 'range', [({'HashRangePrefix': {
            'hash': 'measure', 'prefix': 'key-00012'}}, list(range(120, 130)), 16)] * 96)
        self.reads('range_prefix100', 'range', [({'HashRangePrefix': {
            'hash': 'measure', 'prefix': 'key-0001'}}, list(range(100, 200)), 128)] * 96)
        self.reads('range_partition512', 'range', [({'HashKey': 'measure'},
                   list(range(COUNT)), COUNT)] * 24)
        # Exercise duplicate input positions and misses through the same measured API.
        self.reads('range_duplicates_and_miss', 'range', [({'HashRangeKeys': [
            ['measure', key(n)] for n in [7, 4, 7, COUNT, 9, 4]]}, [7, 4, COUNT, 9], 16)] * 64)
        for kind in SCHEMAS:
            for mode in ('queued', 'durable'):
                for width, count in ((1, 64), (32, 12)):
                    self.writes(f'{kind}_{mode}_write{width}', kind, width, count, mode,
                                1000 + (100 if mode == 'durable' else 0) + width * 10)
        for kind in SCHEMAS:
            for n in range(0, COUNT, 32):
                if kind == 'range':
                    self.read(kind, {'HashRangeKeys': [['measure', key(i)] for i in range(n, n+32)]},
                              list(range(n, n+32)), 32)
                else:
                    for i in range(n, n+32):
                        self.read(kind, {'HashKey': key(i)}, [i], 1)
        # Post-write delete visibility and graceful reopen are benchmark integrity checks.
        for kind in SCHEMAS:
            self.write(kind, [0, 1, 2], 0, 'durable', 'delete')
            self.read(kind, {'HashKey': key(0)} if kind == 'hash' else
                      {'HashRangeKeys': [['measure', key(i)] for i in range(4)]},
                      [0] if kind == 'hash' else list(range(4)), 8)
        for kind in SCHEMAS:
            self.write(kind, [3, 4], 0, 'durable', 'purge')
        self.read('range', {'HashRangePrefix': {'hash': 'measure', 'prefix': 'key-00000'}},
                  list(range(10)), 16)
        self.flush()
        self.stop()
        self.start()
        for kind in SCHEMAS:
            for n in [0, 1, 2, 3, 31, 127, 511]:
                self.read(kind, {'HashKey': key(n)} if kind == 'hash' else
                          {'HashRangeKey': {'hash': 'measure', 'range': key(n)}}, [n], 1)
        self.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'scratch', 'output'):
        parser.add_argument('--' + name, type=Path, required=name != 'output',
                            default=default_output('exact-prefix-benchmark-20261023') if name == 'output' else None)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--config', choices=['plain', 'encrypted', 'preload'], required=True)
    parser.add_argument('--rustc', default='rustc 1.96.1')
    args = parser.parse_args()
    args.output = resolve_output(args.output)
    bench = Bench(args)
    summary = {'complete': False}
    try:
        bench.start()
        bench.install()
        bench.stop()
        bench.start()
        bench.workloads()
        summary.update(complete=True, phases=bench.phases, values_and_delete_visibility_verified=True)
    except BaseException as error:
        summary['error'] = str(error)
        raise
    finally:
        bench.stop()
        save(bench.output / 'samples.json', bench.samples)
        summary.update(source_revision=args.source_revision, config=args.config,
                       all_owned_processes_stopped=bench.process is None,
                       free_bytes_after=shutil.disk_usage(bench.root).free)
        save(bench.output / 'summary.json', summary)
        print(json.dumps({'complete': summary['complete'], 'output': str(bench.output)}), flush=True)


if __name__ == '__main__':
    main()
