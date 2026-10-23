#!/usr/bin/env python3
"""Identical-fixture read/write comparison of owned query result formatting."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'native-routing-benchmark-20261023'))
from cleanup import cleanup
from controlled import ControlledBench, fixture_digest
from benchmark import COUNT, FIELDS, SCHEMAS, file_hash, key, query, save, stats, values

NESTED_SCHEMA = "OwnedFormatNested"
NESTED_COUNT = 256


def document(n):
    return {"enabled": n % 2 == 0, "optional": None, "items": [
        {"index": i, "text": hashlib.sha256(f"{n}:{i}".encode()).hexdigest(),
         "tags": ["alpha", i, True, None], "attributes": {"title": f"item-{n}-{i}",
         "counts": [i, i+1, i+2]}} for i in range(16)]}


class OwnedFormatBench(ControlledBench):
    def __init__(self, args):
        super().__init__(args)
        self.capture_name = None
        self.fingerprints = {}
        self.documents = [document(n) for n in range(NESTED_COUNT)]

    def install(self):
        super().install()
        self.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': NESTED_SCHEMA, 'descriptive_name': NESTED_SCHEMA,
            'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': ['bucket', 'id', 'document'], 'field_descriptions': {
                'bucket': 'Nested profile partition', 'id': 'Nested profile key',
                'document': 'Structured JSON benchmark document'}}})
        for start in range(0, NESTED_COUNT, 32):
            result = self.call('/api/mutations/batch', {'mutations': [dict(type='mutation',
                schema=NESTED_SCHEMA, mutation_type='create', durability='durable',
                key_value={'hash': 'measure', 'range': key(n)}, fields_and_values={
                'bucket': 'measure', 'id': key(n), 'document': self.documents[n]})
                for n in range(start, start+32)]})
            if result.get('count') != 32 or result.get('durability') != 'durable':
                raise RuntimeError('Nested fixture seed was not fully durable')
        self.flush()

    def nested_read(self, width):
        request = dict(schema_name=NESTED_SCHEMA, fields=['bucket', 'id', 'document'],
            filter={'HashRangeRange': {'hash': 'measure', 'start': key(0), 'end': key(width)}}, limit=width)
        result, elapsed = self.request('/api/query', request)
        rows = result['results']
        if (result.get('returned_count') != width or result.get('has_more') is not False
                or len(rows) != width):
            raise RuntimeError('Nested read cardinality differs')
        for n, row in enumerate(rows):
            if row['key']['range'] != key(n) or row['fields'] != {
                    'bucket': 'measure', 'id': key(n), 'document': self.documents[n]}:
                raise RuntimeError('Nested query value/order differs')
        return elapsed

    def request(self, path, value=None):
        payload, elapsed = super().request(path, value)
        if path == '/api/query':
            for row in payload['results']:
                if set(row['metadata']) != set(row['fields']):
                    raise RuntimeError('Projection/metadata field sets differ')
                for metadata in row['metadata'].values():
                    if not isinstance(metadata.get('atom_uuid'), str):
                        raise RuntimeError('Atom metadata missing')
            if self.capture_name:
                raw = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
                self.fingerprints[self.capture_name] = hashlib.sha256(raw).hexdigest()
                save(self.output / f'response-{self.capture_name}.json', payload)
        return payload, elapsed

    def response_fingerprints(self):
        cases = [
            ('hash-point', 'hash', {'HashKey': key(7)}, [7], 1),
            ('range-point', 'range', {'HashRangeKey': {'hash': 'measure', 'range': key(7)}}, [7], 1),
            ('range-miss', 'range', {'HashRangeKey': {'hash': 'measure', 'range': key(COUNT)}}, [], 1),
            ('range-duplicates', 'range', {'HashRangeKeys': [['measure', key(n)]
                for n in (7, 4, 7, COUNT, 9, 4)]}, [4, 7, 9], 16),
            ('range64', 'range', {'HashRangeRange': {'hash': 'measure', 'start': key(128),
                'end': key(192)}}, list(range(128, 192)), 64),
            ('range512', 'range', {'HashKey': 'measure'}, list(range(COUNT)), COUNT)]
        for name, kind, selector, numbers, limit in cases:
            self.capture_name = name
            self.read(kind, selector, numbers, limit)
        for ordered in (False, True):
            self.capture_name = 'hidden-sort' if ordered else 'projection-desc'
            request = query('range', {'HashRangeKeys': [['measure', key(n)] for n in (7, 4, 9)]}, 16)
            request['fields'] = ['title']
            if ordered:
                request['order_by'] = {'field': 'id', 'order': 'desc'}
            else:
                request['sort_order'] = 'desc'
            result = self.call('/api/query', request)
            if ([row['key']['range'] for row in result['results']] != [key(n) for n in (9, 7, 4)]
                    or any(row['fields'] != {'bucket': 'measure', 'title': values(n, 0)['title']}
                           for row, n in zip(result['results'], (9, 7, 4)))):
                raise RuntimeError('Projected descending/hidden-sort values differ')
        for width in (16, NESTED_COUNT):
            self.capture_name = f'nested{width}'
            self.nested_read(width)
        self.capture_name = None
        save(self.output / 'response-fingerprints.json', self.fingerprints)

    def workloads(self):
        self.response_fingerprints()
        for width, count in ((16, 128), (64, 64), (256, 32)):
            samples = [self.nested_read(width) for _ in range(count)]
            phase = f'nested_range{width}'
            self.phases[phase] = stats(samples)
            self.samples.extend({'phase': phase, 'ms': value} for value in samples)
            print(f'{phase}: {self.phases[phase]["median_ms"]:.3f} ms median', flush=True)
        self.reads('range_multiget32', 'range', [({'HashRangeKeys': [
            ['measure', key(n)] for n in range(32)]}, list(range(32)), 32)] * 128)
        self.reads('range_prefix100', 'range', [({'HashRangePrefix': {
            'hash': 'measure', 'prefix': 'key-0001'}}, list(range(100, 200)), 128)] * 96)
        self.reads('range_partition512', 'range', [({'HashKey': 'measure'}, list(range(COUNT)), COUNT)] * 32)
        super().workloads()
        # Warm the affected Hash keys before a sustained single-write comparison.
        for n in range(COUNT):
            self.read('hash', {'HashKey': key(n)}, [n], 1)
        self.writes('hash_durable_write1', 'hash', 1, 256, 'durable', 12000)
        self.writes('range_durable_write32', 'range', 32, 64, 'durable', 14000)
        for kind in SCHEMAS:
            self.write(kind, [0, 1, 2], 0, 'durable', 'delete')
            self.write(kind, [3, 4], 0, 'durable', 'purge')
            self.read(kind, {'HashKey': key(0)} if kind == 'hash' else
                      {'HashRangeKeys': [['measure', key(n)] for n in range(6)]},
                      [0] if kind == 'hash' else list(range(6)), 8)
        self.stop()
        self.start()
        for start in range(0, COUNT, 32):
            numbers = list(range(start, start + 32))
            self.read('range', {'HashRangeKeys': [['measure', key(n)] for n in numbers]}, numbers, 32)
        for n in range(COUNT):
            self.read('hash', {'HashKey': key(n)}, [n], 1)
        self.nested_read(NESTED_COUNT)
        self.flush()


def execute(args, fixture=None, expected=None):
    bench = OwnedFormatBench(args)
    summary = {'complete': False}
    try:
        if fixture is None:
            bench.start()
            bench.install()
            bench.stop()
            summary['fixture_digest'] = fixture_digest(bench.home)
        else:
            shutil.copytree(fixture, bench.home, dirs_exist_ok=True, copy_function=shutil.copy2)
            observed = fixture_digest(bench.home)
            if observed != expected:
                raise RuntimeError('Cloned fixture differs before startup')
            inputs = json.loads((bench.output / 'inputs.json').read_text())
            inputs.update(experiment_driver_sha256=file_hash(Path(__file__)), fixture_digest=observed,
                          client_cpu=args.client_cpu, server_cpu=args.server_cpu)
            save(bench.output / 'inputs.json', inputs)
            bench.start()
            bench.workloads()
            summary.update(phases=bench.phases, values_metadata_and_reopen_verified=True,
                           response_fingerprints=bench.fingerprints)
        summary['complete'] = True
    except BaseException as error:
        summary['error'] = str(error)
        raise
    finally:
        bench.stop()
        save(bench.output / 'samples.json', bench.samples)
        summary.update(source_revision=args.source_revision, config=args.config,
                       all_owned_processes_stopped=bench.process is None)
        save(bench.output / 'summary.json', summary)
    return bench, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binaries', 'output', 'scratch', 'provenance'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=4)
    parser.add_argument('--client-cpu', type=int, default=0)
    parser.add_argument('--server-cpu', type=int, default=1)
    opts = parser.parse_args()
    if opts.client_cpu == opts.server_cpu or not {opts.client_cpu, opts.server_cpu} <= os.sched_getaffinity(0):
        raise RuntimeError('Need two distinct allowed CPUs')
    os.sched_setaffinity(0, {opts.client_cpu})
    opts.output.mkdir(parents=True, exist_ok=False)
    opts.scratch.mkdir(mode=0o700, parents=True, exist_ok=False)
    provenance = json.loads(opts.provenance.read_text())
    for label in ('before', 'after'):
        if file_hash(opts.binaries / label) != provenance['binaries'][label]:
            raise RuntimeError('Binary hash differs from declared provenance')
    def arguments(name, config, label):
        return argparse.Namespace(binary=opts.binaries / label, scratch=opts.scratch / name,
            output=opts.output / name, source_revision=provenance['revisions'][label], config=config,
            rustc=provenance['rustc'], client_cpu=opts.client_cpu, server_cpu=opts.server_cpu)
    lock_path = Path.home() / '.cache/fold-db-perf-guard/host-cargo.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fixtures, fingerprints, index = {}, {}, []
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for config in ('plain', 'encrypted', 'preload'):
            fixtures[config] = execute(arguments(f'fixture-{config}', config, 'before'))
            print(f'Fixture ready: {config}', flush=True)
        for pair in range(opts.rounds):
            for config in fixtures:
                order = ('before', 'after') if pair % 2 == 0 else ('after', 'before')
                for label in order:
                    name = f'{config}-{pair}-{label}'
                    print(f'Starting {name}', flush=True)
                    fixture, summary = fixtures[config]
                    started = time.time()
                    bench, result = execute(arguments(name, config, label), fixture.home,
                                            summary['fixture_digest'])
                    cleanup(bench.root, bench.output)
                    fingerprints.setdefault(config, result['response_fingerprints'])
                    if fingerprints[config] != result['response_fingerprints']:
                        raise RuntimeError('Full response fingerprint differs within configuration')
                    index.append(dict(name=name, config=config, pair=pair, label=label,
                                      source_revision=provenance['revisions'][label], seconds=time.time()-started))
                    save(opts.output / 'index.json', index)
        for fixture, summary in fixtures.values():
            assert fixture_digest(fixture.home) == summary['fixture_digest']
            cleanup(fixture.root, fixture.output)
    print('All owned-format measurements complete', flush=True)


if __name__ == '__main__':
    main()
