#!/usr/bin/env python3
"""Record additional ordinary Mini operations on a fresh synthetic home."""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import time

sys.dont_write_bytecode = True
import measure
import extra_loading
import extra_hash

measure.SCHEMA = 'SchemaPreloadExtraProduct'
measure.FIELDS = ['title', 'body', 'score']
SCHEMA, FIELDS = measure.SCHEMA, measure.FIELDS


class Run(measure.Run):
    def __init__(self, args):
        super().__init__(args)
        self.created = set()
        path = self.root / 'inputs.json'
        metadata = json.loads(path.read_text())
        metadata.update(extra_driver_sha256=measure.profile.file_hash(Path(__file__)),
                        score_recipe='hash number times 1000 plus range number',
                        extra_product_operations=True, operation_set=args.operation_set,
                        loading_first=args.loading_first,
                        extra_loading_sha256=measure.profile.file_hash(Path(extra_loading.__file__)),
                        extra_hash_sha256=measure.profile.file_hash(Path(extra_hash.__file__)))
        measure.profile.save(path, metadata)
        for source in [Path(__file__), Path(measure.__file__),
                       Path(extra_loading.__file__),
                       Path(extra_hash.__file__),
                       measure.SHARED / 'range_profile.py', measure.SHARED / 'ownership.py']:
            shutil.copy2(source, self.receipt_dir / source.name)

    def values(self, h, n):
        return dict(super().values(h, n), score=h * 1000 + n)

    def live(self, h, n):
        known = (0 <= h < self.args.hashes and 0 <= n < self.args.records_per_hash) or (h, n) in self.created
        return known and (h, n) not in self.absent


class Node(measure.Node):
    def call(self, path, value=None, cost=False):
        if not path.startswith('/api/db/compact'):
            return super().call(path, value, cost)
        with self.request_lock:
            self.sequence += 1
            request_id = f'extra-admin-{self.run.identity}-{self.sequence}'
        headers = {'Content-Type': 'application/json', 'X-LastDB-Client': 'schema-preload-extra-product',
                   'X-LastDB-Request-Id': request_id}
        connection = measure.profile.UnixConnection(self.socket)
        started = time.perf_counter_ns()
        try:
            connection.request('POST', path, json.dumps(value, separators=(',', ':')), headers)
            response = connection.getresponse()
            raw = response.read().decode('utf-8', errors='replace')
            elapsed = time.perf_counter_ns() - started
            try:
                result = json.loads(raw)
            except json.JSONDecodeError:
                self.remember(operation='admin-response', path=path, request=value,
                              request_id=request_id, http_status=response.status,
                              response_headers=dict(response.getheaders()), raw_body=raw,
                              elapsed_nanos=elapsed)
                raise RuntimeError(f'{path}: HTTP {response.status} returns a non-JSON body')
            self.remember(operation='request', path=path, request=value, request_id=request_id,
                          http_status=response.status, result=result, elapsed_nanos=elapsed)
            if response.status != 200:
                raise RuntimeError(f'{path}: HTTP {response.status}: {result}')
            return result, elapsed, None, request_id
        finally:
            connection.close()

    def seed(self):
        self.start()
        fields = ['bucket', 'id', *FIELDS]
        self.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': SCHEMA,
            'descriptive_name': 'Schema Preload Extra Product Operations',
            'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': fields, 'field_descriptions': {field: 'generated ' + field for field in fields}}})
        pairs = [(h, n) for h in range(self.run.args.hashes)
                 for n in range(self.run.args.records_per_hash)]
        for first in range(0, len(pairs), self.run.args.seed_batch):
            self.mutate(pairs[first:first + self.run.args.seed_batch], 'seed',
                        operation='create', durability='durable')
            self.flush()
        self.status('seed-complete')
        self.stop()

    def mutate(self, pairs, phase, operation='delete', durability='durable'):
        pairs = list(pairs)
        mutations = []
        for h, n in pairs:
            fields = {} if operation == 'delete' else dict(self.run.values(h, n))
            if operation == 'create':
                fields.update(bucket=measure.bucket(h), id=measure.key(n))
            mutations.append({'type': 'mutation', 'schema': SCHEMA,
                'mutation_type': operation, 'durability': durability,
                'key_value': {'hash': measure.bucket(h), 'range': measure.key(n)},
                'fields_and_values': fields})
        result, elapsed, _, request_id = self.call('/api/mutations/batch', {'mutations': mutations})
        actual = result.get('durability')
        if (result.get('count') != len(pairs) or actual not in ('queued', 'durable')
                or durability == 'durable' and actual != 'durable'):
            raise RuntimeError('The mutation batch lacks its complete acknowledgement')
        if operation == 'delete':
            self.run.absent.update(pairs)
        else:
            self.run.absent.difference_update(pairs)
            self.run.created.update(pair for pair in pairs if pair[1] >= self.run.args.records_per_hash)
        self.remember(operation='mutation', phase=phase, mutation_type=operation,
                      batch=len(pairs), elapsed_nanos=elapsed, request_id=request_id,
                      actual_durability=actual)

    def checked(self, selector, pairs, phase, **options):
        result, elapsed, cost, request_id = self.call('/api/query', {
            'schema_name': SCHEMA, 'fields': FIELDS, 'filter': selector, **options}, True)
        rows = result.get('results', [])
        wanted = {(measure.bucket(h), measure.key(n)): self.run.values(h, n)
                  for h, n in pairs if self.run.live(h, n)}
        actual = {(row['key']['hash'], row['key']['range']):
                  {field: row['fields'].get(field) for field in FIELDS} for row in rows}
        if len(rows) != len(wanted) or actual != wanted:
            raise RuntimeError(f'{phase}: returned keys or values differ')
        for row in rows:
            for field in FIELDS:
                if not row['metadata'][field].get('atom_uuid'):
                    raise RuntimeError(f'{phase}: atom identity is absent')
        if cost is None or result.get('returned_count') != len(wanted):
            raise RuntimeError(f'{phase}: read receipt or returned count differs')
        expected_total = options.get('expected_total_count')
        if expected_total is not None and result.get('total_count') != expected_total:
            raise RuntimeError(f'{phase}: exact total count differs')
        self.remember(operation='query', phase=phase, shape=phase, batch=len(wanted),
                      request_id=request_id, elapsed_nanos=elapsed, cost=cost,
                      expected_values_match=True, returned_records=len(wanted),
                      response_sha256=measure.profile.digest(result))
        return result

    def sample(self, phase):
        result, elapsed, cost, request_id = self.call('/api/query', {
            'schema_name': SCHEMA, 'fields': FIELDS, 'filter': {'SampleN': 10}, 'limit': 10}, True)
        rows, pairs = result.get('results', []), []
        for row in rows:
            h = int(row['key']['hash'].removeprefix('generated-'))
            n = int(row['key']['range'].split('#')[1])
            if not (0 <= h < self.run.args.hashes and 0 <= n < self.run.args.records_per_hash):
                raise RuntimeError(f'{phase}: sample returns a key outside the generated schema')
            actual = {field: row['fields'].get(field) for field in FIELDS}
            if not self.run.live(h, n) or actual != self.run.values(h, n):
                raise RuntimeError(f'{phase}: sample returns a deleted key or wrong value')
            pairs.append((h, n))
        if len(rows) != 10 or len(set(pairs)) != 10 or cost is None:
            raise RuntimeError(f'{phase}: sample lacks ten distinct live records or its receipt')
        self.remember(operation='query', phase=phase, shape='sample', batch=10,
                      request_id=request_id, elapsed_nanos=elapsed, cost=cost,
                      expected_values_match=True, returned_records=10,
                      selected_pairs=pairs, response_sha256=measure.profile.digest(result))
        return pairs


def same_ready(before, after, phase):
    left, right = before['schema_keys'], after['schema_keys']
    if left['loads'] != right['loads'] or right['ready'] != left['ready'] or right['loading']:
        raise RuntimeError(f'{phase}: the complete key domains unexpectedly reload or lose Ready')


def envelope_operations(node):
    pair = (0, 20)
    selector = {'HashRangeKey': {'hash': measure.bucket(0), 'range': measure.key(20)}}
    node.flush()
    result, elapsed, _, request_id = node.call('/api/db/compact-record', {
        'schema': SCHEMA, 'hash': measure.bucket(0), 'range': measure.key(20)})
    report = result.get('data', result)
    if (report.get('keys_compacted') != 1 or report.get('keys_skipped') != 0
            or not report.get('record_molecule_uuid')):
        raise RuntimeError('The record compact request did not produce one complete record envelope')
    node.remember(operation='record-compaction', phase='complete-record-envelope',
                  request_id=request_id, elapsed_nanos=elapsed, result=result)
    first = node.checked(selector, [pair], 'record-envelope-first', limit=1)
    before = node.status('record-envelope-warm-before')
    second = node.checked(selector, [pair], 'record-envelope-repeated', limit=1)
    cost = node.run.rows[-1]['cost']
    after = node.status('record-envelope-warm-after')
    if (cost.get('logical_warm_hits', 0) < 1
            or after['core_warm_hits'] <= before['core_warm_hits']):
        raise RuntimeError('The repeated complete record envelope lacks a core warm hit')
    for response in (first, second):
        identities = {entry['molecule_uuid'] for entry in response['results'][0]['metadata'].values()}
        if identities != {report['record_molecule_uuid']}:
            raise RuntimeError('The complete envelope metadata does not identify the record molecule')
    node.run.revisions[pair] = 1
    node.mutate([pair], 'record-envelope-update', operation='update', durability='queued')
    node.checked(selector, [pair], 'record-envelope-update-ack', limit=1)
    node.flush()
    node.checked(selector, [pair], 'record-envelope-update-flush', limit=1)


def workload(node):
    node.seed()
    node.start()
    extra_hash.operations(node)
    count = node.run.args.records_per_hash
    node.checked({'HashRangeKeys': [[measure.bucket(0), measure.key(0)]]}, [(0, 0)],
                 'first-exact-preload', limit=1, expected_total_count=1)
    ready = node.status('preloaded')
    node.checked({'HashKey': measure.bucket(0)}, [(0, n) for n in range(5, 12)],
                 'offset-page', limit=7, offset=5, expected_total_count=count)
    page = node.checked({'HashKey': measure.bucket(0)}, [(0, n) for n in range(7)],
                        'cursor-page-one', limit=7, expected_total_count=count)
    cursor = page.get('next_cursor')
    if not cursor or page.get('has_more') is not True:
        raise RuntimeError('The first cursor page lacks its next cursor')
    node.checked({'HashKey': measure.bucket(0)}, [(0, n) for n in range(7, 14)],
                 'cursor-page-two', limit=7, cursor=cursor, expected_total_count=count)
    node.checked({'HashRangePrefix': {'hash': measure.bucket(0), 'prefix': 'todo#0000001'}},
                 [(0, n) for n in range(10, 20)], 'range-prefix', limit=10,
                 expected_total_count=10)
    node.checked({'HashRangeRange': {'hash': measure.bucket(0), 'start': measure.key(10),
                                     'end': measure.key(15)}}, [(0, n) for n in range(10, 15)],
                 'end-exclusive-range', limit=5, expected_total_count=5)
    node.checked({'HashRangeRange': {'hash': measure.bucket(0), 'start': measure.key(0),
                                     'end': measure.key(count)}},
                 [(0, n) for n in range(10, 15)], 'numeric-value-filter', limit=16,
                 value_filters=[{'Between': {'field': 'score', 'min': 10, 'max': 14}}])
    envelope_operations(node)
    leading = node.sample('sample-before-deletes')
    ready = node.status('sample-ready-before-deletes')
    node.mutate(leading, 'leading-sample-delete')
    node.sample('sample-after-leading-deletes')
    middle = [(0, count // 2)]
    node.mutate(middle, 'middle-delete')
    node.sample('sample-after-middle-delete')
    live = [(0, n) for n in range(count) if node.run.live(0, n)]
    node.checked({'HashKey': measure.bucket(0)}, live[:9], 'count-after-deletes',
                 limit=9, expected_total_count=len(live))
    node.flush()
    before = node.status('before-physical-compaction')
    same_ready(ready, before, 'ordinary-deletes')
    result, elapsed, _, request_id = node.call('/api/db/compact', {'dry_run': False, 'grace_ms': 0})
    native = result.get('data', result)['compact']['native_disk']
    if native.get('dry_run') is not False:
        raise RuntimeError('The native compact request did not execute')
    node.remember(operation='physical-compaction', phase='physical-compaction',
                  elapsed_nanos=elapsed, request_id=request_id, result=result)
    after = node.status('after-physical-compaction')
    same_ready(before, after, 'physical-compaction')
    node.checked({'HashKey': measure.bucket(0)}, live[:9], 'range-after-compaction',
                 limit=9, expected_total_count=len(live))
    node.sample('sample-after-compaction')
    same_ready(after, node.status('final'), 'post-compaction-queries')



def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--operation-set', choices=['ordinary', 'loading-write'], default='ordinary')
    parser.add_argument('--loading-first', choices=['mixed', 'put'], default='mixed')
    extra, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    args = measure.arguments()
    args.operation_set = extra.operation_set
    args.loading_first = extra.loading_first
    policy = args.extra_env.get('LASTDB_SCHEMA_PRELOAD_SCHEMAS', '')
    if args.operation_set == 'ordinary' and policy and '*' not in policy.split(','):
        args.extra_env['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = policy + ',' + extra_hash.SCHEMA
    if args.records_per_hash < 32:
        raise ValueError('Extra product operations require at least 32 records per hash')
    run, node, complete, error = Run(args), None, False, None
    names = {'LASTDB_RESIDENT_KEY_CAP': str(args.cap), **args.extra_env}
    previous = {name: os.environ.get(name) for name in names}
    try:
        os.environ.update(names)
        node = Node(run)
        (extra_loading.loading_write if args.operation_set == 'loading-write' else workload)(node)
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
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        run.finish(complete, error)
        if node is not None and (node.directory / 'node.log').is_file():
            shutil.copy2(node.directory / 'node.log', run.receipt_dir / 'node.log')
    print(json.dumps({'complete': complete, 'error': error, 'root': str(run.root),
                      'receipt_dir': str(run.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
