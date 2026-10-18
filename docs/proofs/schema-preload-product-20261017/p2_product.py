#!/usr/bin/env python3
"""Record ordinary Mini values, warm reuse, and distinct physical atom times."""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import time
from types import SimpleNamespace

sys.dont_write_bytecode = True
import measure

SCHEMA = 'WarmSecondaryPhysicalSource'
FIELDS = ['title', 'body']
COMMON = {'title': 'same title in two hashes', 'body': 'same body in two hashes'}
RANGE = 'record-01'
measure.SCHEMA = SCHEMA


class Run(measure.Run):
    def __init__(self, args):
        super().__init__(args)
        path = self.root / 'inputs.json'
        inputs = json.loads(path.read_text())
        inputs.update(p2_driver_sha256=measure.profile.file_hash(Path(__file__)),
                      expected_binary_sha256=args.expected_sha256,
                      environment={'LASTDB_ATOM_KEY_ENCODING': 'partition_prefix',
                                   'LASTDB_SCHEMA_PRELOAD_SCHEMAS': SCHEMA,
                                   'LASTDB_WRITE_DEDUPE': '0'},
                      timestamp_observation='GET /api/atom exposes created_at; query omits written_at',
                      source_observation='Mini mutation routes discard source_file_name; query returns null',
                      exact_collision_metadata_result='unobservable through ordinary Mini query API')
        measure.profile.save(path, inputs)
        for source in [Path(__file__), Path(measure.__file__),
                       measure.SHARED / 'range_profile.py', measure.SHARED / 'ownership.py']:
            shutil.copy2(source, self.receipt_dir / source.name)


class Node(measure.Node):
    def setup(self):
        self.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': SCHEMA,
            'descriptive_name': 'Warm secondary physical atom source',
            'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': ['bucket', 'id', *FIELDS],
            'field_descriptions': {name: 'generated ' + name for name in ['bucket', 'id', *FIELDS]}}})

    def mutate(self, bucket, operation='create', values=None, source=None, range_key=RANGE):
        fields = {} if operation == 'delete' else dict(COMMON if values is None else values)
        if operation == 'create':
            fields.update(bucket=bucket, id=range_key)
        mutation = {'type': 'mutation', 'schema': SCHEMA, 'mutation_type': operation,
                    'durability': 'durable', 'key_value': {'hash': bucket, 'range': range_key},
                    'fields_and_values': fields}
        if source is not None:
            mutation['source_file_name'] = source
        result, _, _, _ = self.call('/api/mutations/batch', {'mutations': [mutation]})
        if result.get('count') != 1 or result.get('durability') != 'durable':
            raise RuntimeError('The product mutation lacks its durable acknowledgement')

    def query(self, bucket, phase, expected=None, selector=None, count=1, source=None):
        result, elapsed, cost, request_id = self.call('/api/query', {
            'schema_name': SCHEMA, 'fields': FIELDS,
            'filter': selector or {'HashRangeKeys': [[bucket, RANGE]]}, 'limit': 8}, True)
        rows = result.get('results', [])
        if len(rows) != count or result.get('returned_count') != count or cost is None:
            raise RuntimeError(phase + ': product row count or read receipt differs')
        if count == 1:
            row = rows[0]
            expected = COMMON if expected is None else expected
            if {name: row['fields'].get(name) for name in FIELDS} != expected:
                raise RuntimeError(phase + ': product values differ')
            for name in FIELDS:
                meta = row['metadata'][name]
                if not meta.get('atom_uuid') or meta.get('source_file_name') != source:
                    raise RuntimeError(phase + ': atom identity or physical source differs')
        self.remember(operation='query', phase=phase, shape='ordinary-co-key', batch=count,
                      request_id=request_id, elapsed_nanos=elapsed, cost=cost,
                      response_sha256=measure.profile.digest(result), expected_values_match=True)
        return rows


def identities(row):
    return {field: {key: row['metadata'][field].get(key)
                    for key in ['atom_uuid', 'source_file_name', 'metadata']}
            for field in FIELDS}


def atom_times(node, row):
    observed = {}
    for field in FIELDS:
        atom = row['metadata'][field]['atom_uuid']
        result, _, _, _ = node.call('/api/atom/' + atom)
        if result.get('source_file_name') is not None or result.get('content') != COMMON[field]:
            raise RuntimeError('The UUID atom route does not select the expected physical copy')
        observed[field] = result['created_at']
    return observed


def workload(run, node):
    node.start()
    node.setup()
    node.mutate('copy-A')
    node.mutate('copy-A', 'update', {'title': 'temporary title', 'body': 'temporary body'})
    node.mutate('copy-A', 'update')
    a_initial = node.query('copy-A', 'A-before-B')[0]
    times_a = atom_times(node, a_initial)
    time.sleep(.05)
    node.mutate('copy-B')
    b_initial = node.query('copy-B', 'B-after-create')[0]
    times_b = atom_times(node, b_initial)
    for field in FIELDS:
        if (a_initial['metadata'][field]['atom_uuid'] != b_initial['metadata'][field]['atom_uuid']
                or times_a[field] == times_b[field]):
            raise RuntimeError('The two product writes do not establish distinct copies of one UUID')
    node.flush_phase('two-physical-copies')
    node.stop()
    node.start()
    cold = node.query('copy-A', 'A-cold-after-B')[0]
    for repeat in range(4):
        repeated = node.query('copy-A', 'A-warm-repeat-' + str(repeat))[0]
        if identities(repeated) != identities(cold):
            raise RuntimeError('The repeated warm query changes the selected atom metadata')
        if run.rows[-1]['cost'].get('logical_warm_hits', 0) < 2:
            raise RuntimeError('The repeated query does not report the two selected warm fields')
    node.remember(operation='physical-source-proof', atom_created_at_A=times_a,
                  atom_created_at_B=times_b, first_A=identities(cold), repeated_A_equal=True,
                  same_uuid_distinct_created_at=True, written_at_query_check='not exposed by API',
                  exact_collision_metadata_result='unobservable through ordinary Mini query API')
    node.mutate('small-values', values={'title': '', 'body': 'small body'})
    node.query('small-values', 'small-empty-value', {'title': '', 'body': 'small body'})
    for number in range(2):
        node.mutate('small-range', values={'title': str(number), 'body': 'range body'},
                    range_key='range-' + str(number))
    selector = {'HashRangeRange': {'hash': 'small-range', 'start': 'range-', 'end': 'range.'}}
    rows = node.query('small-range', 'range-before-delete', selector=selector, count=2)
    if [row['key']['range'] for row in rows] != ['range-0', 'range-1']:
        raise RuntimeError('The ordinary range does not preserve key order')
    node.mutate('small-range', 'delete', range_key='range-0')
    node.query('small-range', 'range-after-delete', {'title': '1', 'body': 'range body'},
               selector=selector, source=None)
    node.flush_phase('small-operations')
    node.stop()
    node.start()
    reopened = node.query('copy-A', 'A-normal-reopen')[0]
    if identities(reopened) != identities(cold):
        raise RuntimeError('A normal reopen changes the selected atom metadata')
    node.query('small-range', 'deleted-range-normal-reopen', {'title': '1', 'body': 'range body'},
               selector=selector, source=None)
    node.status('final')
    measure.profile.save(run.root / 'p2-result.json', {
        'same_uuid': {field: cold['metadata'][field]['atom_uuid'] for field in FIELDS},
        'created_at_A': times_a, 'created_at_B': times_b,
        'cold_and_repeated_source': None, 'warm_repeats': 4,
        'small_value_range_delete_flush_reopen': True,
        'exact_collision_metadata_result': 'unobservable through ordinary Mini query API',
        'written_at': 'The ordinary query API does not expose this field; no direct timestamp assertion.'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    options = parser.parse_args()
    if measure.profile.file_hash(options.binary) != options.expected_sha256:
        raise RuntimeError('The supplied binary differs from the reviewed program hash')
    args = SimpleNamespace(**vars(options), variant='bounded-p2-fixed', root=None,
        source_commit='review fixes on current worktree', records_per_hash=1, hashes=2,
        body_bytes=128, seed_batch=1, read_rounds=4, cap=10000, counts=[1],
        concurrent_width=1, extra_env={'LASTDB_SCHEMA_PRELOAD_SCHEMAS': SCHEMA}, reads_only=False)
    names = {'LASTDB_ATOM_KEY_ENCODING': 'partition_prefix',
             'LASTDB_SCHEMA_PRELOAD_SCHEMAS': SCHEMA, 'LASTDB_WRITE_DEDUPE': '0'}
    previous = {name: os.environ.get(name) for name in names}
    run, node, complete, error = None, None, False, None
    try:
        os.environ.update(names)
        run = Run(args)
        node = Node(run)
        workload(run, node)
        complete = True
    except Exception as exc:
        error = type(exc).__name__ + ': ' + str(exc)
        if run is not None:
            run.remember({'operation': 'failure', 'error': error})
    finally:
        if node is not None:
            try:
                node.stop()
            except Exception as exc:
                complete, error = False, str(exc)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if run is not None:
            run.finish(complete, error)
            for path in [run.root / 'p2-result.json', node.directory / 'node.log']:
                if path.exists():
                    shutil.copy2(path, run.receipt_dir / path.name)
    print(json.dumps({'complete': complete, 'error': error,
                      'root': None if run is None else str(run.root)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
