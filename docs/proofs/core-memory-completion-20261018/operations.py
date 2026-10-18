#!/usr/bin/env python3
"""Exercise ordinary memory-state operations on one fresh generated Mini home."""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
PRIOR = HERE.parent / 'core-memory-state-20261018'
sys.path.insert(0, str(PRIOR))
import operational as previous
from api import Node, measure
from driver import ENVIRONMENT
import hash_all


def read_gate(node, kind, expected, phase, singular=False):
    return previous.cost_gate(node, kind, expected, phase, singular=singular)


def absent_then_insert(node):
    for kind in ('hash', 'hashrange'):
        absent, number = {10000: None}, 10000
        previous.query(node, kind, absent, 'missing-key-first')
        for repeat in range(3):
            read_gate(node, kind, absent, f'missing-key-repeat-{repeat}')
        previous.mutate(node, kind, [number], 11, 'create')
        previous.query(node, kind, {number: 11}, 'insert-after-warm-absence')
        read_gate(node, kind, {number: 11}, 'insert-after-warm-absence-repeat')
        node.flush()
        previous.mutate(node, kind, [number], 0, 'delete')
        previous.query(node, kind, absent, 'delete-after-warm-value')
        read_gate(node, kind, absent, 'delete-after-warm-value-repeat')
        node.flush()
        read_gate(node, kind, absent, 'delete-absence-after-flush')


def unchanged_after_writes(node):
    for kind, revision in (('hash', 1), ('hashrange', 5)):
        expected = {0: revision}
        previous.query(node, kind, expected, 'unchanged-prime')
        read_gate(node, kind, expected, 'unchanged-repeat-before-neighbor-update')
        previous.mutate(node, kind, [1], 20)
        read_gate(node, kind, expected, 'unchanged-immediately-after-neighbor-update')
        node.flush()
        read_gate(node, kind, expected, 'unchanged-after-neighbor-update-flush')
        previous.mutate(node, kind, [2], 0, 'delete')
        read_gate(node, kind, expected, 'unchanged-immediately-after-neighbor-delete')
        node.flush()
        read_gate(node, kind, expected, 'unchanged-after-neighbor-delete-flush')


def shared_write(node, kind, number):
    values = previous.field_values(number, 0)
    values.update(title='equal-content-generated-title', body='equal-content-generated-body')
    mutation = {'type': 'mutation', 'schema': previous.SCHEMAS[kind],
        'mutation_type': 'create', 'durability': 'durable',
        'key_value': previous.key(kind, number), 'fields_and_values': values}
    result, _, _, request = node.call('/api/mutations/batch', {'mutations': [mutation]})
    if result.get('count') != 1 or result.get('durability') != 'durable':
        raise RuntimeError('The equal-content record lacks a durable acknowledgement')
    node.remember(operation='completion-shared-write', geometry=kind, number=number,
        request_id=request, all_fields=values)


def shared_read(node, kind, number, phase, warm=False):
    selector = {'HashKey': measure.key(number)} if kind == 'hash' else {
        'HashRangeKey': {'hash': previous.HASH, 'range': measure.key(number)}}
    result, elapsed, cost, request = node.call('/api/query', {
        'schema_name': previous.SCHEMAS[kind], 'fields': ['title', 'body'],
        'filter': selector, 'limit': 1}, True)
    rows = result.get('results', [])
    if len(rows) != 1 or previous.coordinates(rows[0]['key']) != previous.coordinates(previous.key(kind, number)):
        raise RuntimeError('The equal-content keyed response differs')
    expected = {'title': 'equal-content-generated-title', 'body': 'equal-content-generated-body'}
    if any(rows[0]['fields'].get(field) != value for field, value in expected.items()):
        raise RuntimeError('The equal-content fields differ')
    if result.get('returned_count') != 1 or result.get('has_more') is not False:
        raise RuntimeError('The equal-content response is incomplete')
    if warm and (cost is None or cost.get('read_operations') != 0):
        raise RuntimeError('An alternating equal-content warm read needs a file read: ' + str(cost))
    identities = {field: rows[0]['metadata'][field]['atom_uuid'] for field in expected}
    node.remember(operation='completion-shared-read', geometry=kind, number=number,
        phase=phase, warm_gate=warm, request_id=request, elapsed_nanos=elapsed,
        cost=cost, atom_uuids=identities, returned_metadata=rows[0]['metadata'],
        all_values_and_membership_match=True)
    return identities


def equal_content_copies(node):
    for kind in ('hash', 'hashrange'):
        for number in (5000, 5001):
            shared_write(node, kind, number)
            node.flush()
        node.stop()
        node.start()
        identities = [shared_read(node, kind, number, 'equal-content-first')
                      for number in (5000, 5001)]
        if identities[0] != identities[1]:
            raise RuntimeError('The generated equal content does not share atom UUIDs')
        for repeat in range(6):
            for number in (5000, 5001):
                shared_read(node, kind, number, f'alternating-{repeat}', warm=True)
        node.remember(operation='completion-shared-copy-limit', geometry=kind,
            same_content_atom_uuids=identities[0],
            limit='Normal query responses omit atom creation time. API mutation components do not expose arbitrary atom metadata. Source review must assess exact physical-copy metadata preservation.')


def constant_membership_allowance(node, rounds):
    kind, number = 'hashrange', 96
    previous.query(node, kind, {0: 5}, 'churn-key-preload-prime')
    charged, observations = None, []
    for repeat in range(rounds):
        previous.mutate(node, kind, [number], 30 + repeat, 'create', durability='durable')
        previous.query(node, kind, {number: 30 + repeat}, f'churn-created-{repeat}')
        previous.mutate(node, kind, [number], 0, 'delete', durability='durable')
        node.flush()
        previous.query(node, kind, {number: None}, f'churn-deleted-{repeat}')
        native = node.status(f'constant-membership-{repeat}')
        keys = native['schema_keys']
        if keys['ready'] == 0 or keys['disk_only'] or keys['failed']:
            raise RuntimeError('The selected schema loses complete-key readiness during same-key churn')
        observation = {name: keys[name] for name in ('bytes', 'keys', 'domains', 'ready', 'loads')}
        observations.append(observation)
        if charged is None:
            charged = observation
        elif observation != charged:
            raise RuntimeError('The same complete-key membership changes its allowance or reload count: '
                               + json.dumps(observations))
    node.remember(operation='completion-constant-membership', rounds=rounds,
        observations=observations, charged_bytes_stable=True,
        scope='Each observation follows the same-key create, delete and explicit flush. This is a charged allowance, not total process memory.')


def prior_coverage(node):
    previous.prepare(node)
    for kind in ('hash', 'hashrange'):
        expected = previous.warm_and_change(node, kind)
        expected = previous.membership(node, kind, expected)
        previous.schema_map_change(node, kind, expected)
    previous.concurrent_batches(node)
    previous.schema_lifecycle(node)


def preserve(run, source, sha):
    if measure.profile.file_hash(run.binary) != sha:
        raise RuntimeError('The private executable identity differs')
    paths = [Path(previous.__file__), PRIOR / 'api.py', PRIOR / 'driver.py',
             Path(measure.__file__), measure.SHARED / 'range_profile.py',
             measure.SHARED / 'ownership.py', Path(__file__), Path(hash_all.__file__)]
    identities = {str(path.relative_to(HERE.parents[2])): measure.profile.file_hash(path)
                  for path in paths}
    shutil.copy2(source, run.receipt_dir / 'source-identity.json')
    shutil.copy2(Path(__file__), run.receipt_dir / 'operations.py')
    shutil.copy2(Path(hash_all.__file__), run.receipt_dir / 'hash_all.py')
    measure.profile.save(run.receipt_dir / 'driver-inputs.json', {
        'driver_sources_sha256': identities, 'binary_sha256': sha,
        'source_identity_sha256': measure.profile.file_hash(source),
        'oversized_definition_limit': 'No product API permits direct cache insertion. The impossible-admission branch receives a source review, not a test harness.',
        'total_process_memory_limit': 'This proof observes charged allowance counters, not the total process memory.'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--churn-rounds', type=int, default=24, choices=range(3, 129))
    own = parser.parse_args()
    if measure.profile.file_hash(own.binary) != own.sha256:
        raise RuntimeError('The executable identity differs')
    args = SimpleNamespace(binary=own.binary, variant='completion-current', root=None,
        receipt_dir=own.receipt_dir, source_commit='frozen-' + own.sha256,
        records_per_hash=64, hashes=4, body_bytes=1024, cap=10000, seed_batch=64,
        read_rounds=6, counts=[16, 64], concurrent_width=16, extra_env={}, reads_only=False)
    environment = dict(ENVIRONMENT, LASTDB_SCHEMA_PRELOAD_SCHEMAS=previous.SCHEMAS['hashrange'])
    saved = {name: os.environ.get(name) for name in environment}
    run, node, complete, error = None, None, False, None
    try:
        os.environ.update(environment)
        run = measure.Run(args)
        preserve(run, own.source, own.sha256)
        node = Node(run)
        prior_coverage(node)
        absent_then_insert(node)
        unchanged_after_writes(node)
        equal_content_copies(node)
        constant_membership_allowance(node, own.churn_rounds)
        node.flush()
        node.stop()
        node.start()
        for kind in ('hash', 'hashrange'):
            shared_read(node, kind, 5000, 'normal-reopen-equal-content')
        hash_all.run(node)
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
                complete, error = False, (error or '') + '; normal stop: ' + str(exc)
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if run is not None:
            run.finish(complete, error)
            if node is not None and (node.directory / 'node.log').is_file():
                shutil.copy2(node.directory / 'node.log', run.receipt_dir / 'node.log')
            counts = {name: sum(row.get('operation') == name for row in run.rows) for name in (
                'memory-state-read', 'memory-state-warm-storage-gate', 'completion-shared-read',
                'completion-constant-membership', 'completion-hash-all-read')}
            measure.profile.save(run.receipt_dir / 'result.json', {
                'complete': complete, 'error': error, 'operation_counts': counts})
    print(json.dumps({'complete': complete, 'error': error,
        'root': None if run is None else str(run.root), 'receipt_dir': str(own.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
