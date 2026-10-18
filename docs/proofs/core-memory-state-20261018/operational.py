#!/usr/bin/env python3
"""Record ordinary keyed operations on a new generated memory-state home."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import shutil
import sys
import threading
import time
from types import SimpleNamespace

sys.dont_write_bytecode = True
from api import Node, measure
from driver import ENVIRONMENT

HERE = Path(__file__).resolve().parent
FIELDS = ['bucket', 'id', 'title', 'body']
HASH = 'memory-state-generated-hash'
SCHEMAS = {kind: 'MemoryState' + kind.title() for kind in ('hash', 'hashrange')}


def field_values(number, revision):
    return {'bucket': HASH, 'id': measure.key(number),
            'title': f'memory-state-{number}-revision-{revision}',
            'body': measure.profile.body(number + revision * 1000000, 1024)}


def key(kind, number):
    return {'hash': measure.key(number)} if kind == 'hash' else {
        'hash': HASH, 'range': measure.key(number)}


def coordinates(value):
    return value['hash'], value.get('range')


def install(node, kind, donor=False):
    name = SCHEMAS[kind] + ('Donor' if donor else '')
    node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
        'source': 'system_seed', 'name': name, 'descriptive_name': name,
        'schema_type': 'Hash' if kind == 'hash' else 'HashRange',
        'key': {'hash_field': 'id'} if kind == 'hash' else
               {'hash_field': 'bucket', 'range_field': 'id'},
        'fields': FIELDS,
        'field_descriptions': {field: name + ' independent ' + field for field in FIELDS}}})
    return name


def mutate(node, kind, numbers, revision, operation='update', donor=False, durability='queued'):
    mutations = []
    for number in numbers:
        values = {} if operation == 'delete' else field_values(number, revision)
        if operation == 'update':
            values = {field: values[field] for field in ('title', 'body')}
        mutations.append({'type': 'mutation', 'schema': SCHEMAS[kind] + ('Donor' if donor else ''),
            'mutation_type': operation, 'durability': durability,
            'key_value': key(kind, number), 'fields_and_values': values})
    result, elapsed, _, request = node.call('/api/mutations/batch', {'mutations': mutations})
    if result.get('count') != len(numbers) or result.get('durability') not in ('queued', 'durable'):
        raise RuntimeError('The complete generated mutation lacks an acknowledgement')
    node.remember(operation='memory-state-mutation', geometry=kind, request_id=request,
        revision=revision, numbers=list(numbers), mutation_type=operation,
        requested_mode=durability, returned_mode=result['durability'], elapsed_nanos=elapsed)
    return request


def query(node, kind, expected, phase, bounded=False, allowed_revisions=None, singular=False):
    numbers = list(expected)
    if kind == 'hash':
        if len(numbers) != 1:
            raise RuntimeError('The ordinary Hash proof supplies one known hash')
        selector = {'HashKey': measure.key(numbers[0])}
    elif singular:
        if len(numbers) != 1:
            raise RuntimeError('The singular HashRangeKey proof supplies one key pair')
        selector = {'HashRangeKey': {'hash': HASH, 'range': measure.key(numbers[0])}}
    elif bounded:
        selector = {'HashRangeRange': {'hash': HASH, 'start': measure.key(0),
                                      'end': measure.key(256)}}
    else:
        selector = {'HashRangeKeys': [[HASH, measure.key(number)] for number in numbers]}
    log_start = (node.directory / 'node.log').stat().st_size
    result, elapsed, cost, request = node.call('/api/query', {
        'schema_name': SCHEMAS[kind], 'fields': FIELDS, 'filter': selector, 'limit': 256}, True)
    rows, seen, revisions = result.get('results', []), set(), set()
    wanted = set(number for number, revision in expected.items() if revision is not None)
    for row in rows:
        matched = [number for number in numbers
                   if coordinates(row['key']) == coordinates(key(kind, number))]
        if len(matched) != 1 or matched[0] in seen:
            raise RuntimeError('The keyed response has unknown or repeated membership')
        number = matched[0]
        seen.add(number)
        candidates = allowed_revisions if allowed_revisions is not None else [expected[number]]
        equal = [revision for revision in candidates if revision is not None and
                 row['fields'] == field_values(number, revision)]
        if len(equal) != 1:
            raise RuntimeError('A generated record has mixed fields or an unexpected revision')
        revisions.add(equal[0])
        if any(not row['metadata'][field].get('atom_uuid') or
               not row['metadata'][field].get('molecule_uuid') for field in FIELDS):
            raise RuntimeError('A field lacks an atom or molecule identity')
    if seen != wanted or len(rows) != len(wanted) or result.get('returned_count') != len(wanted):
        raise RuntimeError('The generated keyed response membership differs')
    if result.get('has_more') is not False or len(revisions) > 1:
        raise RuntimeError('The response is incomplete or mixes committed batch revisions')
    if bounded and [row['key']['range'] for row in rows] != sorted(measure.key(n) for n in wanted):
        raise RuntimeError('The bounded result order differs')
    node.remember(operation='memory-state-read', geometry=kind, bounded=bounded, singular=singular, phase=phase,
        request_id=request, elapsed_nanos=elapsed, cost=cost, revisions=sorted(revisions),
        all_fields_and_membership_match=True, expected_count=len(wanted),
        node_log_start=log_start, node_log_end=(node.directory / 'node.log').stat().st_size)
    return cost


def cost_gate(node, kind, expected, phase, bounded=False, singular=False):
    cost = query(node, kind, expected, phase, bounded, singular=singular)
    reads = None if cost is None else cost.get('read_operations')
    node.remember(operation='memory-state-warm-storage-gate', phase=phase,
        read_operations=reads, zero_storage_read_operations=reads == 0,
        scope='Receipt read_operations counts file calls; OS cache state is uncontrolled.')
    if reads != 0:
        raise RuntimeError('The fully warm generated query still performs storage reads: ' + str(cost))
    if kind == 'hashrange' and not singular:
        number = next(iter(expected))
        one = {number: expected[number]}
        query(node, kind, one, phase + '-singular-prime', singular=True)
        cost_gate(node, kind, one, phase + '-singular-warm', singular=True)


def prepare(node):
    node.start()
    for kind in ('hash', 'hashrange'):
        for donor, numbers in ((False, range(64)), (True, range(128, 192))):
            install(node, kind, donor)
            mutate(node, kind, list(numbers), 0, 'create', donor, 'durable')
            node.flush()
    node.stop()
    node.start()


def warm_and_change(node, kind):
    expected = {number: 0 for number in (range(64) if kind == 'hashrange' else [0])}
    query(node, kind, expected, 'first-normal-reopen', kind == 'hashrange')
    cost_gate(node, kind, expected, 'fully-warm-before-update', kind == 'hashrange')
    mutate(node, kind, list(expected), 1)
    node.status(kind + '-accepted-update-before-explicit-flush')
    expected = dict.fromkeys(expected, 1)
    query(node, kind, expected, 'accepted-update-before-explicit-flush', kind == 'hashrange')
    cost_gate(node, kind, expected, 'warm-accepted-update', kind == 'hashrange')
    node.flush()
    cost_gate(node, kind, expected, 'warm-after-flush', kind == 'hashrange')
    node.stop()
    node.start()
    query(node, kind, expected, 'normal-reopen-after-update', kind == 'hashrange')
    cost_gate(node, kind, expected, 'warm-normal-reopen-after-update', kind == 'hashrange')
    return expected


def membership(node, kind, expected):
    query(node, kind, {96: None}, 'warm-absence-before-insert')
    if kind == 'hashrange':
        mutate(node, kind, list(expected), 2)
        expected = dict.fromkeys(expected, 2)
    inserted_revision = 2 if kind == 'hashrange' else 0
    mutate(node, kind, [96], inserted_revision, 'create')
    query(node, kind, {96: inserted_revision}, 'accepted-insert-after-warm-absence')
    if kind == 'hashrange':
        expected[96] = inserted_revision
        query(node, kind, expected, 'bounded-after-accepted-insert', True)
    mutate(node, kind, [96], 0, 'delete')
    query(node, kind, {96: None}, 'accepted-delete-removes-warm-value')
    expected.pop(96, None)
    if kind == 'hashrange':
        query(node, kind, expected, 'bounded-after-accepted-delete', True)
    node.flush()
    query(node, kind, expected, 'first-after-membership-flush', kind == 'hashrange')
    cost_gate(node, kind, expected, 'warm-membership-after-flush', kind == 'hashrange')
    return expected


def schema_map_change(node, kind, expected):
    original, _, _, _ = node.call('/api/db/repair-schema-molecule-map', {'schema': SCHEMAS[kind]})
    donor, _, _, _ = node.call('/api/db/repair-schema-molecule-map', {'schema': SCHEMAS[kind] + 'Donor'})
    original, donor = original.get('data', original), donor.get('data', donor)
    original_map, donor_map = [row['current_field_molecule_uuids'] for row in (original, donor)]
    if not all(original_map[field] != donor_map[field] for field in FIELDS):
        raise RuntimeError('The generated schemas do not have independent physical molecules')
    node.call('/api/db/repair-schema-molecule-map', {'schema': SCHEMAS[kind], 'execute': True,
        'expected_current_fingerprint': original['current_fingerprint'],
        'field_molecule_uuids': donor_map})
    donor_expected = {number: 0 for number in (range(128, 192) if kind == 'hashrange' else [128])}
    query(node, kind, donor_expected, 'after-schema-molecule-map-change', kind == 'hashrange')
    query(node, kind, {0: None}, 'old-key-absent-after-schema-molecule-map-change')
    cost_gate(node, kind, donor_expected, 'warm-after-schema-molecule-map-change', kind == 'hashrange')
    node.call('/api/db/repair-schema-molecule-map', {'schema': SCHEMAS[kind], 'execute': True,
        'expected_current_fingerprint': donor['current_fingerprint'],
        'field_molecule_uuids': original_map})
    query(node, kind, expected, 'after-original-molecule-map-restored', kind == 'hashrange')
    cost_gate(node, kind, expected, 'warm-after-original-molecule-map-restored', kind == 'hashrange')
    node.flush()
    node.stop()
    node.start()
    query(node, kind, expected, 'normal-reopen-after-original-molecule-map-restored', kind == 'hashrange')


def concurrent_batches(node):
    numbers = list(range(64))
    for revision in (3, 4, 5):
        barrier = threading.Barrier(4)
        def write():
            barrier.wait(timeout=30)
            return mutate(node, 'hashrange', numbers, revision)
        def read(reader):
            barrier.wait(timeout=30)
            for repeat in range(6):
                query(node, 'hashrange', dict.fromkeys(numbers, revision),
                    f'concurrent-{revision}-{reader}-{repeat}', bounded=reader != 0,
                    allowed_revisions=[revision - 1, revision])
        started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [pool.submit(write), *[pool.submit(read, number) for number in range(3)]]
            for job in jobs:
                job.result(timeout=180)
        query(node, 'hashrange', dict.fromkeys(numbers, revision), 'after-concurrent-ack', True)
        node.remember(operation='memory-state-concurrent-cohort', revision=revision,
            wall_nanos=time.perf_counter_ns() - started,
            limit='Concurrent ordinary calls; no deterministic pause at captured read state.')
    node.flush()
    node.stop()
    node.start()
    expected = dict.fromkeys(numbers, 5)
    query(node, 'hashrange', expected, 'normal-reopen-after-concurrent-writes', True)
    cost_gate(node, 'hashrange', expected, 'warm-after-concurrent-writes', True)


def absent_schema(node, phase):
    request = {'schema_name': SCHEMAS['hash'], 'fields': FIELDS,
               'filter': {'HashKey': measure.key(0)}, 'limit': 1}
    try:
        node.call('/api/query', request, True)
    except (RuntimeError, json.JSONDecodeError):
        row = node.run.rows[-1]
        body = json.dumps(row.get('result', {})).lower()
        body += base64.b64decode(row.get('raw_body_base64', '')).decode(errors='replace').lower()
        name = re.escape(SCHEMAS['hash'].lower())
        exact_absence = re.search(r"\bschema\s+['\"]?" + name +
                                 r"['\"]?\s+(?:not found|not loaded|does not exist)\b", body)
        if (row.get('operation') != 'refusal' or row.get('path') != '/api/query'
                or row.get('http_status') not in (400, 404) or exact_absence is None):
            raise
        node.remember(operation='memory-state-expected-schema-absence', phase=phase,
            request_id=row['request_id'], http_status=row['http_status'], result=row.get('result'),
            raw_body_base64=row.get('raw_body_base64'))
        return
    raise RuntimeError('An absent generated schema unexpectedly answers the keyed query')


def schema_lifecycle(node):
    prior = SCHEMAS['hash']
    SCHEMAS['hash'] = 'MemoryStateLifecycle'
    try:
        absent_schema(node, 'schema-absent-before-create')
        install(node, 'hash')
        mutate(node, 'hash', [0], 0, 'create', durability='durable')
        query(node, 'hash', {0: 0}, 'schema-created-after-captured-absence')
        cost_gate(node, 'hash', {0: 0}, 'warm-schema-created-after-captured-absence')
        node.call('/api/schemas/drop', {'schema': SCHEMAS['hash'], 'must_exist': True})
        absent_schema(node, 'schema-absent-after-drop')
        install(node, 'hash')
        query(node, 'hash', {0: 0}, 'schema-reinstalled-after-drop')
        cost_gate(node, 'hash', {0: 0}, 'warm-schema-reinstalled-after-drop')
        node.flush()
        node.stop()
        node.start()
        query(node, 'hash', {0: 0}, 'normal-reopen-after-schema-lifecycle')
    finally:
        SCHEMAS['hash'] = prior


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--read-family-log', action='store_true')
    own = parser.parse_args()
    if measure.profile.file_hash(own.binary) != own.sha256:
        raise RuntimeError('The program identity differs')
    args = SimpleNamespace(binary=own.binary, variant='operational-current', root=None,
        receipt_dir=own.receipt_dir, source_commit='frozen-' + own.sha256,
        records_per_hash=64, hashes=4, body_bytes=1024, cap=10000, seed_batch=64,
        read_rounds=6, counts=[16, 64], concurrent_width=16, extra_env={}, reads_only=False)
    environment = dict(ENVIRONMENT, LASTDB_SCHEMA_PRELOAD_SCHEMAS=SCHEMAS['hashrange'])
    prior = {name: os.environ.get(name) for name in environment}
    run, node, complete, error = None, None, False, None
    try:
        os.environ.update(environment)
        run = measure.Run(args)
        if own.read_family_log:
            from family_node import FamilyNode
            node = FamilyNode(run)
            shutil.copy2(HERE / 'family_node.py', run.receipt_dir / 'family_node.py')
        else:
            node = Node(run)
        for name in ('operational.py', 'api.py', 'driver.py'):
            shutil.copy2(HERE / name, run.receipt_dir / name)
        shutil.copy2(own.source, run.receipt_dir / 'source-identity.json')
        measure.profile.save(run.receipt_dir / 'operational-inputs.json', {
            'environment': environment, 'binary_sha256': own.sha256,
            'fixed_read_family_log': 'lastdb::native_read_family=debug' if own.read_family_log else None,
            'schemas': SCHEMAS, 'all_fields': FIELDS,
            'limits': ['All homes are new generated data; no previous home is read.',
                'Concurrent calls do not deterministically pause an older captured read.',
                'A read before explicit flush can follow a background flush; native state is preserved.']})
        prepare(node)
        for kind in ('hash', 'hashrange'):
            expected = warm_and_change(node, kind)
            expected = membership(node, kind, expected)
            schema_map_change(node, kind, expected)
        concurrent_batches(node)
        schema_lifecycle(node)
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
        for name, value in prior.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if run is not None:
            run.finish(complete, error)
            if node is not None and (node.directory / 'node.log').is_file():
                shutil.copy2(node.directory / 'node.log', run.receipt_dir / 'node.log')
            measure.profile.save(run.receipt_dir / 'result.json', {
                'complete': complete, 'error': error,
                'keyed_read_checks': sum(row.get('operation') == 'memory-state-read' for row in run.rows),
                'zero_read_gates': sum(row.get('operation') == 'memory-state-warm-storage-gate' for row in run.rows)})
    print(json.dumps({'complete': complete, 'error': error,
        'root': None if run is None else str(run.root), 'receipt_dir': str(own.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
