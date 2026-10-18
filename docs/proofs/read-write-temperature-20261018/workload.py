"""Bounded ordinary operations; every HashRange request names its hash."""
import json
import time
from api import ALL_FIELDS, FIELDS, HASH, SCHEMAS, address, measure, values


def batch_body(kind, numbers, operation, revision, durability):
    mutations = []
    for number in numbers:
        fields = values(number, revision)
        if operation == 'update':
            fields = {field: fields[field] for field in FIELDS}
        mutations.append({'type': 'mutation', 'schema': SCHEMAS[kind],
            'mutation_type': operation, 'durability': durability,
            'key_value': address(kind, number), 'fields_and_values': fields})
    return {'mutations': mutations}


def accept(node, kind, numbers, revision, request):
    result, elapsed, _, request_id = node.call('/api/mutations/batch', request)
    requested = request['mutations'][0]['durability']
    actual = result.get('durability')
    if (result.get('count') != len(numbers) or result.get('ok') is False
            or actual not in ('queued', 'durable') or requested == 'durable' and actual != 'durable'):
        raise RuntimeError('The complete mutation batch lacks its correct acknowledgement')
    for number in numbers:
        node.run.revision[(kind, number)] = revision
    return result, elapsed, request_id


def seed(node):
    node.start()
    for kind in ('hash', 'hashrange'):
        node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': SCHEMAS[kind],
            'descriptive_name': 'Generated Temperature ' + kind,
            'schema_type': 'Hash' if kind == 'hash' else 'HashRange',
            'key': {'hash_field': 'id'} if kind == 'hash' else
                   {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': ALL_FIELDS,
            'field_descriptions': {field: kind + ' temperature generated ' + field for field in ALL_FIELDS}}})
        for first in range(0, 256, 64):
            numbers = list(range(first, first + 64))
            body = batch_body(kind, numbers, 'create', 0, 'durable')
            _, elapsed, request_id = accept(node, kind, numbers, 0, body)
            node.remember(operation='temperature-seed', geometry=kind, width=64,
                          elapsed_nanos=elapsed, request_id=request_id)
            node.flush()
    node.status('seed-complete')
    node.stop()


def ready_keys(native):
    keys = native['schema_keys']
    if not (keys['loads'] > 0 and keys['keys'] > 0 and keys['ready'] > 0 and keys['ready'] == keys['domains']
            and keys['loading'] == 0 and keys['disk_only'] == 0 and keys['failed'] == 0):
        raise RuntimeError('The selected HashRange domains are not complete Ready domains: ' + json.dumps(keys))
    return keys


def read_conditions(node):
    cases = [('hash', 'exact', width) for width in (1, 16, 64)]
    cases += [('hashrange', 'exact', width) for width in (1, 16, 64)]
    cases += [('hashrange', 'bounded', width) for width in (16, 64)]
    for kind, shape, width in cases:
        phase = f'{kind}-{shape}-{width}'
        node.start()
        node.status(phase + '-before-first-read')
        numbers = list(range(width))
        node.keyed_read(kind, numbers, shape, 'first-after-reopen')
        first_state = node.status(phase + '-after-first-read')
        if kind == 'hash' and node.run.label == 'current-on' and first_state['schema_keys']['loads'] != 0:
            raise RuntimeError('A Hash-only first read unexpectedly starts a range preload')
        for repeat in range(5):
            node.keyed_read(kind, numbers, shape, 'repeated-same-keys', repeat)
        repeated_state = node.status(phase + '-after-repeats')
        if kind == 'hash' and node.run.label == 'current-on' and repeated_state['schema_keys']['loads'] != 0:
            raise RuntimeError('A repeated Hash-only read unexpectedly starts a range preload')
        if kind == 'hashrange':
            before = node.status(phase + '-before-other-keys')
            node.keyed_read(kind, list(range(128, 128 + width)), shape, 'other-keys-same-process')
            after = node.status(phase + '-after-other-keys')
            if node.run.label == 'current-on':
                left, right = ready_keys(before), ready_keys(after)
                if left['loads'] != right['loads']:
                    raise RuntimeError('Other HashRange keys unexpectedly reload the complete domains')
                if shape == 'bounded' and right['memory_selections'] <= left['memory_selections']:
                    raise RuntimeError('The complete Ready keys did not supply bounded key selection')
            node.remember(operation='other-keys-native-state', geometry=kind, shape=shape,
                width=width, before=before, after=after,
                complete_ready_keys_required=node.run.label == 'current-on')
        node.stop()


def timed_write(node, kind, numbers, operation, revision, condition, repeat=0):
    request = batch_body(kind, numbers, operation, revision, 'queued')
    payload = sum(len(json.dumps(mutation['fields_and_values'], separators=(',', ':'),
                  ensure_ascii=False).encode()) for mutation in request['mutations'])
    before = node.status(f'{kind}-{condition}-{repeat}-before-write')
    started = time.perf_counter_ns()
    result, ack, request_id = accept(node, kind, numbers, revision, request)
    after_ack = node.status(f'{kind}-{condition}-{repeat}-after-ack')
    node.flush()
    finished = time.perf_counter_ns()
    after_flush = node.status(f'{kind}-{condition}-{repeat}-after-flush')
    node.remember(operation='temperature-write', geometry=kind, width=len(numbers),
        mutation_type=operation, condition=condition, repeat=repeat, request_id=request_id,
        requested_durability='queued', returned_durability=result['durability'],
        ack_elapsed_nanos=ack, wall_nanos_through_explicit_flush=finished - started,
        serialized_field_payload_bytes=payload, native_before=before,
        native_after_ack=after_ack, native_after_flush=after_flush,
        scope='Wall time includes one ordinary native-status call between ack and explicit flush.',
        limit='Returned mode and global pending state do not establish memory-only preparation or a per-mutation preflush observation.')
    node.keyed_read(kind, numbers, 'exact', 'write-after-flush-value-check', full=True)


def write_conditions(node):
    for kind in ('hash', 'hashrange'):
        node.start()
        timed_write(node, kind, list(range(256, 320)), 'create', 0, 'new-key-after-reopen')
        node.stop()
        node.start()
        timed_write(node, kind, list(range(64)), 'update', 1, 'update-no-prior-value-read')
        node.stop()
        node.start()
        for repeat in range(5):
            node.keyed_read(kind, list(range(64)), 'exact', 'update-read-prime', repeat)
            timed_write(node, kind, list(range(64)), 'update', repeat + 2,
                        'update-read-primed', repeat)
        node.stop()


def final_readback(node):
    node.start()
    for kind in ('hash', 'hashrange'):
        for first in range(0, 320, 64):
            node.keyed_read(kind, list(range(first, first + 64)), 'exact',
                            'final-normal-reopen-all-values', full=True)
    node.status('final-normal-reopen-complete')


def run(node):
    seed(node)
    read_conditions(node)
    write_conditions(node)
    final_readback(node)
