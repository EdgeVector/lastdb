"""Ordinary batched mutation and keyed response operations for generated records."""
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

from api import measure

FIELDS = ['bucket', 'id', 'title', 'body']
REQUIRE_WRITE_THROUGH = True


@dataclass(frozen=True)
class Spec:
    kind: str
    schema: str
    hash: str = 'core-owned-generated-hash'

    def key(self, number):
        return ({'hash': measure.key(number)} if self.kind == 'hash' else
                {'hash': self.hash, 'range': measure.key(number)})

    def values(self, number, revision, body_bytes=1024):
        return {'bucket': self.hash, 'id': measure.key(number),
            'title': f'core-owned-{number}-revision-{revision}',
            'body': measure.profile.body(number + revision * 1000000, body_bytes)}


def coordinates(key):
    return key['hash'], key.get('range')


def install(node, spec):
    node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
        'source': 'system_seed', 'name': spec.schema, 'descriptive_name': spec.schema,
        'schema_type': 'Hash' if spec.kind == 'hash' else 'HashRange',
        'key': {'hash_field': 'id'} if spec.kind == 'hash' else
               {'hash_field': 'bucket', 'range_field': 'id'},
        'fields': FIELDS,
        'field_descriptions': {field: spec.schema + ' independent ' + field for field in FIELDS}}})


def mutation(spec, number, revision, action='update', durability='queued', full=False,
             body_bytes=1024):
    values = {} if action == 'delete' else spec.values(number, revision, body_bytes)
    if action == 'update' and not full:
        values = {field: values[field] for field in ('title', 'body')}
    return {'type': 'mutation', 'schema': spec.schema, 'mutation_type': action,
            'durability': durability, 'key_value': spec.key(number),
            'fields_and_values': values}


def write(node, mutations, phase):
    result, elapsed, _, request = node.call('/api/mutations/batch', {'mutations': mutations})
    if result.get('count') != len(mutations) or result.get('durability') not in ('queued', 'durable'):
        raise RuntimeError('The generated batch lacks its complete acknowledgement')
    if all(item['durability'] == 'durable' for item in mutations) and result['durability'] != 'durable':
        raise RuntimeError('The durable batch receives another acknowledgement mode')
    node.remember(operation='core-owned-write', phase=phase, request_id=request,
        count=len(mutations), elapsed_nanos=elapsed, returned_mode=result['durability'],
        requested_modes=sorted({item['durability'] for item in mutations}),
        scope='Queued and durable describe the product reply; neither alone proves memory-only preparation.')
    return result


def query(spec, numbers, bounded=False, fields=None):
    fields = FIELDS if fields is None else fields
    if spec.kind == 'hash':
        if len(numbers) != 1:
            raise RuntimeError('Each Hash query needs exactly one known hash')
        selector = {'HashKey': measure.key(numbers[0])}
    elif bounded:
        selector = {'HashRangeRange': {'hash': spec.hash,
            'start': measure.key(min(numbers)), 'end': measure.key(max(numbers) + 1)}}
    else:
        selector = {'HashRangeKeys': [[spec.hash, measure.key(number)] for number in numbers]}
    return {'schema_name': spec.schema, 'fields': fields, 'filter': selector, 'limit': 256}


def check_response(spec, expected, result, body_bytes=1024, fields=None, bounded=False,
                   allowed_revisions=None):
    fields = FIELDS if fields is None else fields
    wanted = {coordinates(spec.key(number)): number for number, revision in expected.items()
              if revision is not None}
    rows, seen, revisions = result.get('results', []), set(), set()
    for row in rows:
        key = coordinates(row['key'])
        if key not in wanted or key in seen:
            raise RuntimeError('The generated query has unknown or repeated membership')
        seen.add(key)
        number = wanted[key]
        candidates = [expected[number]] if allowed_revisions is None else allowed_revisions
        choices = [revision for revision in candidates if all(
            row['fields'].get(field) == spec.values(number, revision, body_bytes)[field]
            for field in fields)]
        if len(choices) != 1:
            raise RuntimeError('The generated query has wrong or mixed field values')
        revisions.add(choices[0])
        if any(not row['metadata'][field].get('atom_uuid')
               or not row['metadata'][field].get('molecule_uuid') for field in fields):
            raise RuntimeError('A returned field lacks its atom or molecule identity')
    if (seen != set(wanted) or len(rows) != len(wanted)
            or result.get('returned_count') != len(wanted)
            or result.get('has_more') is not False
            or (allowed_revisions is not None and len(revisions) > 1)):
        raise RuntimeError('The query is incomplete or mixes batch revisions: ' + str({
            'missing_keys': sorted(set(wanted) - seen),
            'expected_count': len(wanted), 'actual_count': len(rows),
            'returned_count': result.get('returned_count'), 'has_more': result.get('has_more'),
            'revisions': sorted(revisions)}))
    if bounded and [row['key']['range'] for row in rows] != sorted(key[1] for key in wanted):
        raise RuntimeError('The bounded HashRange result order differs')


def read(node, spec, expected, phase, zero=False, bounded=False, body_bytes=1024,
         fields=None, allowed_revisions=None):
    numbers = list(expected)
    if spec.kind == 'hash' and len(numbers) > 64:
        chunks = [dict((number, expected[number]) for number in numbers[start:start + 64])
                  for start in range(0, len(numbers), 64)]
        with ThreadPoolExecutor(max_workers=min(4, len(chunks))) as pool:
            jobs = [pool.submit(read, node, spec, chunk, phase + '-batch-' + str(index),
                                zero, False, body_bytes, fields, allowed_revisions)
                    for index, chunk in enumerate(chunks)]
            return [job.result(timeout=180) for job in jobs]
    queries = ([query(spec, [number], fields=fields) for number in numbers]
               if spec.kind == 'hash' else [query(spec, numbers, bounded, fields)])
    batched = spec.kind == 'hash' and len(numbers) > 1
    result, elapsed, cost, identity = node.call('/api/queries/batch' if batched else '/api/query',
        {'queries': queries} if batched else queries[0], True)
    if batched:
        items = result.get('results', [])
        if result.get('count') != len(numbers) or len(items) != len(numbers):
            raise RuntimeError('The Hash batch response is incomplete')
        costs = []
        for number, item in zip(numbers, items):
            if item.get('status') != 200 or not item.get('read_receipt'):
                raise RuntimeError('A Hash batch item fails or lacks a read receipt')
            check_response(spec, {number: expected[number]}, item['response'], body_bytes,
                           fields, allowed_revisions=allowed_revisions)
            costs.append(item['read_receipt'])
    else:
        check_response(spec, expected, result, body_bytes, fields, bounded, allowed_revisions)
        costs = [cost]
    calls = None if any(item is None for item in costs) else sum(item['read_operations'] for item in costs)
    if calls is None:
        raise RuntimeError('The ordinary query lacks its requested file-read receipt')
    node.remember(operation='core-owned-read', phase=phase, schema=spec.schema,
        geometry=spec.kind, bounded=bounded, expected_key_revisions=expected,
        request_id=identity, elapsed_nanos=elapsed, costs=costs,
        require_zero_file_calls=zero and REQUIRE_WRITE_THROUGH,
        candidate_write_through_rule=zero, reference_observation_only=zero and not REQUIRE_WRITE_THROUGH,
        file_read_calls=calls,
        all_fields_and_membership_match=True)
    if zero and REQUIRE_WRITE_THROUGH and calls != 0:
        raise RuntimeError('The accepted logical write still needs a file read: ' + str(costs))
    return result


def drained(node, phase):
    node.flush()
    status = node.status(phase)
    if (status.get('accepted_batches') != 0 or status.get('accepted_batch') is not False
            or status.get('accepted_sequence') != status.get('completed_sequence')
            or status.get('core_warm_dirty') != 0):
        raise RuntimeError('The explicit flush leaves accepted or dirty work')
    node.remember(operation='core-owned-queue-drained', phase=phase,
                  accepted_sequence=status['accepted_sequence'])
    return status
