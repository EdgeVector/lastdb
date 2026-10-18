"""Use the ordinary HashKey API on a fresh generated HashRange schema."""
from api import measure
import operational as previous


def read(node, expected, phase):
    request = {'schema_name': previous.SCHEMAS['hashrange'], 'fields': previous.FIELDS,
               'filter': {'HashKey': previous.HASH}, 'limit': 256}
    result, elapsed, cost, identity = node.call('/api/query', request, True)
    rows = result.get('results', [])
    wanted = {previous.coordinates(previous.key('hashrange', number)):
              previous.field_values(number, revision)
              for number, revision in expected.items()}
    actual = {previous.coordinates(row['key']): row['fields'] for row in rows}
    if (actual != wanted or len(rows) != len(wanted)
            or result.get('returned_count') != len(wanted)
            or result.get('has_more') is not False):
        raise RuntimeError('The HashKey response on the HashRange schema differs')
    if any(not row['metadata'][field].get('atom_uuid')
           or not row['metadata'][field].get('molecule_uuid')
           for row in rows for field in previous.FIELDS):
        raise RuntimeError('The HashKey response lacks a field identity')
    node.remember(operation='completion-hash-all-read', phase=phase,
        request_id=identity, elapsed_nanos=elapsed, cost=cost,
        expected_key_revisions=expected, excluded_hash='generated-other-hash',
        all_fields_and_membership_match=True,
        scope='HashKey names one hash in a HashRange schema. This is not a standalone Range query.')


def run(node):
    saved = previous.SCHEMAS['hashrange']
    previous.SCHEMAS['hashrange'] = 'MemoryStateHashAllSupplement'
    try:
        schema = previous.install(node, 'hashrange')
        mutations = []
        for number in range(6):
            mutations.append({'type': 'mutation', 'schema': schema,
                'mutation_type': 'create', 'durability': 'durable',
                'key_value': previous.key('hashrange', number),
                'fields_and_values': previous.field_values(number, 41)})
        other = previous.field_values(9000, 91)
        other['bucket'] = 'generated-other-hash'
        mutations.append({'type': 'mutation', 'schema': schema,
            'mutation_type': 'create', 'durability': 'durable',
            'key_value': {'hash': other['bucket'], 'range': measure.key(9000)},
            'fields_and_values': other})
        result, _, _, identity = node.call('/api/mutations/batch', {'mutations': mutations})
        if result.get('count') != 7 or result.get('durability') != 'durable':
            raise RuntimeError('The supplementary write batch lacks a durable acknowledgement')
        node.remember(operation='completion-hash-all-seed', request_id=identity,
            schema=schema, selected_hash_records=6, excluded_hash_records=1)
        node.flush()
        node.stop()
        node.start()
        expected = dict.fromkeys(range(6), 41)
        for phase in ('normal-reopen', 'repeat-1', 'repeat-2', 'repeat-3'):
            read(node, expected, phase)
        previous.mutate(node, 'hashrange', [2], 42)
        expected[2] = 42
        read(node, expected, 'after-accepted-update')
        previous.mutate(node, 'hashrange', [3], 0, 'delete')
        expected.pop(3)
        read(node, expected, 'after-accepted-delete')
        node.flush()
        node.stop()
        node.start()
        read(node, expected, 'normal-reopen-after-changes')
    finally:
        previous.SCHEMAS['hashrange'] = saved
