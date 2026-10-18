"""Hash-only product reads and writes use no complete range key preload."""
SCHEMA = 'SchemaPreloadExtraHashProduct'
FIELDS = ['title', 'score']


def address(number):
    return f'hash-only-{number:02d}'


def read_batch(node, expected, phase):
    selected = [0, 3, 7, 15]
    queries = [{'schema_name': SCHEMA, 'fields': FIELDS,
                'filter': {'HashKey': address(number)}, 'limit': 1} for number in selected]
    result, elapsed, _, request_id = node.call('/api/queries/batch', {'queries': queries}, True)
    items = result.get('results', [])
    if result.get('count') != len(selected) or len(items) != len(selected):
        raise RuntimeError('The Hash query batch lacks a complete result array')
    for number, item in zip(selected, items):
        if item.get('status') != 200 or not item.get('read_receipt'):
            raise RuntimeError('A Hash query item lacks its success status or read receipt')
        rows = item['response'].get('results', [])
        wanted = expected.get(number)
        if wanted is None:
            if rows:
                raise RuntimeError('A Hash query returns a deleted record')
            continue
        if len(rows) != 1 or rows[0]['key']['hash'] != address(number):
            raise RuntimeError('A Hash query returns the wrong key')
        if {field: rows[0]['fields'].get(field) for field in FIELDS} != wanted:
            raise RuntimeError('A Hash query returns the wrong values')
        if any(not rows[0]['metadata'][field].get('atom_uuid') for field in FIELDS):
            raise RuntimeError('A Hash value lacks its atom identity')
    node.remember(operation='hash-query-batch', phase=phase, batch=len(selected),
                  request_id=request_id, elapsed_nanos=elapsed, expected_values_match=True,
                  item_receipts=[item['read_receipt'] for item in items])


def operations(node):
    node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
        'source': 'system_seed', 'name': SCHEMA, 'schema_type': 'Hash',
        'descriptive_name': 'Hash Only Extra Product Operations', 'key': {'hash_field': 'id'},
        'fields': ['id', *FIELDS], 'field_descriptions': {
            'id': 'generated hash-only identity', 'title': 'generated hash-only title',
            'score': 'generated hash-only number'}}})
    expected = {n: {'title': f'hash-title-{n}-revision-0', 'score': n} for n in range(16)}
    mutations = [{'type': 'mutation', 'schema': SCHEMA, 'mutation_type': 'create',
                  'durability': 'durable', 'key_value': {'hash': address(n)},
                  'fields_and_values': dict(values, id=address(n))} for n, values in expected.items()]
    result, _, _, _ = node.call('/api/mutations/batch', {'mutations': mutations})
    if result.get('count') != 16 or result.get('durability') != 'durable':
        raise RuntimeError('The Hash seed lacks its complete durable acknowledgement')
    node.flush()
    read_batch(node, expected, 'hash-first')
    read_batch(node, expected, 'hash-repeated')
    expected[7] = {'title': 'hash-title-7-revision-1', 'score': 107}
    mutations = [
        {'type': 'mutation', 'schema': SCHEMA, 'mutation_type': 'update',
         'durability': 'queued', 'key_value': {'hash': address(7)},
         'fields_and_values': expected[7]},
        {'type': 'mutation', 'schema': SCHEMA, 'mutation_type': 'delete',
         'durability': 'queued', 'key_value': {'hash': address(3)}, 'fields_and_values': {}}]
    result, _, _, _ = node.call('/api/mutations/batch', {'mutations': mutations})
    if result.get('count') != 2 or result.get('durability') not in ('queued', 'durable'):
        raise RuntimeError('The Hash change batch lacks its complete acknowledgement')
    expected.pop(3)
    read_batch(node, expected, 'hash-change-ack')
    node.flush()
    read_batch(node, expected, 'hash-change-flush')
    if node.status('hash-before-reopen')['schema_keys']['loads']:
        raise RuntimeError('Hash-only operations unexpectedly start a complete range key load')
    node.stop()
    node.start()
    read_batch(node, expected, 'hash-normal-reopen')
    if node.status('hash-after-reopen')['schema_keys']['loads']:
        raise RuntimeError('A reopened Hash query unexpectedly starts a complete range key load')
