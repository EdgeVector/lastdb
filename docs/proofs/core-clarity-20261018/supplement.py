"""Ordinary CAS, repeated writes, and unsupported-operation refusals."""
from concurrent.futures import ThreadPoolExecutor

import logical

SPECS = [logical.Spec('hash', 'CoreClarityHash'),
         logical.Spec('hashrange', 'CoreClarityHashRange')]


def require_refusal(node, path, request, statuses, message):
    first = len(node.run.rows)
    try:
        node.call(path, request)
    except RuntimeError:
        rows = [row for row in node.run.rows[first:]
                if row.get('operation') == 'refusal' and row.get('path') == path
                and row.get('request') == request]
        if len(rows) != 1 or rows[0]['http_status'] not in statuses:
            raise
        if message not in str(rows[0].get('result', '')):
            raise RuntimeError('The refusal names another cause')
        node.remember(operation='expected-operation-refusal', path=path,
            request_id=rows[0]['request_id'], http_status=rows[0]['http_status'],
            cause=message)
        return
    raise RuntimeError('The unsupported operation succeeds')


def reads(node, expected, phase, zero=True):
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(logical.read, node, spec, expected, phase, zero)
                   for spec in SPECS]
        for future in futures:
            future.result(timeout=180)


def cas(node):
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(logical.install, node, spec) for spec in SPECS]
        for future in futures:
            future.result(timeout=180)
    operations = [logical.mutation(spec, number, 40, 'create')
                  for spec in SPECS for number in (0, 1)]
    logical.write(node, operations, 'clarity-create')
    reads(node, {0: 40, 1: 40}, 'clarity-create-first-values')
    guarded = []
    for spec in SPECS:
        operation = logical.mutation(spec, 0, 41)
        operation['expected'] = {'type': 'value', 'field': 'title',
                                 'value': spec.values(0, 40)['title']}
        guarded.append(operation)
    logical.write(node, guarded, 'clarity-cas-current-value')
    reads(node, {0: 41, 1: 40}, 'clarity-cas-first-values')
    require_refusal(node, '/api/mutations/batch', {'mutations': guarded},
                    (409,), 'conflict')
    reads(node, {0: 41, 1: 40}, 'clarity-cas-refusal-preserves-values')
    repeated = [logical.mutation(spec, 0, 41) for spec in SPECS]
    first = logical.write(node, repeated, 'clarity-repeated-public-payload-first')
    second = logical.write(node, repeated, 'clarity-repeated-public-payload-second')
    if set(first['mutation_ids']) & set(second['mutation_ids']):
        raise RuntimeError('Fresh public writes unexpectedly reuse mutation identities')
    node.remember(operation='public-repeat-contract',
        first_mutation_ids=first['mutation_ids'], second_mutation_ids=second['mutation_ids'],
        first_operations=first['operations'], second_operations=second['operations'],
        scope='Each public call receives a fresh author clock. This is repeated-write evidence, not an internal idempotency-hit claim.')
    reads(node, {0: 41, 1: 40}, 'clarity-public-repeat-values')
    logical.drained(node, 'clarity-repeated-writes-flushed')
    before = node.status('clarity-before-rejected-empty-public-batch')
    require_refusal(node, '/api/mutations/batch', {'mutations': []},
                    (400,), 'at least one mutation')
    after = node.status('clarity-after-rejected-empty-public-batch')
    for name in ('accepted_sequence', 'completed_sequence', 'accepted_batches', 'core_warm_dirty'):
        if before[name] != after[name]:
            raise RuntimeError('An empty public batch changes the native write frontier')
    node.remember(operation='empty-public-batch-no-write-frontier',
        before=before, after=after,
        scope='The public route rejects the empty array before core. The internal empty transaction remains a source-reviewed branch.')


def layouts(node):
    def declare_unsupported(kind):
        schema = {'source': 'system_seed', 'name': 'CoreClarityUnsupported' + kind,
            'descriptive_name': 'CoreClarityUnsupported' + kind, 'schema_type': kind,
            'fields': ['id', 'title'],
            'field_descriptions': {'id': 'clarity unsupported id', 'title': 'clarity unsupported title'}}
        if kind == 'Range':
            schema['key'] = {'range_field': 'id'}
        require_refusal(node, '/api/schemas/seed-system',
            {'isolated_copy': True, 'schema': schema}, (400,), 'only Hash and HashRange')
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(declare_unsupported, kind) for kind in ('Single', 'Range')]
        for future in futures:
            future.result(timeout=180)
    selectors = [{'RangeKey': 'record'}, {'RangePrefix': 'record'},
        {'RangeRange': {'start': 'a', 'end': 'z'}},
        {'HashRange': {'start': 'a', 'end': 'z'}}, {'HashPattern': '*'},
        {'RangePattern': '*'}, {'HashRangePattern': {'hash': SPECS[1].hash, 'pattern': '*'}}]
    queries = [{'schema_name': SPECS[1].schema, 'fields': ['title'],
                'filter': selector, 'limit': 8} for selector in selectors]
    result, _, _, request = node.call('/api/queries/batch', {'queries': queries})
    results = result.get('results', [])
    if result.get('count') != len(queries) or len(results) != len(queries):
        raise RuntimeError('The unsupported-selector response is incomplete')
    for item in results:
        if item.get('status') not in (400, 409):
            raise RuntimeError('An unsupported selector succeeds')
    node.remember(operation='native-unsupported-filter-refusals', count=len(results),
        request_id=request, results=results)


def cloud(node):
    names = ('cloud_sync.json', 'cloud_sync.json.paused',
             '.cloud_resume_required', '.cloud_resume_requested', '.cloud_resume_ready')
    before = {name: (node.home / name).exists() for name in names}
    require_refusal(node, '/api/sync/cloud-on', {}, (409,), 'cloud sync is disabled')
    after = {name: (node.home / name).exists() for name in names}
    if before != after:
        raise RuntimeError('The refused cloud activation changes its intent files')
    node.remember(operation='native-cloud-on-refused-before-intent-change',
                  before=before, after=after, remote_claim_observation='Source gate precedes remote operations; no network packet capture.')


def history(node):
    direct = logical.query(SPECS[1], [0], fields=['title'])
    direct['as_of'] = '2020-01-01T00:00:00Z'
    counted = dict(direct, expected_total_count=1)
    result, _, _, request = node.call('/api/queries/batch', {'queries': [direct, counted]})
    results = result.get('results', [])
    if result.get('count') != 2 or len(results) != 2:
        raise RuntimeError('The historical-query refusal batch is incomplete')
    for item in results:
        if item.get('status') != 400 or 'as_of' not in str(item):
            raise RuntimeError('A historical query succeeds or names another cause')
    node.remember(operation='native-as-of-query-and-count-refusals', request_id=request,
        results=results, expected_total_count_forces_keyed_count=True,
        count_route_source='lastdb_host/src/handlers/query/plan.rs::QueryRun::key_restricted')


def literal_encryption_prefixes(node):
    literals = ('ENC:', 'ENZ:', 'ENB:')
    operations, cases = [], []
    for spec in SPECS:
        for index, literal in enumerate(literals):
            number = 8000 + index
            values = spec.values(number, 50)
            values.update(title=literal, body=literal)
            operations.append({'type': 'mutation', 'schema': spec.schema,
                'mutation_type': 'create', 'durability': 'queued',
                'key_value': spec.key(number), 'fields_and_values': values})
            cases.append((spec, number, values))
    logical.write(node, operations, 'literal-encryption-prefix-create')

    def read(phase, zero):
        queries = [logical.query(spec, [number]) for spec, number, _ in cases]
        result, _, _, request = node.call('/api/queries/batch', {'queries': queries}, True)
        results, identities, costs = result.get('results', []), [], []
        if result.get('count') != len(cases) or len(results) != len(cases):
            raise RuntimeError('The literal-prefix response is incomplete')
        for (spec, number, values), item in zip(cases, results):
            response = item.get('response', {})
            rows = response.get('results', [])
            if (item.get('status') != 200 or len(rows) != 1
                    or rows[0]['key'] != spec.key(number) or rows[0]['fields'] != values
                    or response.get('returned_count') != 1 or response.get('has_more') is not False
                    or not item.get('read_receipt')):
                raise RuntimeError('A literal encryption-prefix value changes or disappears')
            identities.append({field: rows[0]['metadata'][field]['atom_uuid']
                               for field in logical.FIELDS})
            costs.append(item['read_receipt'])
        calls = sum(cost['read_operations'] for cost in costs)
        if zero and calls != 0:
            raise RuntimeError('The first literal-prefix read still requires a file')
        node.remember(operation='literal-encryption-prefix-read', phase=phase,
            request_id=request, records=len(cases), literal_values=list(literals),
            identities=identities, file_read_calls=calls, require_zero_file_calls=zero,
            content_binary_override=False, all_values_match=True)
        return identities

    first = read('literal-prefix-first-values-after-ack', True)
    logical.drained(node, 'literal-prefix-flushed')
    node.stop()
    node.start()
    if read('literal-prefix-after-normal-reopen', False) != first:
        raise RuntimeError('Literal-prefix atom identities change after reopen')


def purge_warm_unique_atoms(node):
    number, operations, wanted, uuids = 9000, [], [], []
    for spec in SPECS:
        values = spec.values(number, 55)
        values.update(title='unique-purge-title-' + spec.schema,
                      body='unique-purge-body-' + spec.schema)
        operations.append({'type': 'mutation', 'schema': spec.schema,
            'mutation_type': 'create', 'durability': 'queued',
            'key_value': spec.key(number), 'fields_and_values': values})
        wanted.append(values)
    logical.write(node, operations, 'warm-unique-purge-create')
    queries = [logical.query(spec, [number]) for spec in SPECS]
    result, _, _, request = node.call('/api/queries/batch', {'queries': queries}, True)
    responses = result.get('results', [])
    if result.get('count') != 2 or len(responses) != 2:
        raise RuntimeError('The warm-purge discovery is incomplete')
    for spec, values, item in zip(SPECS, wanted, responses):
        rows = item.get('response', {}).get('results', [])
        if (item.get('status') != 200 or len(rows) != 1 or rows[0]['fields'] != values
                or rows[0]['key'] != spec.key(number)
                or item.get('read_receipt', {}).get('read_operations') != 0):
            raise RuntimeError('The unique purge values are not warm after ACK')
        uuids.extend(rows[0]['metadata'][field]['atom_uuid'] for field in ('title', 'body'))
    if len(set(uuids)) != 4:
        raise RuntimeError('The selected purge atoms are not unique')
    node.remember(operation='warm-purge-uuid-selection', request_id=request, uuids=uuids,
        scope='Fresh unique title/body contents. The completed value query warms every selected logical path.')
    erasures = [dict(logical.mutation(spec, number, 0, 'delete'), mutation_type='purge')
                for spec in SPECS]
    logical.write(node, erasures, 'warm-unique-purge-ack')

    def verify(phase, zero):
        reads(node, {number: None}, phase + '-logical-absence', zero)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(require_refusal, node, '/api/atom/' + uuid, None,
                                   (404,), 'not found') for uuid in uuids]
            for future in futures:
                future.result(timeout=180)
        node.remember(operation='warm-purge-public-uuid-absence', phase=phase, uuids=uuids,
            http_status=404, source_reviewed_deletion=True, reclaimed_disk_bytes_proved=False)

    verify('warm-purge-after-ack', True)
    logical.drained(node, 'warm-unique-purge-flushed')
    node.stop()
    node.start()
    verify('warm-purge-after-normal-reopen', False)


def run(node):
    cas(node)
    layouts(node)
    history(node)
    literal_encryption_prefixes(node)
    purge_warm_unique_atoms(node)
    cloud(node)
    logical.drained(node, 'clarity-supplement-final-flush')
