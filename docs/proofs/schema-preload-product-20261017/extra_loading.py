"""Ordinary concurrent requests during a selected schema's first key load."""
from concurrent.futures import ThreadPoolExecutor
import time

import measure


def same_ready(before, after):
    left, right = before['schema_keys'], after['schema_keys']
    if left['loads'] != right['loads'] or left['ready'] != right['ready'] or right['loading']:
        raise RuntimeError('A current query or physical flush reloads the complete key domains')


def queued_change(node, put_only):
    first = node.run.args.records_per_hash + (2 if put_only else 0)
    revision = node.run.revisions.get((0, 0), 0)
    node.run.revisions[(0, 0)] = revision + 1
    updated = node.run.values(0, 0)
    node.run.revisions[(0, 0)] = revision
    changes = [('update', (0, 0), updated)]
    if not put_only:
        changes.extend([('delete', (0, 3), {}), ('delete', (0, 5), {})])
    changes.extend([('create', (0, n), node.run.values(0, n)) for n in (first, first + 1)])
    mutations = []
    for operation, (h, n), values in changes:
        fields = dict(values)
        if operation == 'create':
            fields.update(bucket=measure.bucket(h), id=measure.key(n))
        mutations.append({'type': 'mutation', 'schema': measure.SCHEMA, 'mutation_type': operation,
            'durability': 'queued', 'key_value': {'hash': measure.bucket(h), 'range': measure.key(n)},
            'fields_and_values': fields})
    result, elapsed, _, request_id = node.call('/api/mutations/batch', {'mutations': mutations})
    if result.get('count') != len(mutations) or result.get('durability') not in ('queued', 'durable'):
        raise RuntimeError('The concurrent write lacks its complete batch acknowledgement')
    node.run.revisions[(0, 0)] = revision + 1
    node.run.created.update([(0, first), (0, first + 1)])
    if not put_only:
        node.run.absent.update([(0, 3), (0, 5)])
    node.remember(operation='mutation', phase='observed-loading-put' if put_only else 'observed-loading-mixed',
                  batch=len(mutations), elapsed_nanos=elapsed, request_id=request_id,
                  requested_durability='queued', actual_durability=result['durability'])


def coherent_values(node, pairs):
    return {(measure.bucket(h), measure.key(n)): node.run.values(h, n)
            for h, n in pairs if node.run.live(h, n)}


def one_cold_case(node, put_only):
    label = 'put' if put_only else 'mixed'
    node.mutate([(0, 1)], 'writer-prime-' + label, operation='update', durability='queued')
    if node.status('before-cold-query-' + label)['schema_keys']['loads']:
        raise RuntimeError('The ordinary preparatory update unexpectedly starts a key load')
    first = node.run.args.records_per_hash + (2 if put_only else 0)
    pairs = [(0, n) for n in range(16)] + [(0, first), (0, first + 1)]
    before_values = coherent_values(node, pairs)
    request = {'schema_name': measure.SCHEMA, 'fields': measure.FIELDS,
               'filter': {'HashRangeKeys': [[measure.bucket(h), measure.key(n)] for h, n in pairs]},
               'limit': len(pairs)}
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(node.call, '/api/query', request, True)
        observed = False
        for _ in range(200):
            if node.status('loading-observation-' + label)['schema_keys']['loading']:
                observed = True
                break
            if future.done():
                break
            time.sleep(.025)
        if not observed:
            future.result()
            node.remember(operation='evidence-limit', phase='loading-write-' + label,
                          detail='No Loading state was observed; no concurrent-publication claim')
            return False
        queued_change(node, put_only)
        ack = node.status('loading-write-ack-' + label)
        published_while_loading = ack['schema_keys']['loading'] > 0
        node.remember(operation='publication-evidence', phase='loading-write-' + label,
                      loading_after_ack=published_while_loading,
                      accepted_sequence=ack['accepted_sequence'], completed_sequence=ack['completed_sequence'])
        result, elapsed, cost, request_id = future.result()
    rows = result.get('results', [])
    actual = {(row['key']['hash'], row['key']['range']):
              {field: row['fields'].get(field) for field in measure.FIELDS} for row in rows}
    after_values = coherent_values(node, pairs)
    if len(rows) != len(actual) or actual not in (before_values, after_values) or cost is None:
        raise RuntimeError('The concurrent cold query does not return one complete coherent state')
    node.remember(operation='query', phase='concurrent-cold-query-' + label, shape='exact', batch=len(rows),
                  elapsed_nanos=elapsed, cost=cost, request_id=request_id,
                  coherent_state='before' if actual == before_values else 'after',
                  expected_values_match=True, returned_records=len(rows))
    loaded = node.status('loading-write-complete-' + label)
    keys = loaded['schema_keys']
    if keys['loading'] or keys['disk_only'] or keys['failed'] or keys['loads'] != keys['ready']:
        raise RuntimeError('The initial concurrent load did not reach Ready once per domain')
    node.checked(request['filter'], pairs, 'current-after-loading-write-' + label, limit=len(pairs))
    node.flush()
    node.checked(request['filter'], pairs, 'current-after-loading-flush-' + label, limit=len(pairs))
    same_ready(loaded, node.status('final-loading-write-' + label))
    return published_while_loading


def loading_write(node):
    node.seed()
    node.start()
    order = (True, False) if node.run.args.loading_first == 'put' else (False, True)
    for attempt, put_only in enumerate(order):
        if attempt:
            node.stop()
            node.start()
        if one_cold_case(node, put_only):
            return
        node.remember(operation='evidence-limit', phase='loading-put' if put_only else 'loading-mixed',
                      detail='The ack does not precede Ready; only request overlap is established')
