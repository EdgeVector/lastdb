"""Ordinary Mini transport and keyed value checks for the temperature run."""
import base64
import json
from pathlib import Path
import sys
import time

BASE = Path('/Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016')
sys.path.insert(0, str(BASE / 'docs/proofs/schema-preload-product-20261017'))
import measure

SCHEMAS = {'hash': 'TemperatureHashProduct', 'hashrange': 'TemperatureHashRangeProduct'}
FIELDS = ['title', 'body']
ALL_FIELDS = ['bucket', 'id', *FIELDS]
HASH = measure.bucket(0)


def values(number, revision):
    return {'bucket': HASH, 'id': measure.key(number), 'title': f'title-{number}-revision-{revision}',
            'body': measure.profile.body(number + revision * 1000000, 1024)}


def address(kind, number):
    return {'hash': measure.key(number)} if kind == 'hash' else {'hash': HASH, 'range': measure.key(number)}


class Node(measure.Node):
    def call(self, path, value=None, cost=False):
        with self.request_lock:
            self.sequence += 1
            request_id = f'temperature-{self.run.identity}-{self.variant}-{self.sequence}'
        headers = {'Content-Type': 'application/json', 'X-LastDB-Client': 'read-write-temperature',
                   'X-LastDB-Request-Id': request_id}
        if cost:
            headers['x-lastdb-read-receipt'] = 'true'
        connection = measure.profile.UnixConnection(self.socket)
        started, response = time.perf_counter_ns(), None
        try:
            connection.request('GET' if value is None else 'POST', path,
                None if value is None else json.dumps(value, separators=(',', ':')), headers)
            response = connection.getresponse()
            raw = response.read()
            try:
                result = json.loads(raw)
            except Exception as exc:
                self.remember(operation='refusal' if response.status != 200 else 'malformed-response',
                    path=path, request_id=request_id, http_status=response.status, request=value,
                    raw_body_base64=base64.b64encode(raw).decode('ascii'),
                    decode_error=f'{type(exc).__name__}: {exc}')
                raise
            elapsed = time.perf_counter_ns() - started
            if response.status != 200:
                self.remember(operation='refusal', path=path, request_id=request_id,
                    http_status=response.status, request=value, result=result,
                    raw_body_base64=base64.b64encode(raw).decode('ascii'))
                raise RuntimeError(f'{path}: HTTP {response.status}')
            receipt = response.getheader('x-lastdb-read-receipt')
            receipt = None if receipt is None else json.loads(receipt)
        except Exception as exc:
            if response is None:
                self.remember(operation='transport-error', path=path, request_id=request_id,
                    request=value, error=f'{type(exc).__name__}: {exc}')
            raise
        finally:
            connection.close()
        self.remember(operation='request', path=path, request_id=request_id,
            request=value, result=result, cost=receipt, elapsed_nanos=elapsed)
        return result, elapsed, receipt, request_id

    def status(self, phase):
        result, elapsed, _, request_id = self.call('/api/db/native-status')
        native = result.get('data', result)['native_disk']
        if native.get('writer_failed') is not False:
            raise RuntimeError('The ordinary native status reports a failed writer')
        self.remember(operation='native-status', phase=phase, request_id=request_id,
            native=native, elapsed_nanos=elapsed,
            missing_metrics='Absent original-version fields remain unavailable, not zero.')
        return native

    def check_response(self, kind, numbers, result, fields, shape):
        rows = result.get('results', [])
        wanted = {(address(kind, n)['hash'], address(kind, n).get('range')):
                  {field: values(n, self.run.revision.get((kind, n), 0))[field] for field in fields}
                  for n in numbers}
        actual = {}
        for row in rows:
            key = (row['key']['hash'], row['key'].get('range'))
            actual[key] = {field: row['fields'].get(field) for field in fields}
            if any(not row['metadata'][field].get('atom_uuid')
                   or not row['metadata'][field].get('molecule_uuid') for field in fields):
                raise RuntimeError('A returned field lacks its atom or molecule identity')
        if actual != wanted or len(rows) != len(wanted):
            raise RuntimeError('The exact keys, membership, or values differ')
        if result.get('returned_count') != len(wanted) or result.get('has_more') is not False:
            raise RuntimeError('The returned count or exhaustion differs')
        if shape == 'bounded' and [row['key']['range'] for row in rows] != sorted(key[1] for key in wanted):
            raise RuntimeError('The bounded HashRange result order differs')

    def keyed_read(self, kind, numbers, shape, phase, repeat=0, full=False):
        numbers, fields = list(numbers), ALL_FIELDS if full else FIELDS
        if kind == 'hash':
            queries = [{'schema_name': SCHEMAS[kind], 'fields': fields,
                'filter': {'HashKey': measure.key(n)}, 'limit': 1} for n in numbers]
        else:
            selector = ({'HashRangeRange': {'hash': HASH, 'start': measure.key(numbers[0]),
                         'end': measure.key(numbers[-1] + 1)}} if shape == 'bounded' else
                        {'HashRangeKeys': [[HASH, measure.key(n)] for n in numbers]})
            queries = [{'schema_name': SCHEMAS[kind], 'fields': fields,
                        'filter': selector, 'limit': len(numbers)}]
        batched = kind == 'hash' and len(numbers) > 1
        result, elapsed, cost, request_id = self.call('/api/queries/batch' if batched else '/api/query',
            {'queries': queries} if batched else queries[0], True)
        costs = [cost]
        if batched:
            items = result.get('results', [])
            if result.get('count') != len(numbers) or len(items) != len(numbers):
                raise RuntimeError('The Hash batch lacks its complete item array')
            costs = []
            for number, item in zip(numbers, items):
                if item.get('status') != 200 or not item.get('read_receipt'):
                    self.remember(operation='query-item-failure', phase=phase,
                        request_id=request_id, number=number, item=item)
                    raise RuntimeError('A Hash batch item failed or lacks its read receipt')
                self.check_response(kind, [number], item['response'], fields, shape)
                costs.append(item['read_receipt'])
        else:
            if cost is None:
                raise RuntimeError('The query read receipt is absent')
            self.check_response(kind, numbers, result, fields, shape)
        self.remember(operation='temperature-read', geometry=kind, shape=shape, phase=phase,
            width=len(numbers), repeat=repeat, request_id=request_id, elapsed_nanos=elapsed,
            endpoint='/api/queries/batch' if batched else '/api/query', costs=costs,
            all_values_and_membership_match=True, fields=list(fields))
        return result
