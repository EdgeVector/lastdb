"""Use a normal named-database header and local catalog membership."""
import base64
import json
import time

from api import measure
import logical

LOCATOR = 'lastdb://org/core-owned/generated'


class View:
    def __init__(self, node, locator):
        self.node, self.locator = node, locator

    def __getattr__(self, name):
        return getattr(self.node, name)

    def remember(self, **row):
        self.node.remember(db_locator=self.locator, **row)

    def call(self, path, value=None, cost=False):
        with self.node.request_lock:
            self.node.sequence += 1
            identity = f'core-owned-{self.node.run.identity}-scoped-{self.node.sequence}'
        headers = {'Content-Type': 'application/json', 'X-LastDB-Client': 'core-owned-writes',
                   'X-LastDB-Request-Id': identity, 'X-LastDB-Db': self.locator}
        if cost:
            headers['x-lastdb-read-receipt'] = 'true'
        connection = measure.profile.UnixConnection(self.node.socket)
        started, response = time.perf_counter_ns(), None
        try:
            connection.request('GET' if value is None else 'POST', path,
                None if value is None else json.dumps(value, separators=(',', ':')), headers)
            response = connection.getresponse()
            raw = response.read()
            try:
                result = json.loads(raw)
            except Exception as exc:
                self.remember(operation='malformed-response', path=path, request_id=identity,
                    http_status=response.status, request=value,
                    raw_body_base64=base64.b64encode(raw).decode('ascii'),
                    decode_error=type(exc).__name__ + ': ' + str(exc))
                raise
            elapsed = time.perf_counter_ns() - started
            if response.status != 200:
                self.remember(operation='refusal', path=path, request_id=identity,
                    http_status=response.status, request=value, result=result)
                raise RuntimeError(f'{path}: HTTP {response.status}')
            receipt = response.getheader('x-lastdb-read-receipt')
            receipt = None if receipt is None else json.loads(receipt)
        except Exception as exc:
            if response is None:
                self.remember(operation='transport-error', path=path, request_id=identity,
                              request=value, error=type(exc).__name__ + ': ' + str(exc))
            raise
        finally:
            connection.close()
        self.remember(operation='request', path=path, request_id=identity,
                      request=value, result=result, cost=receipt, elapsed_nanos=elapsed)
        return result, elapsed, receipt, identity


def run(node):
    specs = [logical.Spec('hash', 'CoreOwnedScopedHash'),
             logical.Spec('hashrange', 'CoreOwnedScopedHashRange')]
    scoped = View(node, LOCATOR)
    for spec in specs:
        logical.install(node, spec)
        node.call('/api/db/catalog', {'db_locator': LOCATOR, 'schema_name': spec.schema})
    logical.write(node, [logical.mutation(spec, 0, 1, 'create') for spec in specs],
                  'scoped-personal-create')
    for spec in specs:
        logical.read(node, spec, {0: 1}, 'scoped-personal-first-read', zero=True)
        logical.read(scoped, spec, {0: None}, 'scoped-empty-definitions-and-keys')
    for revision, action in ((2, 'create'), (3, 'update'), (0, 'delete'), (4, 'create')):
        logical.write(scoped, [logical.mutation(spec, 0, revision, action)
                               for spec in specs], 'scoped-' + action)
        node.status('scoped-' + action + '-after-ack-before-first-value-read')
        for spec in specs:
            logical.read(scoped, spec, {0: None if action == 'delete' else revision},
                         'scoped-after-' + action, zero=True)
            logical.read(node, spec, {0: 1}, 'personal-unchanged-after-scoped-' + action, zero=True)
    logical.drained(node, 'scoped-complete-flush')
    node.stop()
    node.start()
    for spec in specs:
        logical.read(scoped, spec, {0: 4}, 'scoped-normal-reopen')
        logical.read(node, spec, {0: 1}, 'personal-normal-reopen')
    node.remember(operation='core-owned-scoped-complete', db_locator=LOCATOR,
        scope='Local named-database membership plus X-LastDB-Db. No shared schema service or cloud request supplies this data.')
