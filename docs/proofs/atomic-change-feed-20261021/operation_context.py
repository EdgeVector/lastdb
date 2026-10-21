"""Ordinary mutation acknowledgements, keyed values, and durable feed observations."""
import base64
import json

from bench_support import logical
import bench_notices


def cursor_number(value):
    if not isinstance(value, str) or not value.startswith('v1-') or len(value) != 19:
        raise RuntimeError('An ordinary notice cursor has a different grammar')
    number = int(value[3:], 16)
    if value != f'v1-{number:016x}':
        raise RuntimeError('An ordinary notice cursor is not canonical')
    return number


def check_order(events, previous, following):
    before, after = 0 if previous is None else cursor_number(previous), cursor_number(following)
    positions = [cursor_number(event.get('cursor')) for event in events]
    if (after < before or any(left >= right for left, right in zip([before, *positions], positions))
            or (positions and positions[-1] != after) or (not positions and after != before)):
        raise RuntimeError('Ordinary notice cursors repeat, regress, or differ from their page continuation')


def item(spec, number, revision, action='create', mode=None, derived=False, **controls):
    operation = logical.mutation(spec, number, revision, action, full=True)
    if mode is None:
        operation.pop('durability')
    else:
        operation['durability'] = mode
    if derived:
        # The public grammar requires key_value. Empty coordinates select field-derived keys.
        operation['key_value'] = {}
    operation.update(controls)
    return {'spec': spec, 'number': number, 'revision': revision,
            'action': action, 'operation': operation}


class Context:
    def __init__(self, node, specs):
        self.node, self.specs = node, specs
        self.expected = {spec.kind: {} for spec in specs}
        self.cursor, self.notices, self.calls = None, {}, []

    def validate_reply(self, result, entries, single):
        if not isinstance(result, dict):
            raise RuntimeError('An ordinary mutation returns a non-object response')
        ids = [result.get('mutation_id')] if single else result.get('mutation_ids')
        if (result.get('durability') != 'durable' or not isinstance(ids, list)
                or len(ids) != len(entries) or any(not isinstance(value, str) or not value for value in ids)
                or len(set(ids)) != len(ids) or set(ids) & set(self.notices)
                or result.get('background_tasks_drained') is not False
                or result.get('convergence_pending') is not True):
            raise RuntimeError('An ordinary mutation lacks its unique IDs or durable acknowledgement')
        operations = {name: sum(entry['action'] == verb for entry in entries)
                      for name, verb in (('created', 'create'), ('updated', 'update'), ('deleted', 'delete'))}
        operations.update(no_op=0, total=len(entries))
        returned = result.get('operations')
        if returned != operations or any(type(value) is not int for value in returned.values()):
            raise RuntimeError('An ordinary acknowledgement changes its requested operation counts')
        if single:
            if result.get('success') is not True:
                raise RuntimeError('An ordinary single mutation lacks its success control')
        elif type(result.get('count')) is not int or result['count'] != len(entries):
            raise RuntimeError('An ordinary mutation batch loses caller operations')
        return ids

    def accept_reply(self, reply, entries, phase, single=False):
        result, elapsed, _, request_id = reply
        ids = self.validate_reply(result, entries, single)
        wanted = {}
        for identity, entry in zip(ids, entries):
            spec, number = entry['spec'], entry['number']
            wanted[identity] = {'schema_name': spec.schema,
                'key': {name: spec.key(number).get(name) for name in ('hash', 'range')},
                'operation': entry['action']}
            self.expected[spec.kind][number] = None if entry['action'] == 'delete' else entry['revision']
        self.notices.update(wanted)
        call = {'phase': phase, 'request_id': request_id, 'single': single,
                'mutations': len(entries), 'elapsed_nanos': elapsed, 'mutation_ids': ids,
                'all_successful_writes_durable': True}
        self.calls.append(call)
        self.node.remember(operation='ordinary-write-observation', **call)
        return wanted

    def observe(self, wanted):
        cursor, events, timing = bench_notices.page_pass(self.node, self.cursor)
        check_order(events, self.cursor, cursor)
        seen = set()
        bench_notices.check_events(events, wanted, seen)
        own = [event for event in events if event.get('schema_name') in {spec.schema for spec in self.specs}]
        if seen != set(wanted) or len(own) != len(wanted):
            raise RuntimeError('The first ordinary notice pagination pass differs from the acknowledged operations')
        self.cursor = cursor
        self.node.remember(operation='ordinary-notices-immediately-after-ack',
            required_ids=sorted(wanted), all_notice_ids_coordinates_and_flags_match=True, **timing)

    def write(self, entries, phase, single=False):
        body = entries[0]['operation'] if single else {'mutations': [entry['operation'] for entry in entries]}
        reply = self.node.call('/api/mutation' if single else '/api/mutations/batch', body)
        self.observe(self.accept_reply(reply, entries, phase, single))
        self.read_all(phase + '-values')

    def read_all(self, phase):
        for spec in self.specs:
            expected = self.expected[spec.kind]
            if expected:
                logical.read(self.node, spec, expected, phase, zero=False)

    def refuse(self, entry, status, phase, marker):
        before = self.cursor
        try:
            self.node.call('/api/mutation', entry['operation'])
        except (RuntimeError, json.JSONDecodeError):
            last = self.node.run.rows[-1]
            if last.get('http_status') != status:
                raise
            raw = base64.b64decode(last.get('raw_body_base64', ''), validate=True)
            if marker.encode() not in raw.lower():
                raise RuntimeError('The ordinary refusal is not the requested precondition failure')
        else:
            raise RuntimeError('The ordinary mutation unexpectedly accepts a failed precondition')
        cursor, events, timing = bench_notices.page_pass(self.node, before)
        check_order(events, before, cursor)
        if any(event.get('schema_name') in {spec.schema for spec in self.specs} for event in events):
            raise RuntimeError('A refused precondition emits a product notice')
        self.cursor = cursor
        self.node.remember(operation='ordinary-precondition-refusal', phase=phase,
            expected_http_status=status, refusal_marker=marker, refusal_kind_matches=True,
            no_product_notice=True, **timing)
        self.read_all(phase + '-unchanged-values')

    def reopened(self):
        self.read_all('all-values-after-normal-reopen')
        cursor, events, timing = bench_notices.page_pass(self.node, None)
        check_order(events, None, cursor)
        seen = set()
        bench_notices.check_events(events, self.notices, seen)
        own = [event for event in events if event.get('schema_name') in {spec.schema for spec in self.specs}]
        if seen != set(self.notices) or len(own) != len(self.notices):
            raise RuntimeError('Normal reopen loses, repeats, or adds an acknowledged notice')
        self.node.remember(operation='ordinary-reopen-complete', all_values_and_notices_match=True,
                           required_ids=sorted(self.notices), **timing)
