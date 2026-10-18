"""Ordinary keyed requests and complete-revision checks for generated records."""
import threading
import time

import range_profile as profile
from write_reuse import generated_values

NUMBERS = list(range(32, 96))


def query_bodies():
    queries = []
    for shape in ('exact', 'range'):
        for count in (1, 16, 64):
            numbers = NUMBERS[:count]
            selector = ({'HashRangeKeys': [['generated', profile.identifier(n)] for n in numbers]}
                        if shape == 'exact' else {'HashRangeRange': {
                            'hash': 'generated', 'start': profile.identifier(numbers[0]),
                            'end': profile.identifier(numbers[-1] + 1)}})
            queries.append({'schema_name': profile.SCHEMA, 'fields': profile.FIELDS,
                            'filter': selector, 'limit': count})
    return queries


class CohortState:
    def __init__(self, revision):
        self.changed = threading.Condition()
        self.acknowledged = self.submitted = revision
        self.done = self.failed = False

    def boundary(self):
        with self.changed:
            return self.acknowledged, self.submitted

    def update(self, name, value):
        with self.changed:
            setattr(self, name, value)
            self.changed.notify_all()


class Node(profile.Node):
    def __init__(self, run, variant, binary):
        super().__init__(run, variant, binary, 'off')
        self.expected = dict(run.expected)
        self.known = {revision: {profile.identifier(n): generated_values(n, revision)
                                 for n in NUMBERS}
                      for revision in range(run.args.write_rounds + 2)}
        self.state = CohortState(0)
        self.batch_sequence = 0

    def status(self, phase):
        result, elapsed, _, request_id = self.call('/api/db/native-status')
        native = result.get('data', result)['native_disk']
        if native.get('writer_failed') is not False:
            raise RuntimeError('The owned native writer reports a failure')
        self.remember(operation='native-status', phase=phase, request_id=request_id,
                      elapsed_nanos=elapsed, result=result)
        return native

    def update_cohort(self, revision, durability):
        self.state.update('submitted', revision)
        mutations = [{'type': 'mutation', 'schema': profile.SCHEMA, 'mutation_type': 'update',
                      'durability': durability, 'key_value': {
                          'hash': 'generated', 'range': profile.identifier(n)},
                      'fields_and_values': generated_values(n, revision)} for n in NUMBERS]
        started = time.perf_counter_ns()
        result, elapsed, _, request_id = self.call('/api/mutations/batch', {'mutations': mutations})
        finished = time.perf_counter_ns()
        actual = result.get('durability')
        if (result.get('ok') is False or result.get('count') != len(NUMBERS)
                or actual not in ('queued', 'durable')
                or durability == 'durable' and actual != 'durable'):
            raise RuntimeError('The complete update cohort lacks its requested acknowledgement')
        self.state.update('acknowledged', revision)
        self.remember(operation='mutation', phase='cohort', batch=len(NUMBERS), revision=revision,
                      requested_durability=durability, actual_durability=actual,
                      request_id=request_id, elapsed_nanos=elapsed,
                      started_nanos=started, finished_nanos=finished, result=result)

    def flush_phase(self, phase):
        started = time.perf_counter_ns()
        result, elapsed, _, request_id = self.call('/api/db/flush', {})
        finished = time.perf_counter_ns()
        data = result.get('data', result)
        receipt = data.get('native_flush', {})
        if (data.get('durable') is not True or 'target_sequence' in receipt
                and receipt['completed_sequence'] < receipt['target_sequence']):
            raise RuntimeError('The flush does not cover its accepted prefix')
        self.remember(operation='flush', phase=phase, request_id=request_id,
                      elapsed_nanos=elapsed, started_nanos=started,
                      finished_nanos=finished, result=result)

    def read_batch(self, phase, repeat):
        floor, submitted_start = self.state.boundary()
        started = time.perf_counter_ns()
        result, elapsed, _, request_id = self.call(
            '/api/queries/batch', {'queries': query_bodies()}, True)
        finished = time.perf_counter_ns()
        _, ceiling = self.state.boundary()
        try:
            revision = self.validate_batch(result, floor, ceiling, phase, repeat, request_id)
        except (KeyError, TypeError, RuntimeError):
            profile.save(self.directory / f'batch-mismatch-{repeat}-{phase}.json', {
                'request_id': request_id, 'phase': phase, 'repeat': repeat,
                'acknowledged_floor': floor, 'submitted_ceiling': ceiling, 'response': result})
            raise
        self.remember(operation='query-batch', phase=phase, repeat=repeat, batch=6,
                      request_id=request_id, elapsed_nanos=elapsed, revision=revision,
                      acknowledged_floor=floor, submitted_start=submitted_start,
                      submitted_ceiling=ceiling, started_nanos=started, finished_nanos=finished,
                      response_sha256=profile.digest(result), complete_revision=True)
        return revision

    def validate_batch(self, result, floor, ceiling, phase, repeat, request_id):
        items = result.get('results', [])
        if result.get('count') != 6 or len(items) != 6 or result.get('ok') is False:
            raise RuntimeError('The query batch does not contain six complete replies')
        revisions, metadata = [], {}
        for index, item in enumerate(items):
            if item.get('status') != 200 or not isinstance(item.get('read_receipt'), dict):
                raise RuntimeError('A query batch item is refused or lacks its read receipt')
            count, shape = (1, 16, 64)[index % 3], ('exact' if index < 3 else 'range')
            response = item['response']
            records = response.get('results', [])
            actual = {row['key']['range']: {name: row['fields'].get(name) for name in profile.FIELDS}
                      for row in records}
            wanted_keys = {profile.identifier(n) for n in NUMBERS[:count]}
            if len(records) != count or set(actual) != wanted_keys:
                raise RuntimeError('An overlapping query returns the wrong known keys')
            matches = [r for r in range(floor, ceiling + 1)
                       if actual == {key: self.known[r][key] for key in wanted_keys}]
            if len(matches) != 1:
                raise RuntimeError('A query returns stale, mixed, or unknown revision values')
            profile.validate_query(response, NUMBERS[:count],
                                   {n: generated_values(n, matches[0]) for n in NUMBERS}, shape)
            self.check_metadata(records, metadata)
            revisions.append(matches[0])
            self.remember(operation='query', phase=phase, repeat=repeat, shape=shape, batch=count,
                          request_id=f'{request_id}/item-{index}', elapsed_nanos=item[
                              'read_receipt']['query_storage_elapsed_nanos'], cost=item['read_receipt'],
                          revision=matches[0], expected_values_match=True,
                          response_sha256=profile.digest(response), returned_records=count)
        if len(set(revisions)) != 1:
            raise RuntimeError('Queries in one shared native scope see different complete revisions')
        return revisions[0]

    @staticmethod
    def check_metadata(records, seen):
        for row in records:
            for field in profile.FIELDS:
                metadata = row['metadata'][field]
                if not metadata.get('atom_uuid') or not metadata.get('molecule_uuid'):
                    raise RuntimeError('A selected field lacks its current atom or molecule identity')
                key = (row['key']['range'], field)
                if seen.setdefault(key, metadata) != metadata:
                    raise RuntimeError('Overlapping reads in one native scope disagree on metadata')
