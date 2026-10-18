#!/usr/bin/env python3
"""Measure native read reuse after ordinary writes to fresh synthetic records."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import range_profile as profile


def generated_values(number, revision):
    tag = f'Layer2Queue:{number}:{revision}'
    body = ''.join(hashlib.sha256(f'{tag}:{chunk}'.encode()).hexdigest()
                   for chunk in range(16))
    return {'title': f'title-{number}-revision-{revision}', 'body': body}


class Node(profile.Node):
    def __init__(self, run, variant, binary):
        super().__init__(run, variant, binary, 'off')
        self.expected = copy.deepcopy(run.expected)
        self.versions = {}

    def mutate(self, numbers, phase, revision=None, operation='update', durability='queued'):
        numbers = list(numbers)
        changes = {}
        mutations = []
        for number in numbers:
            values = (dict(self.expected[number]) if revision is None else
                      generated_values(number, revision)) if operation != 'delete' else {}
            changes[number] = values
            fields = dict(values)
            if operation == 'create':
                fields.update(bucket='generated', id=profile.identifier(number))
            mutation = {'type': 'mutation', 'schema': profile.SCHEMA,
                        'mutation_type': operation, 'durability': durability,
                        'key_value': {'hash': 'generated', 'range': profile.identifier(number)},
                        'fields_and_values': fields}
            mutations.append(mutation)
        result, elapsed, _, request_id = self.call('/api/mutations/batch', {'mutations': mutations})
        if result.get('ok') is False or result.get('count') != len(numbers):
            raise RuntimeError('Mutation reply does not cover the complete generated batch')
        actual = result.get('durability')
        if actual not in ('queued', 'durable') or durability == 'durable' and actual != 'durable':
            raise RuntimeError('Mutation reply has an absent or weaker durability guarantee')
        for number, values in changes.items():
            if operation == 'delete':
                self.expected.pop(number, None)
            else:
                self.expected[number] = values
        self.remember(operation='mutation', phase=phase, mutation_type=operation, batch=len(numbers),
                      requested_durability=durability, actual_durability=actual,
                      request_id=request_id, elapsed_nanos=elapsed, result=result)

    def flush_phase(self, phase):
        result, elapsed, _, request_id = self.call('/api/db/flush', {})
        data = result.get('data', result)
        receipt = data.get('native_flush', {})
        if data.get('durable') is not True or ('target_sequence' in receipt
                and receipt['completed_sequence'] < receipt['target_sequence']):
            raise RuntimeError('Flush reply does not cover its accepted prefix')
        self.remember(operation='flush', phase=phase, request_id=request_id,
                      elapsed_nanos=elapsed, result=result)

    def status_phase(self, phase):
        result, elapsed, _, request_id = self.call('/api/db/native-status')
        self.remember(operation='native-status', phase=phase, request_id=request_id,
                      elapsed_nanos=elapsed, result=result)

    def query(self, numbers, shape, phase, repeat):
        numbers = list(numbers)
        selector = ({'HashRangeKeys': [['generated', profile.identifier(number)] for number in numbers]}
                    if shape == 'exact' else {'HashRangeRange': {
                        'hash': 'generated', 'start': profile.identifier(numbers[0]),
                        'end': profile.identifier(numbers[-1] + 1)}})
        result, elapsed, cost, request_id = self.call('/api/query', {
            'schema_name': profile.SCHEMA, 'fields': profile.FIELDS,
            'filter': selector, 'limit': len(numbers)}, True)
        try:
            self.validate(result, numbers, shape)
        except (KeyError, TypeError, RuntimeError):
            profile.save(self.directory / f'query-mismatch-{self.sequence}.json', {
                'request_id': request_id, 'phase': phase, 'numbers': numbers,
                'shape': shape, 'actual_response': result, 'cost': cost})
            raise
        if cost is None:
            raise RuntimeError('Requested file-work receipt is absent')
        response_hash = profile.digest(result)
        content = {row['key']['range']: {field: row['fields'][field] for field in profile.FIELDS}
                   for row in result['results']}
        metadata_hash = profile.digest({row['key']['range']: row['metadata']
                                        for row in result['results']})
        self.remember(operation='query', phase=phase if repeat >= 0 else f'{phase}-prime',
                      shape=shape, batch=len(numbers), repeat=repeat,
                      request_id=request_id, elapsed_nanos=elapsed, cost=cost,
                      response_sha256=response_hash, content_sha256=profile.digest(content),
                      metadata_sha256=metadata_hash, expected_values_match=True,
                      response_metadata_checked=True, returned_records=len(result['results']))
        if repeat == -1:
            profile.save(self.directory / f'{phase}-{shape}-{len(numbers)}.json', result)
        return {'response': response_hash, 'content': profile.digest(content),
                'metadata': metadata_hash}

    def validate(self, result, numbers, shape):
        records = result.get('results', [])
        wanted = {profile.identifier(number): self.expected[number]
                  for number in numbers if number in self.expected}
        actual = {row['key']['range']: {field: row['fields'].get(field) for field in profile.FIELDS}
                  for row in records}
        if actual != wanted or len(records) != len(wanted):
            raise RuntimeError('Query differs from the generated values or live record count')
        flags = {'returned_count': len(wanted), 'has_more': False, 'next_cursor': None,
                 'unresolved_rows': 0, 'offset': 0, 'limit': len(numbers)}
        if any(result.get(name) != value for name, value in flags.items()):
            raise RuntimeError('Query completeness or page metadata differs')
        absent = len(numbers) - len(wanted)
        if (not isinstance(result.get('tombstoned_rows'), int)
                or not 0 <= result['tombstoned_rows'] <= absent):
            raise RuntimeError('Tombstone count exceeds the known absent keys')
        if shape == 'range' and [row['key']['range'] for row in records] != sorted(wanted):
            raise RuntimeError('Range order differs from the generated key order')
        for row in records:
            self.validate_row(row)

    def validate_row(self, row):
        key = row['key']
        if key.get('hash') != 'generated' or row['fields'].get('bucket') != 'generated':
            raise RuntimeError('The returned hash or required hash field differs')
        if 'id' in row['fields'] and row['fields']['id'] != key['range']:
            raise RuntimeError('The returned range field differs from its key')
        for field in profile.FIELDS:
            metadata = row['metadata'][field]
            if not metadata.get('atom_uuid') or not metadata.get('molecule_uuid'):
                raise RuntimeError('A selected field lacks its atom or molecule identity')
            if metadata.get('source_file_name') is not None or metadata.get('metadata') not in (None, {}):
                raise RuntimeError('Generated writes return unexpected source or custom metadata')
            version = metadata.get('molecule_version')
            if version is not None:
                molecule = metadata['molecule_uuid']
                if not isinstance(version, int) or version < self.versions.get(molecule, 0):
                    raise RuntimeError('A returned molecule version decreases')
                self.versions[molecule] = version


def ordered(nodes, repeat):
    return nodes if repeat % 2 == 0 else list(reversed(nodes))


def measure_phase(run, nodes, phase, counts=None, rounds=None):
    result = {}
    for node in nodes:
        node.status_phase(f'{phase}-start')
    for shape in run.args.shapes:
        for count in run.args.counts if counts is None else counts:
            numbers = list(range(run.args.start, run.args.start + count))
            primes = {node.variant: node.query(numbers, shape, phase, -1) for node in nodes}
            for repeat in range(run.args.read_rounds if rounds is None else rounds):
                for node in ordered(nodes, repeat):
                    current = node.query(numbers, shape, phase, repeat)
                    if current != primes[node.variant]:
                        raise RuntimeError(f'{node.variant} complete response changes within {phase}')
            for node in nodes:
                result[(node.variant, shape, count)] = primes[node.variant]
            print(f'{phase} {shape} {count}: paired reads complete', flush=True)
    for node in nodes:
        node.status_phase(f'{phase}-end')
    return result


def compare_phases(run, earlier, later, phase, fields):
    for key, old in earlier.items():
        new = later[key]
        equal = {field: old[field] == new[field] for field in fields}
        run.remember(dict(operation='phase-comparison', phase=phase, variant=key[0], shape=key[1],
                          batch=key[2], equal=equal, before=old, after=new))
        if not all(equal.values()):
            raise RuntimeError(f'{key[0]} {phase} differs for {key[1]} {key[2]}')


def paired_mutation(nodes, numbers, phase, **kwargs):
    for node in nodes:
        node.mutate(numbers, phase, **kwargs)


def paired_flush(nodes, phase):
    for node in nodes:
        node.flush_phase(phase)


def workload(run, nodes):
    baseline = measure_phase(run, nodes, 'baseline')
    paired_mutation(nodes, range(16), 'unrelated', revision=1)
    accepted = measure_phase(run, nodes, 'unrelated-ack')
    compare_phases(run, baseline, accepted, 'unrelated-values', ('content',))
    paired_flush(nodes, 'unrelated')
    flushed = measure_phase(run, nodes, 'unrelated-flush')
    compare_phases(run, accepted, flushed, 'unrelated-flush', ('response', 'content', 'metadata'))

    paired_mutation(nodes, range(32, 48), 'same-value')
    accepted = measure_phase(run, nodes, 'same-value-ack')
    compare_phases(run, flushed, accepted, 'same-values', ('content',))
    paired_flush(nodes, 'same-value')
    flushed = measure_phase(run, nodes, 'same-value-flush')
    compare_phases(run, accepted, flushed, 'same-value-flush', ('response', 'content', 'metadata'))

    for phase, options in (
            ('related', {'revision': 2}),
            ('delete', {'operation': 'delete', 'durability': 'durable'}),
            ('recreate', {'operation': 'create', 'revision': 3})):
        paired_mutation(nodes, range(32, 48), phase, **options)
        accepted = measure_phase(run, nodes, f'{phase}-ack', [64], 1)
        paired_flush(nodes, phase)
        flushed = measure_phase(run, nodes, f'{phase}-flush', [64], 1)
        compare_phases(run, accepted, flushed, f'{phase}-flush', ('response', 'content', 'metadata'))


def reopen_known_keys(run, nodes):
    final = {node.variant: profile.digest(node.expected) for node in nodes}
    if len(set(final.values())) != 1 or any(len(node.expected) != 320 for node in nodes):
        raise RuntimeError('The programs finish with different generated values')
    profile.save(run.root / 'expected-final.json', nodes[0].expected)
    for node in nodes:
        node.flush_phase('final')
        before_stop = {}
        for first in (0, 256):
            numbers = range(first, min(first + 256, 320))
            for shape in run.args.shapes:
                before_stop[(shape, first)] = node.query(numbers, shape, 'before-normal-stop', first)
        node.stop()
        node.start()
        for first in (0, 256):
            numbers = range(first, min(first + 256, 320))
            for shape in run.args.shapes:
                reopened = node.query(numbers, shape, 'normal-reopen', first)
                if reopened != before_stop[(shape, first)]:
                    raise RuntimeError(f'{node.variant} response metadata differs after normal reopen')
    return final


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('before', 'after', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--read-rounds', type=int, default=21)
    args = parser.parse_args()
    if args.read_rounds < 1:
        parser.error('Use at least one read round')
    args.records, args.body_bytes, args.start = 320, 1024, 32
    args.counts, args.shapes = [1, 16, 64, 256], ['exact', 'range']
    return args


def main():
    run, nodes, errors, final = profile.Run(arguments()), [], [], None
    try:
        nodes = [Node(run, variant, getattr(run.args, variant)) for variant in ('before', 'after')]
        run.inputs(nodes)
        inputs = json.loads((run.root / 'inputs.json').read_text())
        inputs.update(driver_sha256=profile.file_hash(Path(__file__)),
                      helper_sha256=profile.file_hash(Path(profile.__file__)),
                      write_batch=16, measured_read_start=32,
                      unrelated_write_keys=[0, 15], related_write_keys=[32, 47],
                      metadata_rule=('identities, no custom source, nondecreasing exposed versions, '
                                     'full hash equality after flush and normal restart'),
                      metadata_limit='HTTP mutation has no custom metadata input; source_file_name is discarded',
                      response_rule='full hash stable within phases and across each flush',
                      measurement_rule=('first call has a separate prime receipt; repeated groups have '
                                        f'{run.args.read_rounds} calls'),
                      ack_phase_rule='after reply, before forced flush; normal automatic flush can occur',
                      same_value_rule='identical fields with a new ordinary update request')
        profile.save(run.root / 'inputs.json', inputs)
        profile.save(run.root / 'ownership.json', {'run_id': run.identity, 'uid': os.getuid(),
                     'root': str(run.root), 'homes': [str(node.home) for node in nodes],
                     'origin': 'generated-empty'})
        for node in nodes:
            node.seed()
        for node in nodes:
            node.start()
        workload(run, nodes)
        final = reopen_known_keys(run, nodes)
    except Exception as error:
        errors.append(f'{type(error).__name__}: {error}')
    finally:
        for node in nodes:
            try:
                node.stop()
            except Exception as error:
                errors.append(f'{type(error).__name__}: {error}')
        run.summarize()
        profile.save(run.root / 'completion.json', {
            'success': final is not None and not errors, 'errors': errors,
            'finished_utc': profile.now(), 'load_end': os.getloadavg(),
            'final_expected_sha256': final,
            'query_count': sum(row['operation'] == 'query' for row in run.rows),
            'receipt_sha256': profile.receipt_hashes(run, nodes)})
    if errors:
        raise RuntimeError('; '.join(errors))
    profile.save(run.root / 'completed.json', {'finished_utc': profile.now(),
                 'expected_values_match': True,
                 'response_metadata_equal_after_flush_and_reopen': True,
                 'custom_metadata_updates_measured': False,
                 'final_expected_sha256': final})
    print(str(run.root / 'summary.json'), flush=True)


if __name__ == '__main__':
    main()
