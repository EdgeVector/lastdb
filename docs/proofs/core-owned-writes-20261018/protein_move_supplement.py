#!/usr/bin/env python3
"""Use partial public writes to move one protein member without removal of equal-content peers."""
import argparse
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import run_operations as base

HASHES = ('core-owned-partial-move-a', 'core-owned-partial-move-b', 'core-owned-partial-move-c')


class MoveSpec(base.logical.Spec):
    def values(self, number, revision, body_bytes=1024):
        return {'bucket': HASHES[0] if number == 1 else self.hash,
            'id': base.logical.measure.key(number), 'title': 'equal-generated-protein-title',
            'body': base.logical.measure.profile.body(8675309, body_bytes)}


def verify_all(node, source, targets, current, phase, shared):
    queries = [base.logical.query(source, [number]) for number in (0, 1)]
    queries += [base.logical.query(target, [0, 1]) for target in targets]
    response, _, _, request_id = node.call('/api/queries/batch', {'queries': queries}, True)
    items = response.get('results', [])
    if response.get('count') != len(queries) or len(items) != len(queries):
        raise RuntimeError('The partial-move batch lacks a complete result array')
    positive, costs = [], []
    for index, item in enumerate(items):
        if item.get('status') != 200 or not item.get('read_receipt'):
            raise RuntimeError('The partial-move keyed query fails or lacks its file receipt')
        if index < 2:
            spec, expected = source, {index: 70}
        else:
            spec = targets[index - 2]
            expected = {0: 70 if spec.hash == current else None,
                        1: 70 if spec.hash == HASHES[0] else None}
        base.logical.check_response(spec, expected, item['response'])
        positive.extend(item['response']['results'])
        costs.append(item['read_receipt'])
    for record in positive:
        for field in ('title', 'body'):
            uuid = record['metadata'][field]['atom_uuid']
            if field in shared and uuid != shared[field]:
                raise RuntimeError('A partial key move replaces an unchanged shared content UUID')
            shared.setdefault(field, uuid)
    node.remember(operation='core-owned-partial-protein-move-read', phase=phase,
        current_hash=current, request_id=request_id, shared_content_uuids=shared,
        file_read_calls=sum(cost['read_operations'] for cost in costs),
        all_source_and_member_values_match=True,
        scope='Two distinct IDs share equal title/body content. Every known old member key is checked. Public query metadata exposes UUIDs, not move or content clocks.')


def cohort(node):
    source = MoveSpec('hash', 'CoreOwnedProteinHash', HASHES[0])
    targets = [MoveSpec('hashrange', 'CoreOwnedProteinHashRange', value) for value in HASHES]
    base.protein.install(node, source)
    base.protein.install(node, targets[0])
    queries = [base.logical.query(source, [number]) for number in (0, 1)]
    queries += [base.logical.query(target, [0, 1]) for target in targets]
    node.call('/api/queries/batch', {'queries': queries}, True)
    base.logical.write(node, [base.logical.mutation(spec, number, 70, 'create')
        for spec in (source, targets[0]) for number in (0, 1)], 'partial-protein-create-both-layouts')
    shared = {}
    node.status('partial-protein-create-after-ack')
    verify_all(node, source, targets, HASHES[0], 'partial-protein-initial', shared)
    for index, target_hash in enumerate((HASHES[1], HASHES[2], HASHES[0], HASHES[1], HASHES[0]), 1):
        source = MoveSpec('hash', source.schema, target_hash)
        mutation = base.logical.mutation(source, 0, 70)
        mutation['fields_and_values'] = {'bucket': target_hash}
        base.logical.write(node, [mutation], 'partial-protein-bucket-only-' + str(index))
        node.status('partial-protein-move-after-ack-' + str(index))
        verify_all(node, source, targets, target_hash, 'partial-protein-after-move-' + str(index), shared)
        if index == 3:
            base.logical.drained(node, 'partial-protein-first-return-flushed')
            node.stop()
            node.start()
            verify_all(node, source, targets, target_hash, 'partial-protein-return-normal-reopen', shared)
    base.logical.drained(node, 'partial-protein-final-flushed')
    node.remember(operation='core-owned-partial-protein-move-complete', moves=5,
        untouched_equal_content_record=1, moved_record=0, shared_content_uuids=shared,
        contract='docs/lastdb-canonical-model.md:132-133',
        scope='Only bucket changes after the initial full creates. Title/body content and UUIDs stay unchanged through repeated moves and reopen. No internal clock or record-envelope creation claim.')


def inputs():
    result = base.freeze_driver.inputs()
    path = Path(__file__).resolve()
    result[str(path.relative_to(ROOT))] = {'sha256': base.freeze_driver.file_hash(path),
                                          'bytes': path.stat().st_size}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--identity', type=Path, required=True)
    parser.add_argument('--freeze', action='store_true')
    own, remaining = parser.parse_known_args()
    current = inputs()
    if own.freeze:
        if current != inputs():
            raise RuntimeError('A partial-move driver input changes during capture')
        with own.identity.open('x') as stream:
            json.dump({'files': current, 'scope': __doc__}, stream, sort_keys=True, indent=2)
            stream.write('\n')
        print(json.dumps({'identity': str(own.identity),
                          'sha256': base.freeze_driver.file_hash(own.identity)}))
        return 0
    if json.loads(own.identity.read_text())['files'] != current:
        raise RuntimeError('A partial-move driver input differs')
    if '--workflow' in remaining:
        parser.error('The partial-move supplement selects logical automatically')
    receipt_parser = argparse.ArgumentParser(add_help=False)
    receipt_parser.add_argument('--receipt-dir', type=Path, required=True)
    receipt_args, _ = receipt_parser.parse_known_args(remaining)
    base.cohorts.run = cohort
    sys.argv = [sys.argv[0], *remaining, '--workflow', 'logical']
    result = base.main()
    if receipt_args.receipt_dir.is_dir():
        shutil.copy2(own.identity, receipt_args.receipt_dir / 'supplemental-driver-identity.json')
        shutil.copy2(Path(__file__), receipt_args.receipt_dir / Path(__file__).name)
    if inputs() != current:
        raise RuntimeError('A partial-move driver input changes during operation')
    return result


if __name__ == '__main__':
    raise SystemExit(main())
