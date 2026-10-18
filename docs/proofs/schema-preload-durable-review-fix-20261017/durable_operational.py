#!/usr/bin/env python3
"""Observe ordinary durable Mini mutations and repeated reads on a fresh home."""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
BASE = Path('/Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016')
HERE = Path(__file__).resolve().parent
HELPERS = BASE / 'docs/proofs/schema-preload-product-20261017'
sys.path.insert(0, str(HELPERS))
import measure

measure.SCHEMA = 'SchemaPreloadDurableReview'


class Run(measure.Run):
    def __init__(self, args):
        super().__init__(args)
        self.created = set()

    def live(self, h, n):
        known = (0 <= h < self.args.hashes and 0 <= n < self.args.records_per_hash) or (h, n) in self.created
        return known and (h, n) not in self.absent


def drained(node, phase):
    native = node.status(phase)
    if native['accepted_batches'] != 0 or native['memory_write_records'] != 0:
        raise RuntimeError(phase + ': the returned boundary retains an accepted raw graph')
    node.remember(operation='durable-boundary-observation', phase=phase,
                  accepted_sequence=native['accepted_sequence'], completed_sequence=native['completed_sequence'],
                  memory_write_records=native['memory_write_records'], accepted_batches=native['accepted_batches'],
                  sequence_limit='The global sequences can include later private background work; a gap alone does not expose raw bodies.')
    return native


def ready(native, expected, phase):
    keys = native['schema_keys']
    if (keys['ready'] < 2 or keys['loading'] or keys['disk_only'] or keys['failed']
            or expected is not None and (keys['loads'] != expected['loads'] or keys['ready'] != expected['ready'])):
        raise RuntimeError(phase + ': the complete domains lost Ready or reloaded')
    return keys


def reads(node, phase, expected):
    exact = [(0, n) for n in [0, 1, 2, 3, 4, 5, 6, 7, 32, 33, 34]] + [(1, 0), (1, 1)]
    first = None
    for repeat in range(2):
        before = node.status(phase + f'-repeat-{repeat}-before')
        answer = node.query(exact, 'exact', phase + '-exact', repeat)
        for h in range(2):
            node.query([(h, n) for n in range(36)], 'range', phase + '-fixed-hash-range', repeat)
        after = node.status(phase + f'-repeat-{repeat}-after')
        keys = ready(after, expected, phase)
        if repeat and after['core_warm_hits'] <= before['core_warm_hits']:
            raise RuntimeError(phase + ': repeated reads do not use the warm values')
        node.remember(operation='ready-membership-and-warm-observation', phase=phase, repeat=repeat,
                      live_generated_records=sum(node.run.live(h, n) for h in range(2) for n in range(36)),
                      schema_keys=keys, core_warm_hits_delta=after['core_warm_hits'] - before['core_warm_hits'],
                      core_warm_misses_delta=after['core_warm_misses'] - before['core_warm_misses'],
                      values_and_membership_checked=True)
        first = first or answer
    return first, keys


def atom(answer, h, n, field):
    for row in answer['results']:
        if row['key'] == {'hash': measure.bucket(h), 'range': measure.key(n)}:
            return row['metadata'][field]['atom_uuid']
    raise RuntimeError('The selected live record is absent')


def workload(node):
    node.seed()
    node.start()
    status, _, _, _ = node.call('/api/status')
    memory = status.get('data', status)['status']['memory_budget']
    if memory['deferred_persist_cap_bytes'] != 1:
        raise RuntimeError('The one-byte deferred cap is absent from public status')
    old, keys = reads(node, 'prime-ready-and-warm', None)
    for operation, pairs, revision in [
            ('update', [(0, 0), (0, 1)], 1),
            ('create', [(0, 32), (0, 33)], None),
            ('delete', [(0, 2), (0, 3)], None)]:
        before = drained(node, 'before-durable-' + operation)
        node.mutate(pairs, 'durable-' + operation, operation=operation, durability='durable', revision=revision)
        if operation == 'create':
            node.run.created.update(pairs)
        after = drained(node, 'durable-' + operation + '-ack')
        ready(after, keys, operation + '-ack')
        current, _ = reads(node, 'after-durable-' + operation, keys)
        if operation == 'update':
            for h, n in pairs:
                for field in measure.FIELDS:
                    if atom(old, h, n, field) == atom(current, h, n, field):
                        raise RuntimeError('The changed value retains its old atom identity')
            node.remember(operation='warm-invalidation-value-observation',
                          phase='durable-update', changed_values_and_atom_identities_match=True,
                          core_paths_before=before['core_warm_paths'], core_paths_after_ack=after['core_warm_paths'])
    node.move((0, 4), (0, 34))
    node.run.created.add((0, 34))
    ready(drained(node, 'durable-key-move-ack'), keys, 'key-move-ack')
    reads(node, 'after-durable-key-move', keys)
    node.mutate([(0, 1)], 'queued-request-one-byte-cap', durability='queued', revision=2)
    boundary = drained(node, 'queued-low-cap-returned-boundary')
    ready(boundary, keys, 'queued-low-cap')
    node.remember(operation='queued-low-cap-observation', source_requires_inline_durable=True,
                  requested_durability='queued', returned_ack_is_not_fallback_proof=True,
                  accepted_sequence=boundary['accepted_sequence'], completed_sequence=boundary['completed_sequence'],
                  raw_graph_records=boundary['memory_write_records'], deferred_byte_cap=1)
    reads(node, 'after-queued-low-cap', keys)
    node.flush()
    ready(drained(node, 'explicit-flush-complete'), keys, 'explicit-flush')
    reads(node, 'after-explicit-flush', keys)
    node.stop()
    node.start()
    reads(node, 'normal-reopen', None)
    drained(node, 'normal-reopen-complete')


def preserve(run):
    sources = {
        'measure.py': Path(measure.__file__),
        'range_profile.py': measure.SHARED / 'range_profile.py',
        'ownership.py': measure.SHARED / 'ownership.py',
        'queue.rs': BASE / 'fold_db/crates/core/src/storage/disk_pack/queue.rs',
        'view.rs': BASE / 'fold_db/crates/core/src/storage/disk_pack/view.rs',
        'request.rs': BASE / 'fold_db/crates/core/src/storage/disk_pack/transaction/request.rs',
        'transaction.rs': BASE / 'fold_db/crates/core/src/storage/disk_pack/transaction.rs',
        'write.rs': BASE / 'fold_db/crates/core/src/storage/disk_pack/write.rs',
        'read.rs': BASE / 'fold_db/crates/core/src/storage/disk_pack/read.rs',
        'native.rs': BASE / 'fold_db/crates/core/src/fold_db_core/mutation_manager/write/native.rs',
        'env_parse.rs': BASE / 'fold_db/crates/core/src/memory_budget/env_parse.rs',
        'create_update_batch.rs': BASE / 'fold_db/crates/core/src/fold_db_core/mutation_manager/write/create_update_batch.rs',
        'source_invalidation.rs': BASE / 'fold_db/crates/core/src/resident/core_set/sources.rs',
        'schema_key_publication.rs': BASE / 'fold_db/crates/core/src/storage/disk_pack/schema_keys/publication.rs',
        'durable_operational.py': Path(__file__)}
    folder = run.receipt_dir / 'source-inputs'
    folder.mkdir(exist_ok=False)
    manifest = {}
    for name, source in sources.items():
        shutil.copy2(source, folder / name)
        manifest[str(source)] = {'copy': 'source-inputs/' + name,
                                 'sha256': measure.profile.file_hash(folder / name)}
    measure.profile.save(run.receipt_dir / 'source-inputs.json', manifest)


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--expected-sha256', required=True)
    own, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    args = measure.arguments()
    if args.records_per_hash != 32 or args.hashes != 2 or measure.profile.file_hash(args.binary) != own.expected_sha256:
        raise RuntimeError('Use the reviewed 64-record configuration and exact fixed binary')
    names = {'LASTDB_RESIDENT_KEY_CAP': str(args.cap), 'LASTDB_RESIDENT_PERSIST_MS': '500',
             'LASTDB_RESIDENT_MAX_DEFERRED_BYTES': '1', **args.extra_env}
    previous = {name: os.environ.get(name) for name in names}
    run, node, complete, error = Run(args), None, False, None
    try:
        if measure.profile.file_hash(run.binary) != own.expected_sha256:
            raise RuntimeError('The fixed binary changed during its private copy')
        preserve(run)
        os.environ.update(names)
        node = measure.Node(run)
        workload(node)
        complete = True
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        run.remember({'operation': 'failure', 'error': error})
    finally:
        if node is not None:
            try:
                node.stop()
            except Exception as exc:
                complete, error = False, f'{error or ""}; stop: {exc}'
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        run.finish(complete, error)
        if node is not None and (node.directory / 'node.log').is_file():
            shutil.copy2(node.directory / 'node.log', run.receipt_dir / 'node.log')
        measure.profile.save(run.receipt_dir / 'operational-result.json', {
            'complete': complete, 'error': error, 'fixed_binary_sha256': own.expected_sha256,
            'environment': names, 'source_and_returned_ack_limit':
            'The one-byte cap cannot reserve a nonempty native write; source selects durable fallback. The queued receipt alone does not report that fallback.',
            'scope': 'Fresh generated Mini API workload. No test hooks or diagnostic binary.'})
    print(json.dumps({'complete': complete, 'error': error, 'root': str(run.root), 'receipt_dir': str(run.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
