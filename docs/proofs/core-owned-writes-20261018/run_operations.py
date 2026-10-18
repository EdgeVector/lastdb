#!/usr/bin/env python3
"""Run ordinary core-owned write cohorts on new generated Mini homes."""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE.parent / 'core-memory-state-20261018'))
sys.path.insert(0, str(HERE.parent / 'core-memory-completion-20261018'))
import operations as completion
from api import Node, measure
from driver import ENVIRONMENT
sys.path.insert(0, str(HERE))
import cohorts
import freeze_driver
import logical
import memory
import protein
import replay
import scoped


def prior_completion(node):
    completion.prior_coverage(node)
    completion.absent_then_insert(node)
    completion.unchanged_after_writes(node)
    completion.equal_content_copies(node)
    completion.constant_membership_allowance(node, 24)
    completion.hash_all.run(node)
    logical.drained(node, 'prior-completion-workload-flushed')


def preserve(run, own, identity):
    if measure.profile.file_hash(run.binary) != own.sha256:
        raise RuntimeError('The private executable differs')
    for name, expected in identity['files'].items():
        source = ROOT / name
        if measure.profile.file_hash(source) != expected['sha256']:
            raise RuntimeError('The exact driver input differs: ' + name)
        target = run.receipt_dir / 'driver-source-inputs' / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    shutil.copy2(own.driver_identity, run.receipt_dir / 'driver-identity.json')
    shutil.copy2(own.source, run.receipt_dir / 'source-identity.json')
    measure.profile.save(run.receipt_dir / 'driver-inputs.json', {
        'role': own.role, 'binary_sha256': own.sha256,
        'workflow': own.workflow,
        'driver_identity_sha256': measure.profile.file_hash(own.driver_identity),
        'source_identity_sha256': measure.profile.file_hash(own.source),
        'pressure_deferred_byte_cap': 16 * 1024 * 1024,
        'pressure_scope': 'Sixteen 32KiB records, four concurrent bounded readers, six ordered overwrite batches, direct model body/metadata owners and published memory observations. This is operational evidence, not a timed benchmark.',
        'system_route_scope': 'Replay remains private disk-first and must invalidate old core state before success. Inactive-store imports are outside active logical write-through.'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--driver-identity', type=Path, required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--role', choices=('before', 'candidate'), required=True)
    parser.add_argument('--model-field', default='core_model')
    parser.add_argument('--workflow', choices=('all', 'logical', 'related', 'protein',
                                               'scoped', 'pressure', 'replay'), default='all')
    own = parser.parse_args()
    identity = freeze_driver.verify(own.driver_identity)
    if measure.profile.file_hash(own.binary) != own.sha256:
        raise RuntimeError('The executable differs from the declared identity')
    if json.loads(own.source.read_text())['binary_sha256'] != own.sha256:
        raise RuntimeError('The source identity names another executable')
    logical.REQUIRE_WRITE_THROUGH = own.role == 'candidate'
    args = SimpleNamespace(binary=own.binary, variant='core-owned-' + own.role, root=None,
        receipt_dir=own.receipt_dir, source_commit='frozen-' + own.sha256,
        records_per_hash=64, hashes=4, body_bytes=1024, cap=10000, seed_batch=64,
        read_rounds=6, counts=[16, 64], concurrent_width=16, extra_env={}, reads_only=False)
    preload = [completion.previous.SCHEMAS['hashrange'], cohorts.SPECS[1].schema,
        'CoreOwnedProteinHashRange', 'CoreOwnedScopedHashRange',
        'CoreOwnedMemoryPressure', 'CoreOwnedReplayHashRange']
    environment = dict(ENVIRONMENT, LASTDB_SCHEMA_PRELOAD_SCHEMAS=','.join(preload))
    saved = {name: os.environ.get(name) for name in (*environment, 'LASTDB_PROOF_REPLAY')}
    run, nodes, complete, error = None, [], False, None
    try:
        os.environ.update(environment)
        os.environ.pop('LASTDB_PROOF_REPLAY', None)
        run = measure.Run(args)
        preserve(run, own, identity)
        if own.workflow in ('all', 'logical', 'related', 'protein', 'scoped'):
            node = Node(run)
            nodes.append(node)
            if own.workflow == 'all':
                prior_completion(node)
            else:
                node.start()
            if own.workflow in ('all', 'logical'):
                cohorts.run(node)
            if own.workflow in ('all', 'related', 'protein'):
                protein.run(node)
            if own.workflow in ('all', 'related', 'scoped'):
                scoped.run(node)
            node.stop()
        if own.workflow in ('all', 'pressure'):
            os.environ['LASTDB_RESIDENT_MAX_DEFERRED_BYTES'] = str(16 * 1024 * 1024)
            pressure = replay.NamedNode(run, 'pressure')
            nodes.append(pressure)
            pressure.start()
            memory.run(pressure, own.role == 'candidate', own.model_field)
            pressure.stop()
            os.environ['LASTDB_RESIDENT_MAX_DEFERRED_BYTES'] = environment['LASTDB_RESIDENT_MAX_DEFERRED_BYTES']
        if own.workflow in ('all', 'replay'):
            replay.run(run)
        freeze_driver.verify(own.driver_identity)
        complete = True
    except Exception as exc:
        error = type(exc).__name__ + ': ' + str(exc)
        if run is not None:
            run.remember({'operation': 'failure', 'error': error})
    finally:
        for node in nodes:
            try:
                node.stop()
            except Exception as exc:
                complete, error = False, (error or '') + '; normal stop: ' + str(exc)
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if run is not None:
            run.finish(complete, error)
            for name in ('c', 'p', 'r', 't'):
                path = run.root / name / 'node.log'
                if path.is_file():
                    shutil.copy2(path, run.receipt_dir / ('node-' + name + '.log'))
            counts = {}
            for row in run.rows:
                operation = row.get('operation', 'unknown')
                counts[operation] = counts.get(operation, 0) + 1
            measure.profile.save(run.receipt_dir / 'result.json', {
                'complete': complete, 'error': error, 'role': own.role,
                'workflow': own.workflow,
                'operation_counts': counts, 'benchmark_result': False})
    print(json.dumps({'complete': complete, 'error': error,
        'root': None if run is None else str(run.root), 'receipt_dir': str(own.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
