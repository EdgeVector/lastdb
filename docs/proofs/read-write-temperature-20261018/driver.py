#!/usr/bin/env python3
"""Run matched Hash and HashRange operations on one fresh generated home."""
import argparse
import json
import os
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
from api import ALL_FIELDS, FIELDS, Node, SCHEMAS, measure
import workload

HERE = Path(__file__).resolve().parent
ENVIRONMENT = {'LASTDB_RESIDENT_KEY_CAP': '10000', 'LASTDB_RESIDENT_PERSIST_MS': '500',
    'LASTDB_RESIDENT_MAX_DEFERRED_BYTES': '134217728', 'LASTDB_RESIDENT_MAX_DEFERRED': '512',
    'LASTDB_MUTATION_SYNC_FLUSH': '0', 'LASTDB_BACKGROUND_FLUSH_MS': '500',
    'LASTDB_SCHEMA_KEY_BYTES': '134217728'}


class Run(measure.Run):
    def __init__(self, args, own):
        super().__init__(args)
        self.label = own.label
        self.revision = {}
        path = self.root / 'inputs.json'
        metadata = json.loads(path.read_text())
        metadata['unused_helper_argument_metadata'] = {key: metadata.pop(key) for key in (
            'schema', 'records_per_hash', 'hashes', 'range_recipe',
            'concurrent_queries', 'concurrent_keys_per_query', 'counts')}
        metadata.update(total_seed_records=512, records_per_schema=256,
            schemas={SCHEMAS['hash']: {'type': 'Hash', 'distinct_hash_keys': 256},
                     SCHEMAS['hashrange']: {'type': 'HashRange', 'hashes': 1, 'ranges': 256}},
            current_label=own.label, round=own.round,
            read_fields=FIELDS, write_readback_fields=ALL_FIELDS,
            read_cases=[{'geometry': kind, 'shape': shape, 'width': width}
                for kind, shape, widths in [('hash', 'exact', (1, 16, 64)),
                    ('hashrange', 'exact', (1, 16, 64)), ('hashrange', 'bounded', (16, 64))]
                for width in widths],
            source_identity=str(own.source_identity.resolve(strict=True)),
            source_identity_sha256=measure.profile.file_hash(own.source_identity),
            environment=dict(ENVIRONMENT, LASTDB_SCHEMA_PRELOAD_SCHEMAS=
                SCHEMAS['hashrange'] if own.label == 'current-on' else ''),
            first_read_scope='One first request after normal process reopen; OS file cache uncontrolled.',
            write_condition_scope='No-prior-value-read and read-primed describe caller operations. They do not claim memory-only internal write preparation.',
            transport_scope='Width1 uses /api/query for both geometries. Hash widths16/64 use /api/queries/batch; HashRange uses one /api/query.')
        measure.profile.save(path, metadata)


def preserve(run, own):
    sources = {name: HERE / name for name in ('driver.py', 'api.py', 'workload.py')}
    sources.update({'measure.py': Path(measure.__file__),
        'range_profile.py': measure.SHARED / 'range_profile.py',
        'ownership.py': measure.SHARED / 'ownership.py'})
    identities = {}
    for name, source in sources.items():
        shutil.copy2(source, run.receipt_dir / name)
        identities[name] = {'source': str(source), 'sha256': measure.profile.file_hash(source)}
    shutil.copy2(own.source_identity, run.receipt_dir / 'source-identity.json')
    measure.profile.save(run.receipt_dir / 'driver-inputs.json', {'sources': identities,
        'expected_binary_sha256': own.expected_sha256, 'label': own.label, 'round': own.round,
        'mode_limit': 'Returned queued mode alone does not establish an internal memory acknowledgement.'})


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--source-identity', type=Path, required=True)
    parser.add_argument('--label', choices=['before', 'current-off', 'current-on'], required=True)
    parser.add_argument('--round', type=int, choices=[1, 2], required=True)
    own, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    args = measure.arguments()
    if args.records_per_hash != 64 or args.hashes != 4 or args.body_bytes != 1024 or args.cap != 10000:
        raise RuntimeError('Use the reviewed fixed generated cohort and path allowance')
    if measure.profile.file_hash(args.binary) != own.expected_sha256:
        raise RuntimeError('The requested executable identity differs')
    own.source_identity.resolve(strict=True)
    run, node = Run(args, own), None
    complete, error = False, None
    environment = dict(ENVIRONMENT, LASTDB_SCHEMA_PRELOAD_SCHEMAS=
                       SCHEMAS['hashrange'] if own.label == 'current-on' else '')
    prior = {name: os.environ.get(name) for name in environment}
    try:
        if measure.profile.file_hash(run.binary) != own.expected_sha256:
            raise RuntimeError('The private executable identity differs')
        preserve(run, own)
        os.environ.update(environment)
        node = Node(run)
        workload.run(node)
        complete = True
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        run.remember({'operation': 'failure', 'error': error})
    finally:
        if node is not None:
            try:
                node.stop()
            except Exception as exc:
                complete, error = False, f'{error or ""}; normal stop: {exc}'
        for name, value in prior.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        run.finish(complete, error)
        if node is not None and (node.directory / 'node.log').is_file():
            shutil.copy2(node.directory / 'node.log', run.receipt_dir / 'node.log')
        measure.profile.save(run.receipt_dir / 'result.json', {
            'complete': complete, 'error': error, 'label': own.label, 'round': own.round,
            'read_checks': sum(row.get('operation') == 'temperature-read' for row in run.rows),
            'write_checks': sum(row.get('operation') == 'temperature-write' for row in run.rows),
            'http_refusals': [row for row in run.rows if row.get('operation') == 'refusal'],
            'query_item_failures': [row for row in run.rows if row.get('operation') == 'query-item-failure']})
    print(json.dumps({'complete': complete, 'error': error, 'root': str(run.root),
                      'receipt_dir': str(run.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
