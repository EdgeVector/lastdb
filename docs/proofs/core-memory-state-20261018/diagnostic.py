#!/usr/bin/env python3
"""Record v2 warm control costs on one new ordinary synthetic record."""
import argparse
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

from api import Node, measure
from driver import ENVIRONMENT
from operational import FIELDS, SCHEMAS, field_values, install, mutate

from family_node import FAMILY_LOG, FamilyNode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--read-family-log', action='store_true')
    own = parser.parse_args()
    if measure.profile.file_hash(own.binary) != own.sha256:
        raise RuntimeError('The requested v2 program identity differs')
    args = SimpleNamespace(binary=own.binary, variant='diagnostic-family' if own.read_family_log else 'diagnostic-v2', root=None,
        receipt_dir=own.receipt_dir, source_commit='frozen-' + own.sha256,
        records_per_hash=64, hashes=1, body_bytes=1024, cap=10000, seed_batch=1,
        read_rounds=3, counts=[1], concurrent_width=1, extra_env={}, reads_only=False)
    environment = dict(ENVIRONMENT, LASTDB_SCHEMA_PRELOAD_SCHEMAS='')
    prior = {name: os.environ.get(name) for name in environment}
    run, node, complete, error = None, None, False, None
    try:
        os.environ.update(environment)
        run = measure.Run(args)
        node = FamilyNode(run) if own.read_family_log else Node(run)
        for name in ('diagnostic.py', 'operational.py', 'api.py', 'driver.py', 'family_node.py'):
            shutil.copy2(Path(__file__).parent / name, run.receipt_dir / name)
        shutil.copy2(own.source, run.receipt_dir / 'source-identity.json')
        measure.profile.save(run.receipt_dir / 'diagnostic-inputs.json', {
            'fixed_read_family_log': FAMILY_LOG if own.read_family_log else None,
            'scope': 'Ordinary generated operations for source attribution; excludes timing comparison.'})
        node.start()
        install(node, 'hash')
        mutate(node, 'hash', [0], 0, 'create', durability='durable')
        node.flush()
        node.stop()
        node.start()
        result, _, _, _ = node.call('/api/schema/' + SCHEMAS['hash'])
        schema = result.get('data', result)['schema']
        schema = schema.get('schema', schema)
        node.remember(operation='memory-state-schema-diagnostic',
            molecule_uuid=schema.get('molecule_uuid'),
            physical_field_map=schema.get('field_molecule_uuids'),
            declared_fields=schema.get('fields'), runtime_fields=schema.get('runtime_fields'))
        for fields in (FIELDS, [], ['bucket'], ['id'], ['title'], ['body']):
            for repeat in range(3):
                log_start = (node.directory / 'node.log').stat().st_size
                result, elapsed, cost, request = node.call('/api/query', {
                    'schema_name': SCHEMAS['hash'], 'fields': fields,
                    'filter': {'HashKey': measure.key(0)}, 'limit': 1}, True)
                rows = result.get('results', [])
                expected = field_values(0, 0)
                if fields:
                    # The co-key query also returns its primary key field.
                    expected = {field: expected[field] for field in set(fields) | {'id'}}
                if len(rows) != 1 or rows[0]['fields'] != expected:
                    raise RuntimeError('The generated diagnostic value projection differs')
                node.remember(operation='memory-state-projection-diagnostic',
                    fields=fields, repeat=repeat, elapsed_nanos=elapsed,
                    request_id=request, cost=cost, expected_values_match=True,
                    node_log_start=log_start,
                    node_log_end=(node.directory / 'node.log').stat().st_size)
        node.status('diagnostic-complete')
        complete = True
    except Exception as exc:
        error = type(exc).__name__ + ': ' + str(exc)
        if run is not None:
            run.remember({'operation': 'failure', 'error': error})
    finally:
        if node is not None:
            node.stop()
        for name, value in prior.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if run is not None:
            run.finish(complete, error)
            shutil.copy2(node.directory / 'node.log', run.receipt_dir / 'node.log')
            measure.profile.save(run.receipt_dir / 'result.json', {'complete': complete, 'error': error})
    print(json.dumps({'complete': complete, 'error': error,
        'root': None if run is None else str(run.root), 'receipt_dir': str(own.receipt_dir)}), flush=True)
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
