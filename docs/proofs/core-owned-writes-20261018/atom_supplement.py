#!/usr/bin/env python3
"""Check public Atom UUID reads on generated accepted writes through normal APIs."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import run_operations as base


def cohort(node):
    spec = base.cohorts.SPECS[1]
    base.logical.install(node, spec)
    base.logical.read(node, spec, {999999: None}, 'atom-empty-definition-and-keys')
    base.logical.write(node, [base.logical.mutation(spec, number, 60, 'create')
                             for number in range(4)], 'atom-complete-create')
    node.status('atom-after-ack-before-value-read')
    response, _, cost, request = node.call('/api/query', base.logical.query(spec, list(range(4))), True)
    base.logical.check_response(spec, dict.fromkeys(range(4), 60), response)
    if base.logical.REQUIRE_WRITE_THROUGH and cost.get('read_operations') != 0:
        raise RuntimeError('The supplemental first logical read requires file reads')
    selected = [(record['metadata'][field]['atom_uuid'], record['fields'][field])
                for record in response['results'] for field in ('title', 'body')]
    node.remember(operation='core-owned-atom-metadata-selection', request_id=request,
        selected_uuids=[uuid for uuid, _ in selected], cost=cost,
        scope='The mutation reply has no Atom UUID. One value query obtains UUID metadata before the direct UUID calls.')

    def read_atom(pair):
        uuid, expected = pair
        result, _, receipt, request_id = node.call('/api/atom/' + uuid, cost=True)
        value = result.get('data', result)
        if value.get('atom_uuid') != uuid or value.get('content') != expected:
            raise RuntimeError('The public Atom UUID read returns another body')
        node.remember(operation='core-owned-atom-uuid-read', atom_uuid=uuid,
            request_id=request_id, cost=receipt,
            scope='The direct UUID wrapper follows a zero-file logical metadata query. The API has no batch Atom endpoint; independent requests run in parallel.')

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(read_atom, selected))
    node.status('atom-after-direct-uuid-reads-before-explicit-flush')
    base.logical.drained(node, 'atom-supplement-flushed')


def inputs():
    result = base.freeze_driver.inputs()
    result[str(Path(__file__).resolve().relative_to(ROOT))] = {
        'sha256': base.freeze_driver.file_hash(Path(__file__)),
        'bytes': Path(__file__).stat().st_size}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--identity', type=Path, required=True)
    parser.add_argument('--freeze', action='store_true')
    own, remaining = parser.parse_known_args()
    current = inputs()
    if own.freeze:
        if current != inputs():
            raise RuntimeError('A supplemental driver input changes during capture')
        with own.identity.open('x') as stream:
            json.dump({'files': current, 'scope': __doc__}, stream, sort_keys=True, indent=2)
            stream.write('\n')
        print(json.dumps({'identity': str(own.identity),
                          'sha256': base.freeze_driver.file_hash(own.identity)}))
        return 0
    if json.loads(own.identity.read_text())['files'] != current:
        raise RuntimeError('A supplemental driver input differs')
    if '--workflow' in remaining:
        parser.error('The supplemental workflow selects logical automatically')
    receipt_parser = argparse.ArgumentParser(add_help=False)
    receipt_parser.add_argument('--receipt-dir', type=Path, required=True)
    receipt_args, _ = receipt_parser.parse_known_args(remaining)
    base.cohorts.run = cohort
    sys.argv = [sys.argv[0], *remaining, '--workflow', 'logical']
    result = base.main()
    target = receipt_args.receipt_dir
    if target.is_dir():
        shutil.copy2(own.identity, target / 'supplemental-driver-identity.json')
        shutil.copy2(Path(__file__), target / Path(__file__).name)
    if inputs() != current:
        raise RuntimeError('A supplemental driver input changes during operation')
    return result


if __name__ == '__main__':
    raise SystemExit(main())
