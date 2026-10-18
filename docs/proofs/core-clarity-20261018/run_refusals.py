#!/usr/bin/env python3
"""Check the exact native-selector advice through ordinary public requests."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE.parent / 'core-owned-writes-20261018'))
import run_operations as previous
sys.path.insert(0, str(HERE))
import default_node
import supplement

SPEC = importlib.util.spec_from_file_location('clarity_response_freeze', HERE / 'freeze_driver.py')
freeze_driver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(freeze_driver)

V7 = HERE / 'driver-identity-v7.json'
SUPPORTED = ['HashKey', 'HashRangeKey', 'HashRangePrefix', 'HashRangeRange', 'HashRangeKeys']


def inputs():
    data = freeze_driver.verify(V7)
    files = dict(data['files'])
    for path in (V7, Path(__file__).resolve()):
        files[str(path.relative_to(ROOT))] = {'bytes': path.stat().st_size,
                                            'sha256': freeze_driver.file_hash(path)}
    return files


def verify(path):
    data = json.loads(path.read_text())
    if data['files'] != inputs():
        raise RuntimeError('A narrow response driver input differs from its frozen identity')
    return data


def cohort(node):
    previous.logical.install(node, supplement.SPECS[1])
    supplement.layouts(node)
    result = [row for row in node.run.rows
              if row.get('operation') == 'native-unsupported-filter-refusals'][-1]
    lists = [row['response']['remediation']['filter_variants'] for row in result['results']
             if isinstance(row.get('response'), dict)]
    if len(lists) != 3 or any(value != SUPPORTED for value in lists):
        raise RuntimeError('The live refusal advice differs from the exact native selector list')
    node.remember(operation='native-selector-remediation-complete',
        request_id=result['request_id'], structured_refusals=len(lists),
        filter_variants=SUPPORTED, all_exact=True,
        scope='Static response-only correction. Three ordinary pattern refusals advertise the exact five native selectors; all seven unsupported selectors still fail.')


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--freeze-output', type=Path)
    parser.add_argument('--response-identity', type=Path)
    own, remaining = parser.parse_known_args()
    if own.freeze_output:
        files = inputs()
        if files != inputs():
            raise RuntimeError('The response driver changes during capture')
        data = {'version': 1, 'captured_utc': datetime.now(timezone.utc).isoformat(),
            'files': files, 'files_count': len(files),
            'scope': 'Ordinary native-default boot, HashRange declaration, unsupported declarations/selectors, and exact static refusal advice. No benchmark or data-path rerun.'}
        with own.freeze_output.open('x') as stream:
            json.dump(data, stream, sort_keys=True, indent=2)
            stream.write('\n')
        print(json.dumps({'identity': str(own.freeze_output), 'files_count': len(files),
                          'sha256': freeze_driver.file_hash(own.freeze_output)}))
        return 0
    if own.response_identity is None:
        raise RuntimeError('Use an explicit frozen response driver identity')
    identity = verify(own.response_identity)
    previous.Node.start = default_node.start
    previous.cohorts.run = cohort
    original_preserve = previous.preserve

    def preserve(run, options, old_identity):
        original_preserve(run, options, old_identity)
        verify(own.response_identity)
        shutil.copy2(own.response_identity, run.receipt_dir / 'response-driver-identity.json')
        for name in identity['files']:
            target = run.receipt_dir / 'response-driver-inputs' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, target)
        run.remember({'operation': 'response-driver-scope', 'benchmark_result': False,
            'identity_sha256': freeze_driver.file_hash(own.response_identity),
            'data_path_workflows_rerun': False})

    previous.preserve = preserve
    sys.argv = [sys.argv[0], *remaining]
    result = previous.main()
    verify(own.response_identity)
    return result


if __name__ == '__main__':
    raise SystemExit(main())
