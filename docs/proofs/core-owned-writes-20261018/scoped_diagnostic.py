#!/usr/bin/env python3
"""Preserve fixed-family diagnostics after an already-failed strict logical read gate."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import run_operations as base


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
    parser.add_argument('--diagnostic-workflow', choices=('scoped', 'protein'), default='scoped')
    own, remaining = parser.parse_known_args()
    current = inputs()
    if own.freeze:
        if current != inputs():
            raise RuntimeError('A diagnostic input changes during capture')
        with own.identity.open('x') as stream:
            json.dump({'files': current, 'scope': __doc__}, stream, sort_keys=True, indent=2)
            stream.write('\n')
        print(json.dumps({'identity': str(own.identity),
                          'sha256': base.freeze_driver.file_hash(own.identity)}))
        return 0
    if json.loads(own.identity.read_text())['files'] != current:
        raise RuntimeError('A diagnostic input differs')
    if '--workflow' in remaining:
        parser.error('Use diagnostic-workflow for the selected traced cohort')
    receipt_parser = argparse.ArgumentParser(add_help=False)
    receipt_parser.add_argument('--receipt-dir', type=Path, required=True)
    receipt_parser.add_argument('--binary', type=Path, required=True)
    receipt_parser.add_argument('--sha256', required=True)
    receipt_args, _ = receipt_parser.parse_known_args(remaining)
    strict_read, observed = base.logical.read, []

    def read_with_preserved_gate(node, *args, **kwargs):
        try:
            return strict_read(node, *args, **kwargs)
        except RuntimeError as error:
            if not str(error).startswith('The accepted logical write still needs a file read:'):
                raise
            phase = args[2] if len(args) > 2 else kwargs.get('phase')
            observed.append({'phase': phase, 'error': str(error)})
            node.remember(operation='diagnostic-continue-after-file-gate', phase=phase,
                error=str(error), strict_acceptance=False,
                scope='Exact values pass before the preserved file-call gate fails. This diagnostic continues only that gate; all value checks remain active.')

    base.logical.read = read_with_preserved_gate
    original_popen, overrides = subprocess.Popen, []
    original_call, original_scoped_call = base.Node.call, base.scoped.View.call

    def timed_call(original):
        def call(node, path, *args, **kwargs):
            started, response = datetime.now(timezone.utc).isoformat(), None
            try:
                response = original(node, path, *args, **kwargs)
                return response
            finally:
                node.remember(operation='diagnostic-call-window', path=path,
                    started_utc=started, finished_utc=datetime.now(timezone.utc).isoformat(),
                    request_id=None if response is None else response[3])
        return call

    base.Node.call, base.scoped.View.call = timed_call(original_call), timed_call(original_scoped_call)

    def traced_popen(command, *args, **kwargs):
        if command and Path(command[0]).name == 'program-lastdbd':
            environment = dict(kwargs['env'])
            if environment.get('LASTDB_ISOLATED_COPY') != '1':
                raise RuntimeError('The numeric diagnostic only targets the owned isolated node')
            program = Path(command[0]).resolve(strict=True)
            root = program.parent
            marker = root / '.synthetic-seed-owner.json'
            if (not marker.is_file() or root.stat().st_uid != os.getuid()
                    or base.freeze_driver.file_hash(program) != receipt_args.sha256):
                raise RuntimeError('The isolated diagnostic program lacks its exact owned identity')
            environment['RUST_LOG'] = 'warn,lastdb::native_read_family=debug'
            kwargs['env'] = environment
            overrides.append({'binary': str(program), 'sha256': receipt_args.sha256,
                              'RUST_LOG': environment['RUST_LOG']})
        return original_popen(command, *args, **kwargs)

    subprocess.Popen = traced_popen
    sys.argv = [sys.argv[0], *remaining, '--workflow', own.diagnostic_workflow]
    try:
        result = base.main()
    finally:
        base.logical.read = strict_read
        subprocess.Popen = original_popen
        base.Node.call, base.scoped.View.call = original_call, original_scoped_call
    target = receipt_args.receipt_dir
    if target.is_dir():
        shutil.copy2(own.identity, target / 'diagnostic-driver-identity.json')
        shutil.copy2(Path(__file__), target / Path(__file__).name)
        base.measure.profile.save(target / 'diagnostic-result.json', {
            'strict_acceptance': False, 'workflow_exit_code': result,
            'diagnostic_workflow': own.diagnostic_workflow,
            'continued_file_gates': observed,
            'child_environment_override': {'RUST_LOG': 'warn,lastdb::native_read_family=debug'},
            'actual_node_overrides': overrides,
            'scope': 'Separate traced diagnosis. Original failed strict receipts stay unchanged. Value failures still stop this workflow.'})
    if inputs() != current:
        raise RuntimeError('A diagnostic input changes during operation')
    return result


if __name__ == '__main__':
    raise SystemExit(main())
