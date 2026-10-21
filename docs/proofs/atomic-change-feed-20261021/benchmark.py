#!/usr/bin/env python3
"""Quiet ABBA comparison of durable writes and their ordinary change notices."""
import argparse
import os
from pathlib import Path
import shlex
import sys

sys.dont_write_bytecode = True
from bench_support import (SharedHost, UncontrolledHost, default_node, file_identity,
    imported_identities, logical, make_run, measure, quiet_host, require_executable,
    verified_source, verify_files)
import bench_cells
import bench_notices


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('before', 'after'):
        parser.add_argument('--' + name + '-binary', type=Path, required=True)
        parser.add_argument('--' + name + '-source', type=Path, required=True,
                            help='The frozen source inventory file for this executable')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--samples', type=int, default=16,
                        help='HTTP requests per cell; values other than 16 are diagnostics')
    parser.add_argument('--quiet-timeout', type=float, default=300.)
    parser.add_argument('--uncontrolled', action='store_true',
                        help='Diagnostic only; never valid for accepted performance results')
    parser.add_argument('--allow-concurrent', action='store_true',
                        help='Observe concurrent jobs without a quiet gate; report shared-host results')
    args = parser.parse_args()
    if not 1 <= args.samples <= 64 or not 10 <= args.quiet_timeout <= 3600:
        parser.error('Use 1..64 requests and a 10..3600 second quiet deadline')
    if args.uncontrolled and args.allow_concurrent:
        parser.error('Use one host observation mode')
    return args


def freeze(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    versions = {}
    for name in ('before', 'after'):
        binary = require_executable(getattr(args, name + '_binary'))
        versions[name] = {'binary': binary, 'source': verified_source(getattr(args, name + '_source'), binary)}
    frozen = {'captured_utc': measure.now(), 'versions': versions, 'helpers': imported_identities(),
        'order': ['before-r1', 'after-r1', 'after-r2', 'before-r2'],
        'matrix': bench_cells.matrix(args.samples)[0], 'body_bytes': 1024,
        'fields': list(logical.FIELDS), 'requested_durability': 'durable',
        'quiet_preflight_seconds': 0 if args.allow_concurrent or args.uncontrolled else 10,
        'uncontrolled': args.uncontrolled, 'allow_concurrent': args.allow_concurrent,
        'performance_scope': 'uncontrolled_diagnostic' if args.uncontrolled else
            'observed_shared_host' if args.allow_concurrent else 'quiet_host_policy',
        'accepted_performance_recipe': not args.uncontrolled and args.samples == 16,
        'command': [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
        'host_cargo_lock': 'Required externally for strict quiet mode. Explicit concurrent mode permits a shared-host run without it.',
        'timer': 'Prebuilt JSON send through complete HTTP response bytes; excludes JSON decode, hash, check, status, notice reads.',
        'rate': 'First send through final reply for one closed-loop cell, including transport turnaround.',
        'receipts': 'Compact parsed JSON certificates buffered in memory; full original times, IDs, costs, and checks retained.',
        'scope': 'Fresh synthetic homes, no primary or archive; normal reopen only, no crash or hardware-cold proof.'}
    frozen['shell_command'] = shlex.join(frozen['command'])
    measure.profile.save(output / 'command.json', frozen)
    return output, frozen


def prepare_runs(args, output, frozen):
    runs = []
    for label in frozen['order']:
        version = frozen['versions'][label.split('-')[0]]
        proof, node = make_run(Path(version['binary']['path']), version['source'],
                               output, label, args.samples)
        if file_identity(proof.binary)['sha256'] != version['binary']['sha256']:
            raise RuntimeError('A private executable differs from its frozen binary')
        proof.remember({'operation': 'benchmark-definition', 'inputs': frozen})
        runs.append({'label': label, 'proof': proof, 'node': node, 'writes': [], 'timing_complete': False})
    return runs


def time_run(args, run, frozen, specs):
    proof, node = run['proof'], run['node']
    verify_files(frozen['helpers'])
    for version in frozen['versions'].values():
        verify_files({'binary': version['binary'], 'source': version['source']})
    default_node.start(node)
    try:
        for spec in specs:
            logical.install(node, spec)
        observer = (UncontrolledHost() if args.uncontrolled else SharedHost([node.process.pid])
                    if args.allow_concurrent else quiet_host.QuietHost([node.process.pid]))
        with observer:
            observer.set_owned_pids([node.process.pid])
            cells, records = bench_cells.matrix(args.samples)
            cursor = None
            for index, spec in enumerate(specs):
                for base in cells:
                    cell = dict(base, first=base['first'] + index * records)
                    for revision in (0, 1):
                        row, cursor = bench_cells.timed_cell(node, spec, cell, revision, observer,
                            cursor, run['label'].startswith('after'), args.quiet_timeout)
                        run['writes'].append(row)
            run['timing_complete'] = True
    finally:
        if 'observer' in locals():
            run['quiet'] = observer.evidence()
        node.stop()


def reopen_run(run, specs, samples):
    node = run['node']
    default_node.start(node)
    try:
        cells, records = bench_cells.matrix(samples)
        for index, spec in enumerate(specs):
            expected = {index * records: 1}
            logical.read(node, spec, expected, 'first-key-after-normal-reopen', body_bytes=1024)
            for repeat in range(3):
                logical.read(node, spec, expected, f'repeated-key-after-normal-reopen-{repeat}', body_bytes=1024)
            for cell in cells:
                bench_cells.read_all(node, spec, cell['first'] + index * records,
                    cell['batch'] * samples, 1, 'all-values-after-normal-reopen')
        # Replay the ordinary durable feed after reopen and check every measured ID again.
        wanted = {}
        for row in run['proof'].rows:
            if row.get('operation') == 'measured-write':
                for identity in row['result']['mutation_ids']:
                    wanted[identity] = None
        _, events, timing = bench_notices.page_pass(node, None)
        seen = [event['mutation_id'] for event in events if event.get('mutation_id') in wanted]
        if len(seen) != len(wanted) or len(set(seen)) != len(wanted):
            raise RuntimeError('Normal reopen loses or repeats measured notice IDs')
        node.remember(operation='all-notices-after-normal-reopen', expected_ids=sorted(wanted),
                      all_notice_ids_match=True, **timing)
        run['reopen_complete'] = True
    finally:
        node.stop()


def execute(args):
    output, frozen = freeze(args)
    runs, failure = [], None
    previous = os.environ.get('LASTDB_SCHEMA_PRELOAD_SCHEMAS')
    specs = [logical.Spec(kind, 'AtomicNotice' + kind.title()) for kind in ('hash', 'hashrange')]
    os.environ['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = specs[1].schema
    try:
        runs = prepare_runs(args, output, frozen)
        for run in runs:
            time_run(args, run, frozen, specs)
        for run in runs:
            reopen_run(run, specs, args.samples)
            run['proof'].finish(True, None)
    except BaseException as error:
        failure = f'{type(error).__name__}: {error}'
        raise
    finally:
        for run in runs:
            try:
                run['node'].stop()
            finally:
                if not run.get('reopen_complete'):
                    run['proof'].finish(False, failure or 'The run did not complete')
                run['proof'].preserve()
                measure.profile.save(output / run['label'] / 'result.json', {
                    key: value for key, value in run.items() if key not in ('proof', 'node')})
        measure.profile.save(output / 'summary.json', {'complete': failure is None,
            'error': failure, 'finished_utc': measure.now(),
            'accepted_performance': failure is None and frozen['accepted_performance_recipe'],
            'accepted_quiet_performance': failure is None and frozen['accepted_performance_recipe']
                and not args.allow_concurrent,
            'performance_scope': frozen['performance_scope'],
            'runs': [{'label': run['label'], 'root': str(run['proof'].root),
                      'timing_complete': run['timing_complete'],
                      'reopen_complete': run.get('reopen_complete', False)} for run in runs]})
        if previous is None:
            os.environ.pop('LASTDB_SCHEMA_PRELOAD_SCHEMAS', None)
        else:
            os.environ['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = previous


if __name__ == '__main__':
    execute(arguments())
