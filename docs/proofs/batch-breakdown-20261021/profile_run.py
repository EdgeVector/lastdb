#!/usr/bin/env python3
"""Capture ordinary durable write calls and fresh request phases on generated data."""
import argparse
import copy
import ctypes
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE.parent / 'atomic-change-feed-20261021'))
from bench_support import (BufferedRun, Node, default_node, file_identity,
                           imported_identities, logical, require_executable, verified_source)
from operation_context import Context, item
import bench_cells
import quiet_host


class ProfileRun(BufferedRun):
    def remember(self, row):
        # Capture expectation maps now, rather than retain a mutable alias.
        super().remember(copy.deepcopy(row))


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def usage(pid):
    library = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    library.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    value = quiet_host.Usage()
    if library.proc_pid_rusage(pid, 2, ctypes.byref(value)):
        raise RuntimeError('Cannot read owned node resource counters')
    return {name: int(getattr(value, name)) for name in ('birth', 'user', 'system', 'reads', 'writes')}


def phase_snapshot(node, calls):
    wanted = {row['request_id'] for row in calls}
    deadline, observations = time.monotonic() + 20, []
    while True:
        result, _, _, identity = node.call('/api/status?recent=1')
        status = result.get('data', result)['status']
        operations = status.get('request_ops', {})
        recent = operations.get('recent', [])
        found = {row.get('request_id') for row in recent} & wanted
        observations.append({'status_request_id': identity, 'sampled_at': status.get('sampled_at'),
                             'sampler': status.get('sampler'), 'matched': sorted(found)})
        if found == wanted or time.monotonic() >= deadline:
            node.remember(operation='fresh-request-phase-snapshot', requested_ids=sorted(wanted),
                matched_ids=sorted(found), request_ops_snapshot=operations,
                status_sampled_at=status.get('sampled_at'), observations=observations,
                all_requested_phases_present=found == wanted)
            return found == wanted
        time.sleep(.25)


def write(node, context, spec, entries, label):
    native_before, before = node.status('profile-before-call'), usage(node.process.pid)
    request = {'mutations': [entry['operation'] for entry in entries]}
    raw = json.dumps(request, separators=(',', ':')).encode()
    response = bench_cells.send(node, raw, node.sequence + 1)
    node.sequence += 1
    after = usage(node.process.pid)
    result = json.loads(response.pop('raw'))
    node.remember(operation='profiled-write-http', geometry=spec.kind, batch=len(entries),
        action=entries[0]['action'], phase=label, request=request, result=result, **response,
        owned_node_usage_before=before, owned_node_usage_after=after)
    wanted = context.accept_reply((result, response['elapsed_ns'], None, response['request_id']),
                                  entries, label)
    native_after = node.status('profile-after-call')
    node.remember(operation='profiled-write-native', request_id=response['request_id'],
        before=native_before, after=native_after,
        scope='All LastStore work in this node process between the two status observations.')
    return wanted


def run(args):
    binary = require_executable(args.binary)
    source = verified_source(args.source, binary)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    settings = SimpleNamespace(binary=Path(binary['path']), variant='batch-profile', root=None,
        receipt_dir=output / 'run', records_per_hash=args.samples * 161, hashes=2, body_bytes=1024,
        seed_batch=args.samples, read_rounds=1, cap=10000, counts=[1, 32, 128],
        concurrent_width=1, extra_env={}, reads_only=False, source_commit=source['sha256'])
    proof = ProfileRun(settings)
    node = Node(proof)
    specs = [logical.Spec(kind, 'BatchProfile' + kind.title()) for kind in ('hash', 'hashrange')]
    context, error, phase_complete = Context(node, specs), None, True
    environment = {'LASTDB_SELF_METRICS_SAMPLE_SECS': '1', 'LASTDB_SELF_METRICS_TO_DB': '0',
                   'LASTDB_SELF_METRICS_RETENTION_DRAIN': '0',
                   'LASTDB_SELF_METRICS_LOG': str(node.home / 'logs/profile-vitals.jsonl'),
                   'LASTDB_SCHEMA_PRELOAD_SCHEMAS': specs[1].schema}
    previous = {name: os.environ.get(name) for name in environment}
    os.environ.update(environment)
    metadata_path = proof.root / 'inputs.json'
    metadata = json.loads(metadata_path.read_text())
    unused = ('schema', 'records_per_hash', 'hashes', 'total_seed_records', 'range_recipe',
              'range_bytes', 'seed_batch', 'read_rounds', 'path_cap', 'counts',
              'concurrent_queries', 'concurrent_keys_per_query', 'candidate_policy')
    metadata['unused_imported_helper_metadata'] = {name: metadata.pop(name) for name in unused}
    metadata.update(schemas=[spec.schema for spec in specs], source_identity=source,
                    body_bytes=1024, fields=list(logical.FIELDS), environment=environment,
                    samples_per_verb_geometry_batch=args.samples,
                    timing_scope='Candidate-only diagnostic, one caller, small fresh database.')
    metadata_path.write_text(json.dumps(metadata, sort_keys=True, indent=2) + '\n')
    helpers = imported_identities()
    helpers.update({name: file_identity(path) for name, path in (
        ('profile_run.py', Path(__file__)), ('bench_cells.py', Path(bench_cells.__file__)),
        ('operation_context.py', Path(sys.modules['operation_context'].__file__)))})
    save(output / 'command.json', {'binary': binary, 'source': source, 'helpers': helpers,
        'command': [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
        'environment': environment, 'samples_per_verb_geometry_batch': args.samples,
        'runtime_log_filter': 'warn,fold_db::write_gate=debug',
        'fields': list(logical.FIELDS), 'body_bytes': 1024, 'writers': 1,
        'scope': 'Candidate-only phase diagnosis on a small fresh database. Concurrent host; no before/after or hardware-capacity claim.',
        'clock_units': 'HTTP elapsed_ns in nanoseconds; request phase *_us in microseconds; process CPU counters in nanoseconds.',
        'timing': 'Prepared JSON send through complete response bytes; preparation/verification/status polling is outside HTTP timing.'})
    try:
        default_node.start(node)
        filter_state = node.call('/api/system/log-filter')[0].get('data', {})
        if filter_state.get('log_filter', {}).get('installed') is True:
            node.call('/api/system/log-filter', {'directive': 'warn,fold_db::write_gate=debug'})
        for spec in specs:
            logical.install(node, spec)
            first = 0
            for batch in (1, 32, 128):
                for revision in (0, 1):
                    label = f'{spec.kind}-batch-{batch}-revision-{revision}'
                    wanted, begin = {}, len(context.calls)
                    for sample in range(args.samples):
                        entries = [item(spec, first + sample * batch + offset, revision,
                            'create' if revision == 0 else 'update', mode='durable') for offset in range(batch)]
                        wanted.update(write(node, context, spec, entries, label))
                    context.observe(wanted)
                    context.read_all(label)
                    phase_complete &= phase_snapshot(node, context.calls[begin:])
                    if not phase_complete:
                        raise RuntimeError('Measured write IDs never reach a fresh request-phase snapshot')
                first += args.samples * batch
        node.stop()
        default_node.start(node)
        context.reopened()
    except BaseException as failure:
        error = f'{type(failure).__name__}: {failure}'
        raise
    finally:
        try:
            node.stop()
        finally:
            proof.finish(error is None, error)
            proof.preserve()
            save(output / 'result.json', {'complete': error is None, 'error': error,
                'all_request_phase_snapshots_complete': phase_complete,
                'accepted_mutations': len(context.notices), 'writes': context.calls,
                'root': str(proof.root), 'normal_reopen_values_and_notices_match': error is None})
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--samples', type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.samples <= 8:
        parser.error('Use between one and eight calls per verb, key type, and batch size')
    run(args)


if __name__ == '__main__':
    main()
