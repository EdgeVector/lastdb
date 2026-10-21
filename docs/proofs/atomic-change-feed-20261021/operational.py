#!/usr/bin/env python3
"""Exercise supported ordinary candidate mutation routes on a fresh synthetic home."""
import argparse
import json
import os
from pathlib import Path
import shlex
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
from bench_support import (BufferedRun, HERE, Node, default_node, file_identity, imported_identities,
                           logical, measure, require_executable, verified_source)
from operation_context import Context
import operation_cases
import operation_telemetry


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True,
                        help='The candidate release_verified source inventory file')
    parser.add_argument('--output', type=Path, required=True)
    return parser.parse_args()


def prepare(args):
    binary = require_executable(args.binary)
    source = verified_source(args.source, binary)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    helpers = imported_identities()
    helpers.update({name: file_identity(HERE / name) for name in (
        'operational.py', 'operation_context.py', 'operation_cases.py', 'operation_telemetry.py')})
    command = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]]
    inputs = {'captured_utc': measure.now(), 'binary': binary, 'source': source,
        'helpers': helpers, 'command': command, 'shell_command': shlex.join(command),
        'workflow': 'Ordinary API operations with generated values, complete response checks and normal reopen.',
        'performance_scope': 'Operational request observations only; no isolated performance or causal comparison claim.',
        'key_derivation': 'The required key_value member is {}; schema fields supply the coordinates.',
        'limits': ['No crash, power-loss, cloud, caller-supplied UUID, cross-node or protein proof.',
                   'The mixed request has separate update and erasure phases; this does not claim whole-request atomicity.',
                   'Only ordinary single and batch HTTP routes are used. No test or fixture runner.']}
    measure.profile.save(output / 'command.json', inputs)
    settings = SimpleNamespace(binary=Path(binary['path']), variant='operations', root=None,
        receipt_dir=output / 'run', records_per_hash=1, hashes=2, body_bytes=1024,
        seed_batch=2, read_rounds=1, cap=10000, counts=[1], concurrent_width=4,
        extra_env={}, reads_only=False, source_commit=source['sha256'])
    proof = BufferedRun(settings)
    if file_identity(proof.binary)['sha256'] != binary['sha256']:
        raise RuntimeError('The private candidate executable differs from its verified release')
    path = proof.root / 'inputs.json'
    metadata = json.loads(path.read_text())
    metadata['unused_helper_argument_metadata'] = {key: metadata.pop(key) for key in (
        'schema', 'records_per_hash', 'hashes', 'range_recipe', 'range_bytes',
        'concurrent_queries', 'concurrent_keys_per_query', 'counts', 'total_seed_records')}
    metadata.update(source_identity=source, recipe='operation_cases.run', generated_body_bytes=1024,
                    fields=list(logical.FIELDS), schemas=['AtomicOperationsHash', 'AtomicOperationsHashrange'])
    measure.profile.save(path, metadata)
    proof.remember({'operation': 'ordinary-operation-inputs', **inputs})
    return output, proof, Node(proof)


def execute(args):
    output, proof, node = prepare(args)
    specs = [logical.Spec(kind, 'AtomicOperations' + kind.title()) for kind in ('hash', 'hashrange')]
    context, error = Context(node, specs), None
    previous = os.environ.get('LASTDB_SCHEMA_PRELOAD_SCHEMAS')
    os.environ['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = specs[1].schema
    try:
        default_node.start(node)
        for spec in specs:
            logical.install(node, spec)
        operation_cases.run(context)
        node.status('ordinary-operation-routes-complete')
        operation_telemetry.capture(node, context.calls)
        node.stop()
        default_node.start(node)
        context.reopened()
        node.status('ordinary-operation-normal-reopen-complete')
    except BaseException as failure:
        error = f'{type(failure).__name__}: {failure}'
        raise
    finally:
        try:
            node.stop()
        except BaseException as failure:
            error = f'{error or ""}; node stop: {type(failure).__name__}: {failure}'
            raise
        finally:
            proof.finish(error is None, error)
            proof.preserve()
            measure.profile.save(output / 'result.json', {'complete': error is None, 'error': error,
                'root': str(proof.root), 'writes': context.calls, 'accepted_mutations': len(context.notices),
                'all_successful_writes_durable': error is None,
                'normal_reopen_values_and_notices_match': error is None})
            if previous is None:
                os.environ.pop('LASTDB_SCHEMA_PRELOAD_SCHEMAS', None)
            else:
                os.environ['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = previous


if __name__ == '__main__':
    execute(arguments())
