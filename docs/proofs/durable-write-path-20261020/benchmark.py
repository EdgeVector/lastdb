#!/usr/bin/env python3
"""Time ordinary durable Mini calls over fresh generated Hash and HashRange data."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
from types import SimpleNamespace

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
for directory in ('core-memory-state-20261018', 'core-owned-writes-20261018',
                  'core-clarity-20261018'):
    sys.path.insert(0, str(HERE.parent / directory))
from api import Node, measure
import default_node
import logical


def call(node, path, value=None, cost=False):
    result = measure.profile.Node.call(node, path, value, cost)
    response, elapsed, receipt, identity = result
    node.remember(operation='request', path=path, request=value, result=response,
                  elapsed_nanos=elapsed, cost=receipt, request_id=identity)
    return result


def send(node, payload, ordinal):
    connection = measure.profile.UnixConnection(node.socket)
    identity = f'durable-{node.run.identity}-{ordinal}'
    headers = {'Content-Type': 'application/json', 'X-LastDB-Client': 'durable-write-benchmark',
               'X-LastDB-Request-Id': identity}
    start = time.perf_counter_ns()
    try:
        connection.request('POST', '/api/mutations/batch', payload, headers)
        response = connection.getresponse()
        raw = response.read()
        finish = time.perf_counter_ns()
        return {'request_id': identity, 'http_status': response.status,
                'start_ns': start, 'finish_ns': finish, 'elapsed_ns': finish - start,
                'result': json.loads(raw)}
    finally:
        connection.close()


def read_all(node, spec, first, records, revision, body_bytes, phase):
    width = 64 if spec.kind == 'hash' else min(256, max(1, 4 * 1024 * 1024 // body_bytes))
    for start in range(first, first + records, width):
        expected = {n: revision for n in range(start, min(start + width, first + records))}
        logical.read(node, spec, expected, phase, zero=False, body_bytes=body_bytes)
    node.remember(operation='all-key-readback', schema=spec.schema, geometry=spec.kind,
                  first=first, records=records, revision=revision, body_bytes=body_bytes,
                  chunk_limit=width, phase=phase, all_values_and_membership_checked=True)


def matrix():
    cells = [(batch, writers, 1024, count) for batch, count in ((1, 64), (32, 16), (128, 8))
             for writers in (1, 4)] + [(32, writers, 65536, 8) for writers in (1, 4)]
    first, positioned = 0, []
    for batch, writers, size, count in cells:
        positioned.append((first, batch, writers, size, count))
        first += batch * count
    return positioned, first


def reopen_reads(node, checks):
    queried_schemas = set()
    for spec, first, records, expected, size in checks:
        before = node.status('before-first-values-after-reopen')
        logical.read(node, spec, expected, 'first-values-after-reopen', body_bytes=size)
        observation = node.run.rows[-1]
        after = node.status('after-first-values-after-reopen')
        node.remember(operation='first-values-after-reopen-observation', schema=spec.schema,
            geometry=spec.kind, first_query_for_schema=spec.schema not in queried_schemas,
            request_id=observation['request_id'], costs=observation['costs'],
            schema_key_loads_before=before['schema_keys']['loads'],
            schema_key_loads_after=after['schema_keys']['loads'],
            scope='Whole query costs; the first query for a schema includes any key preload. '
                  'Later groups use new value keys with shared definitions already warm.')
        queried_schemas.add(spec.schema)
        for repeat in range(5):
            logical.read(node, spec, expected, f'values-after-reopen-repeat-{repeat}', zero=False,
                         body_bytes=size)
        if spec.kind == 'hashrange':
            logical.read(node, spec, expected, 'values-after-reopen-bounded', bounded=True,
                         body_bytes=size)
        read_all(node, spec, first, records, 1, size, 'all-values-after-reopen')


def writes(node, spec, first, batch, writers, body_bytes, count, revision):
    requests = [{'mutations': [logical.mutation(spec, number, revision,
        action='create' if revision == 0 else 'update', durability='durable', full=True,
        body_bytes=body_bytes) for number in range(start, start + batch)]}
        for start in range(first, first + count * batch, batch)]
    payloads = [json.dumps(value, separators=(',', ':')).encode() for value in requests]
    before = node.status('before-timed-writes')
    with ThreadPoolExecutor(max_workers=writers) as pool:
        results = list(pool.map(lambda item: send(node, item[1], node.sequence + item[0]),
                                enumerate(payloads)))
    # All proof capture, response checks, hashing, and observations occur outside the timed window.
    for request, result in zip(requests, results):
        node.remember(operation='measured-write', schema=spec.schema, geometry=spec.kind,
            batch=batch, writers=writers, body_bytes=body_bytes, revision=revision,
            request=request, **result)
        if (result['http_status'] != 200 or result['result'].get('count') != batch
                or result['result'].get('durability') != 'durable'):
            raise RuntimeError('The timed write lacks its complete durable acknowledgement')
    node.sequence += len(results)
    after = node.status('after-timed-writes')
    # These global frontiers can include later background work. They are observations,
    # not a substitute for the exact request's mandatory durable completion.
    elapsed = max(row['finish_ns'] for row in results) - min(row['start_ns'] for row in results)
    times = sorted(row['elapsed_ns'] / 1e6 for row in results)
    summary = {'geometry': spec.kind, 'batch': batch, 'writers': writers,
        'body_bytes': body_bytes, 'revision': revision, 'requests': count,
        'records': count * batch, 'wall_ns': elapsed,
        'records_per_second': count * batch * 1e9 / elapsed,
        'median_ms': statistics.median(times), 'p95_ms': times[math.ceil(.95 * len(times)) - 1],
        'request_bytes': sum(map(len, payloads)), 'before': before, 'after': after}
    node.remember(operation='write-summary', **summary)
    print(json.dumps({name: value for name, value in summary.items()
                      if name not in ('before', 'after')}), flush=True)
    expected = {n: revision for n in range(first + count * batch - min(batch, 32),
                                          first + count * batch)}
    logical.read(node, spec, expected, 'immediate-after-durable-write', zero=False,
                 body_bytes=body_bytes)
    if spec.kind == 'hashrange':
        logical.read(node, spec, expected, 'immediate-bounded-range', zero=False, bounded=True,
                     body_bytes=body_bytes)
    read_all(node, spec, first, count * batch, revision, body_bytes,
             'all-values-after-durable-write')
    return summary, expected


def run(args):
    settings = SimpleNamespace(binary=args.binary, variant=args.variant, root=None,
        receipt_dir=args.output, records_per_hash=1024, hashes=2, body_bytes=1024,
        seed_batch=32, read_rounds=5, cap=10000, counts=[32], concurrent_width=4,
        extra_env={}, reads_only=False, source_commit=args.source_commit)
    proof = measure.Run(settings)
    node = Node(proof)
    node.call = lambda path, value=None, cost=False: call(node, path, value, cost)
    specs = [logical.Spec(kind, 'DurableBench' + kind.title()) for kind in ('hash', 'hashrange')]
    os.environ['LASTDB_MUTATION_SYNC_FLUSH'] = '0'
    os.environ['LASTDB_SCHEMA_PRELOAD_SCHEMAS'] = specs[1].schema
    summaries, checks, error = [], [], None
    proof.remember({'operation': 'benchmark-definition',
        'driver_sha256': measure.profile.file_hash(Path(__file__)),
        'request_timer': 'After JSON preparation; socket send through complete response bytes; before JSON parse.',
        'throughput_timer': 'First send through last response; no proof capture or status calls.',
        'durability': 'Explicit durable requests on both releases; full local disk barriers unchanged.',
        'filesystem_cache': 'Uncontrolled; reopen is process-cold, not hardware-cold.'})
    try:
        default_node.start(node)
        for spec in specs:
            logical.install(node, spec)
        cells, records_per_schema = matrix()
        if args.reverse:
            cells.reverse()
        for index, spec in enumerate(specs):
            for offset, batch, writers, size, count in cells:
                first = index * records_per_schema + offset
                for revision in (0, 1):
                    summary, expected = writes(node, spec, first, batch, writers, size, count, revision)
                    summaries.append(summary)
                checks.append((spec, first, batch * count, expected, size))
        node.stop()
        default_node.start(node)
        reopen_reads(node, checks)
        node.status('reopen-complete')
    except BaseException as exc:
        error = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        node.stop()
        proof.finish(error is None, error)
        measure.profile.save(args.output / 'result.json', {'complete': error is None,
            'error': error, 'workflow': 'durable-before-after', 'writes': summaries})
        for path in (args.output / 'receipts.jsonl',):
            if path.exists():
                with path.open('rb') as source, gzip.open(str(path) + '.gz', 'wb') as destination:
                    import shutil
                    shutil.copyfileobj(source, destination)
        print(json.dumps({'complete': error is None, 'root': str(proof.root), 'error': error}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--variant', required=True)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--reverse', action='store_true')
    run(parser.parse_args())
