#!/usr/bin/env python3
"""Capture real same-key conditions and warm-memory purge on an owned daemon."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time

sys.dont_write_bytecode = True
import run as driver


def compare_and_set(node, output):
    schema = driver.schemas.schema_name(0)
    rows, expected = [], {}
    for number in range(24):
        barrier = threading.Barrier(4)
        key = f'cas-{number:03}'

        def one(client):
            connection = driver.schemas.measure.UnixHTTP(node.socket)
            body = {'type': 'mutation', 'schema': schema, 'mutation_type': 'create',
                    'key_value': {'hash': schema, 'range': key},
                    'fields_and_values': {'bucket': schema, 'id': key, 'value': f'writer-{client}'},
                    'expected': {'type': 'absent', 'field': 'value'}}
            barrier.wait()
            try:
                status, receipt, elapsed = driver.schemas.measure.request(connection, '/api/mutation', body)
                return {'client': client, 'status': status, 'receipt': receipt, 'ms': elapsed}
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(one, range(4)))
        winners = [r for r in responses if r['status'] == 200 and r['receipt'].get('durability') == 'durable']
        losers = [r for r in responses if r['status'] == 409]
        rows.append({'key': key, 'responses': responses})
        driver.save(output / 'cas.json', rows)
        if len(winners) != 1 or len(losers) != 3:
            raise RuntimeError('same-key create-if-absent did not select exactly one durable winner')
        expected[key] = hashlib.sha256(f"writer-{winners[0]['client']}".encode()).hexdigest()
    return schema, expected


def sample_memory(node, done, output):
    samples = []
    while not done.is_set():
        samples.append({'time_us': time.time_ns() // 1000,
                        'native': node.call('/api/db/native-status')['native_disk']})
        done.wait(.01)
    driver.save(output / 'memory-samples.json', samples)
    return samples


def memory_pressure(node, output):
    expected = {}
    for number in range(2):
        schema = driver.schemas.schema_name(number)
        items, hashes = [], {}
        for key_number in range(128):
            key = f'large-{key_number:03}'
            value = hashlib.shake_256(f'{schema}-{key}'.encode()).hexdigest(8192)
            hashes[key] = hashlib.sha256(value.encode()).hexdigest()
            items.append({'type': 'mutation', 'schema': schema, 'mutation_type': 'create',
                          'key_value': {'hash': schema, 'range': key},
                          'fields_and_values': {'bucket': schema, 'id': key, 'value': value}})
        receipt = node.call('/api/mutations/batch', {'mutations': items})
        if receipt.get('durability') != 'durable':
            raise RuntimeError('large memory-pressure call lacks a durable receipt')
        expected[schema] = hashes
    done = threading.Event()
    with ThreadPoolExecutor(max_workers=5) as pool:
        sampler = pool.submit(sample_memory, node, done, output)
        try:
            jobs = [pool.submit(lambda: [driver.schemas.check_values(node, expected) for _ in range(8)])
                    for _ in range(4)]
            reads = [row for job in jobs for row in job.result()]
        finally:
            done.set()
        samples = sampler.result()
    if not all(row['all_values_match'] for row in reads):
        raise RuntimeError('a held large read differs under warm-memory pressure')
    status = node.call('/api/db/native-status')['native_disk']
    result = {'batched_reads': len(reads), 'all_values_match': True, 'native': status,
              'max_observed_held_paths': max((r['native']['core_warm_held'] for r in samples), default=0),
              'max_dirty_paths': max((r['native']['core_warm_dirty'] for r in samples), default=0)}
    driver.save(output / 'memory-result.json', result)
    if (status['core_warm_bytes']['used_bytes'] > node.warm_bytes
            or status['core_warm_purged'] == 0 or status['core_warm_dirty'] != 0):
        raise RuntimeError('warm memory did not purge back under its limit')
    return expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--name', default='semantics')
    parser.add_argument('--home-name')
    parser.add_argument('--output-root', type=Path)
    args = parser.parse_args()
    if (os.environ.get('GITHUB_ACTIONS') != 'true'
            or os.environ.get('RUNNER_ENVIRONMENT') != 'github-hosted'):
        raise RuntimeError('captures require GitHub-hosted Actions; local benchmarks are retired')
    scratch = driver.external_path(args.scratch.resolve(strict=True))
    if scratch.stat().st_uid != os.getuid():
        raise RuntimeError('scratch owner differs')
    driver.configure_output(args.output_root)
    output, home = driver.PROOF / args.name, scratch / (args.home_name or ('hm' if args.name == 'semantics' else 'hmf'))
    output.mkdir()
    home.mkdir()
    node = driver.Node(scratch / 'candidate', home, scratch / (args.name + '.log'), 4096, False, workers=8)
    node.profile, node.maintenance, node.warm_bytes = True, True, 262144
    driver.save(output / 'inputs.json', {'home': str(home), 'generated_data_only': True,
                'binary_sha256': hashlib.sha256(node.binary.read_bytes()).hexdigest(),
                'warm_bytes': node.warm_bytes})
    try:
        node.start()
        driver.schemas.seed(node, 2, output)
        schema, cas = compare_and_set(node, output)
        expected = memory_pressure(node, output)
        expected[schema].update(cas)
        driver.save(output / 'forced-stop.json', {'exit_code': node.stop(signal.SIGKILL)})
        node.start()
        readback = driver.schemas.check_values(node, expected)
        driver.save(output / 'recovery.json', readback)
        if not readback['all_values_match']:
            raise RuntimeError('condition winners or large values differ after forced stop')
        driver.save(output / 'shutdown.json', {'exit_code': node.stop()})
    except Exception as error:
        driver.save(output / 'failure.json', {'error': str(error)})
        raise
    finally:
        node.stop()
        node.log.close()
        driver.old.preserve_log(scratch, args.name, output)


if __name__ == '__main__':
    main()
