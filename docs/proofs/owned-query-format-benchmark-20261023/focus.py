#!/usr/bin/env python3
"""Longer multi-get/write follow-up retaining the structured-read prelude."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

sys.dont_write_bytecode = True
from run import OwnedFormatBench, cleanup, fixture_digest, file_hash, key, save, stats, COUNT, SCHEMAS


class FocusedBench(OwnedFormatBench):
    def workloads(self):
        self.response_fingerprints()
        # Repeat the primary prelude, including all structured-read counts.
        for width, count in ((16, 128), (64, 64), (256, 32)):
            for _ in range(count):
                self.nested_read(width)
        selector = {'HashRangeKeys': [['measure', key(n)] for n in range(32)]}
        samples, blocks = [], []
        for block in range(8):
            batch = [self.read('range', selector, list(range(32)), 32) for _ in range(128)]
            samples.extend(batch)
            blocks.append({'block': block, **stats(batch)})
        self.phases['focused_multiget32'] = stats(samples)
        self.samples.extend({'phase': 'focused_multiget32', 'ms': value} for value in samples)
        save(self.output / 'multiget-blocks.json', blocks)
        print(f'focused_multiget32: {self.phases["focused_multiget32"]["median_ms"]:.3f} ms median', flush=True)
        if self.args.config == 'encrypted':
            for n in range(COUNT):
                self.read('hash', {'HashKey': key(n)}, [n], 1)
            self.writes('hash_durable_write1', 'hash', 1, 256, 'durable', 12000)
            self.writes('range_durable_write32', 'range', 32, 64, 'durable', 14000)
        self.stop()
        self.start()
        for start in range(0, COUNT, 32):
            numbers = list(range(start, start+32))
            self.read('range', {'HashRangeKeys': [['measure', key(n)] for n in numbers]}, numbers, 32)
        for n in range(COUNT):
            self.read('hash', {'HashKey': key(n)}, [n], 1)
        self.nested_read(256)
        self.flush()


def execute(args, fixture=None, expected=None):
    bench = FocusedBench(args)
    summary = {'complete': False}
    try:
        if fixture is None:
            bench.start()
            bench.install()
            bench.stop()
            summary['fixture_digest'] = fixture_digest(bench.home)
        else:
            shutil.copytree(fixture, bench.home, dirs_exist_ok=True, copy_function=shutil.copy2)
            observed = fixture_digest(bench.home)
            if observed != expected:
                raise RuntimeError('Cloned fixture differs before startup')
            inputs = json.loads((bench.output / 'inputs.json').read_text())
            inputs.update(experiment_driver_sha256=file_hash(Path(__file__)), fixture_digest=observed,
                          client_cpu=args.client_cpu, server_cpu=args.server_cpu)
            save(bench.output / 'inputs.json', inputs)
            bench.start()
            bench.workloads()
            summary.update(phases=bench.phases, values_metadata_and_reopen_verified=True,
                           response_fingerprints=bench.fingerprints)
        summary['complete'] = True
    except BaseException as error:
        summary['error'] = str(error)
        raise
    finally:
        bench.stop()
        save(bench.output / 'samples.json', bench.samples)
        summary.update(source_revision=args.source_revision, config=args.config,
                       all_owned_processes_stopped=bench.process is None)
        save(bench.output / 'summary.json', summary)
    return bench, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binaries', 'output', 'scratch', 'provenance'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=4)
    parser.add_argument('--client-cpu', type=int, default=0)
    parser.add_argument('--server-cpu', type=int, default=1)
    opts = parser.parse_args()
    if opts.client_cpu == opts.server_cpu or not {opts.client_cpu, opts.server_cpu} <= os.sched_getaffinity(0):
        raise RuntimeError('Need two distinct allowed CPUs')
    os.sched_setaffinity(0, {opts.client_cpu})
    opts.output.mkdir(parents=True, exist_ok=False)
    opts.scratch.mkdir(mode=0o700, parents=True, exist_ok=False)
    provenance = json.loads(opts.provenance.read_text())
    for label in ('before', 'after'):
        if file_hash(opts.binaries / label) != provenance['binaries'][label]:
            raise RuntimeError('Binary hash differs from declared provenance')
    def arguments(name, config, label):
        return argparse.Namespace(binary=opts.binaries / label, scratch=opts.scratch / name,
            output=opts.output / name, source_revision=provenance['revisions'][label], config=config,
            rustc=provenance['rustc'], client_cpu=opts.client_cpu, server_cpu=opts.server_cpu)
    lock_path = Path.home() / '.cache/fold-db-perf-guard/host-cargo.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fixtures, fingerprints, index = {}, {}, []
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for config in ('plain', 'encrypted', 'preload'):
            fixtures[config] = execute(arguments(f'fixture-{config}', config, 'before'))
            print(f'Fixture ready: {config}', flush=True)
        for pair in range(opts.rounds):
            for config in fixtures:
                order = ('after', 'before') if pair % 2 == 0 else ('before', 'after')
                for label in order:
                    name = f'{config}-{pair}-{label}'
                    print(f'Starting {name}', flush=True)
                    fixture, summary = fixtures[config]
                    started = time.time()
                    bench, result = execute(arguments(name, config, label), fixture.home,
                                            summary['fixture_digest'])
                    cleanup(bench.root, bench.output)
                    fingerprints.setdefault(config, result['response_fingerprints'])
                    if fingerprints[config] != result['response_fingerprints']:
                        raise RuntimeError('Full response fingerprint differs within configuration')
                    index.append(dict(name=name, config=config, pair=pair, label=label,
                                      source_revision=provenance['revisions'][label], seconds=time.time()-started))
                    save(opts.output / 'index.json', index)
        for fixture, summary in fixtures.values():
            assert fixture_digest(fixture.home) == summary['fixture_digest']
            cleanup(fixture.root, fixture.output)
    print('All focused measurements complete', flush=True)


if __name__ == '__main__':
    main()
