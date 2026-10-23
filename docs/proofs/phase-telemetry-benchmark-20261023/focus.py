#!/usr/bin/env python3
"""Longer write comparison after the mixed primary durable-write result."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import sys
import time

sys.dont_write_bytecode = True
from run import TelemetryBench, cleanup, fixture_digest, file_hash, key, save, stats, COUNT


class FocusedBench(TelemetryBench):
    def workloads(self):
        kind = 'hash' if self.args.config == 'plain' else 'range'
        for n in range(COUNT):
            selector = ({'HashKey': key(n)} if kind == 'hash' else
                        {'HashRangeKey': {'hash': 'measure', 'range': key(n)}})
            self.read(kind, selector, [n], 1)
        width, count = (1, 512) if kind == 'hash' else (32, 128)
        phase = f'{kind}_long_durable_write{width}'
        self.writes(phase, kind, width, count, 'durable', 18000)
        values = [sample['ms'] for sample in self.samples if sample['phase'] == phase]
        block = count // 8
        save(self.output / 'write_blocks.json', [stats(values[i:i+block])
                                                for i in range(0, count, block)])
        samples = [self.request('/api/status?forensics=1')[1] for _ in range(64)]
        self.phases['telemetry_forensics'] = stats(samples)
        self.samples.extend({'phase': 'telemetry_forensics', 'ms': value} for value in samples)
        self.stop()
        self.start()
        for n in range(COUNT):
            selector = ({'HashKey': key(n)} if kind == 'hash' else
                        {'HashRangeKey': {'hash': 'measure', 'range': key(n)}})
            self.read(kind, selector, [n], 1)
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
            summary.update(phases=bench.phases, values_and_write_reopen_verified=True,
                           telemetry_samples_and_aggregates_verified=True)
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
    opts = parser.parse_args()
    os.sched_setaffinity(0, {0})
    opts.output.mkdir(parents=True, exist_ok=False)
    opts.scratch.mkdir(mode=0o700, parents=True, exist_ok=False)
    provenance = json.loads(opts.provenance.read_text())
    for label in ('before', 'after'):
        if file_hash(opts.binaries / label) != provenance['binaries'][label]:
            raise RuntimeError('Binary hash differs')
    def arguments(name, config, label):
        return argparse.Namespace(binary=opts.binaries / label, scratch=opts.scratch / name,
            output=opts.output / name, source_revision=provenance['revisions'][label], config=config,
            rustc=provenance['rustc'], client_cpu=0, server_cpu=1,
            phase_fields=provenance['phase_fields'])
    lock_path = Path.home() / '.cache/fold-db-perf-guard/host-cargo.lock'
    fixtures, index = {}, []
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for config in ('plain', 'preload'):
            fixtures[config] = execute(arguments(f'fixture-{config}', config, 'before'))
            print(f'Fixture ready: {config}', flush=True)
        for pair in range(4):
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
                    index.append(dict(name=name, config=config, pair=pair, label=label,
                                      source_revision=provenance['revisions'][label], seconds=time.time()-started))
                    save(opts.output / 'index.json', index)
        for fixture, summary in fixtures.values():
            assert fixture_digest(fixture.home) == summary['fixture_digest']
            cleanup(fixture.root, fixture.output)
    print('All focused measurements complete', flush=True)


if __name__ == '__main__':
    main()
