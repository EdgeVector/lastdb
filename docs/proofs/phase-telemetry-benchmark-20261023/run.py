#!/usr/bin/env python3
"""Same-fixture read/write comparison for the telemetry phase declaration."""
import argparse
import fcntl
import json
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output
import shutil
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'native-routing-benchmark-20261023'))
from cleanup import cleanup
from controlled import ControlledBench, fixture_digest
from benchmark import COUNT, SCHEMAS, file_hash, key, save, stats


class TelemetryBench(ControlledBench):
    def __init__(self, args):
        super().__init__(args)
        self.settings['LASTDB_SELF_METRICS_SAMPLE_SECS'] = '1'
        inputs = json.loads((self.output / 'inputs.json').read_text())
        inputs['settings'] = self.settings
        save(self.output / 'inputs.json', inputs)

    def writes(self, phase, kind, width, count, mode, revision):
        super().writes(phase, kind, width, count, mode, revision)
        self.capture_telemetry(phase, kind)

    def capture_telemetry(self, phase, kind):
        deadline = time.monotonic() + 10
        while True:
            snapshot = self.call('/api/status?forensics=1')['status']
            ops = snapshot['request_ops']
            phased = [sample for sample in ops.get('recent', []) if sample.get('phases')]
            mutations = [sample for sample in phased if sample['kind'] == 'mutation_batch'
                         and sample.get('schema') == SCHEMAS[kind]]
            if mutations and ops.get('top_by_total_ms'):
                break
            if time.monotonic() > deadline:
                raise RuntimeError('Mutation phases/aggregate telemetry missing')
            time.sleep(.1)
        for sample in phased:
            if any(name not in self.args.phase_fields or not isinstance(value, int) or value <= 0
                   for name, value in sample['phases'].items()):
                raise RuntimeError('Phase JSON vocabulary or zero omission differs')
        # All retained samples from this writer's completed batch must be included
        # in the cumulative aggregate. Status can be cached, so assert only the
        # snapshot's own ring-versus-aggregate relation, never a freshness bound.
        aggregates = ops['top_by_count']
        for sample in mutations:
            candidates = [a for a in aggregates if a['client'] == sample['client']
                          and a['kind'] == sample['kind'] and a.get('schema') == sample.get('schema')]
            if len(candidates) != 1 or any(candidates[0]['phase_sums'].get(name, 0) < value
                                         for name, value in sample['phases'].items()):
                raise RuntimeError('Aggregate phase accumulation differs from sample')
        save(self.output / f'telemetry-{phase}.json', ops)

    def workloads(self):
        super().workloads()
        self.writes('hash_durable_write1', 'hash', 1, 64, 'durable', 12000)
        self.writes('range_durable_write32', 'range', 32, 32, 'durable', 14000)
        # A status request is the consumer of the phase model. Time snapshots as
        # well as point/range reads and queued/durable writes.
        samples = [self.request('/api/status?forensics=1')[1] for _ in range(64)]
        self.phases['telemetry_forensics'] = stats(samples)
        self.samples.extend({'phase': 'telemetry_forensics', 'ms': value} for value in samples)
        self.stop()
        self.start()
        for start in range(0, COUNT, 32):
            numbers = list(range(start, start + 32))
            self.read('range', {'HashRangeKeys': [['measure', key(n)] for n in numbers]}, numbers, 32)
        for n in range(64):
            self.read('hash', {'HashKey': key(n)}, [n], 1)
        self.flush()


def execute(args, fixture=None, expected=None):
    bench = TelemetryBench(args)
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
        parser.add_argument('--' + name, type=Path, required=name != 'output',
                            default=default_output('phase-telemetry-benchmark-20261023') if name == 'output' else None)
    parser.add_argument('--rounds', type=int, default=4)
    parser.add_argument('--client-cpu', type=int, default=0)
    parser.add_argument('--server-cpu', type=int, default=1)
    opts = parser.parse_args()
    opts.output = resolve_output(opts.output)
    allowed = os.sched_getaffinity(0)
    if opts.client_cpu == opts.server_cpu or not {opts.client_cpu, opts.server_cpu} <= allowed:
        raise RuntimeError('Need two distinct allowed CPUs')
    os.sched_setaffinity(0, {opts.client_cpu})
    opts.output.mkdir(parents=True, exist_ok=False)
    opts.scratch.mkdir(mode=0o700, parents=True, exist_ok=False)
    provenance = json.loads(opts.provenance.read_text())
    for label in ('before', 'after'):
        if file_hash(opts.binaries / label) != provenance['binaries'][label]:
            raise RuntimeError('Binary hash differs from declared provenance')
    fields = provenance['phase_fields']
    revisions = provenance['revisions']
    lock_path = Path.home() / '.cache/fold-db-perf-guard/host-cargo.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fixtures, index = {}, []
    def arguments(name, config, label):
        return argparse.Namespace(binary=opts.binaries / label, scratch=opts.scratch / name,
            output=opts.output / name, source_revision=revisions[label], config=config,
            rustc=provenance['rustc'], client_cpu=opts.client_cpu, server_cpu=opts.server_cpu,
            phase_fields=fields)
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for config in ('plain', 'encrypted', 'preload'):
            fixtures[config] = execute(arguments(f'fixture-{config}', config, 'before'))
            print(f'Fixture ready: {config}', flush=True)
        for pair in range(opts.rounds):
            for config in fixtures:
                order = ('before', 'after') if pair % 2 == 0 else ('after', 'before')
                for label in order:
                    name = f'{config}-{pair}-{label}'
                    print(f'Starting {name}', flush=True)
                    fixture, summary = fixtures[config]
                    started = time.time()
                    bench, result = execute(arguments(name, config, label), fixture.home,
                                            summary['fixture_digest'])
                    cleanup(bench.root, bench.output)
                    index.append(dict(name=name, config=config, pair=pair, label=label,
                                      source_revision=revisions[label], seconds=time.time() - started))
                    save(opts.output / 'index.json', index)
        for fixture, summary in fixtures.values():
            assert fixture_digest(fixture.home) == summary['fixture_digest']
            cleanup(fixture.root, fixture.output)
    print('All telemetry measurements complete', flush=True)


if __name__ == '__main__':
    main()
