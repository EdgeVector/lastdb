#!/usr/bin/env python3
"""Longer fully-warm read and queued-write follow-up for plain/preloaded keys."""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'exact-prefix-benchmark-20261023'))
from benchmark import Bench, COUNT, SCHEMAS, key, save


class FocusedBench(Bench):
    def workloads(self):
        points = {}
        for kind in SCHEMAS:
            points[kind] = [({'HashKey': key(n)} if kind == 'hash' else
                {'HashRangeKey': {'hash': 'measure', 'range': key(n)}}, [n], 1)
                for n in range(COUNT)]
            for selector, numbers, limit in points[kind]:
                self.read(kind, selector, numbers, limit)
        for kind in SCHEMAS:
            self.reads(f'{kind}_fully_warm_point', kind, points[kind] * 4)
        self.reads('range_prefix10', 'range', [({'HashRangePrefix': {
            'hash': 'measure', 'prefix': 'key-00012'}}, list(range(120, 130)), 16)] * 512)
        self.reads('range_between64', 'range', [({'HashRangeRange': {
            'hash': 'measure', 'start': key(128), 'end': key(192)}}, list(range(128, 192)), 64)] * 128)
        self.writes('range_fully_warm_queued_write32', 'range', 32, 64, 'queued', 9000)
        self.stop()
        self.start()
        for start in range(0, COUNT, 32):
            numbers = list(range(start, start + 32))
            self.read('range', {'HashRangeKeys': [['measure', key(n)] for n in numbers]}, numbers, 32)
        self.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'scratch', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--config', choices=['plain', 'preload'], required=True)
    parser.add_argument('--rustc', default='rustc 1.96.1')
    args = parser.parse_args()
    bench = FocusedBench(args)
    summary = {'complete': False}
    try:
        bench.start()
        bench.install()
        bench.stop()
        bench.start()
        bench.workloads()
        summary.update(complete=True, phases=bench.phases, values_and_delete_visibility_verified=False,
                       values_and_write_reopen_verified=True)
    except BaseException as error:
        summary['error'] = str(error)
        raise
    finally:
        bench.stop()
        save(bench.output / 'samples.json', bench.samples)
        summary.update(source_revision=args.source_revision, config=args.config,
                       all_owned_processes_stopped=bench.process is None)
        save(bench.output / 'summary.json', summary)
        print(json.dumps({'complete': summary['complete'], 'output': str(bench.output)}), flush=True)


if __name__ == '__main__':
    main()
