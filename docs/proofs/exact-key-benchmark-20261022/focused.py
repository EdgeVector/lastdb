#!/usr/bin/env python3
"""Longer encrypted write phases to investigate variation in the initial matrix."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from benchmark import Bench, COUNT, SCHEMAS, key, save


class Focused(Bench):
    def workloads(self):
        # Load both schemas completely before the sustained write phases.
        self.verify_all()
        for kind in SCHEMAS:
            self.writes(f'{kind}_queued_write1', kind, 1, 256, 'queued', 5000)
            self.writes(f'{kind}_durable_write32', kind, 32, 32, 'durable', 10000)
        self.verify_all()
        self.flush()
        self.stop()
        self.start()
        self.verify_all()
        self.flush()

    def verify_all(self):
        for n in range(COUNT):
            self.read('hash', {'HashKey': key(n)}, [n], 1)
        for n in range(0, COUNT, 32):
            numbers = list(range(n, n + 32))
            self.read('range', {'HashRangeKeys': [['measure', key(i)] for i in numbers]}, numbers, 32)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'scratch', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    args = parser.parse_args()
    args.config, args.rustc = 'encrypted', 'rustc 1.96.1'
    bench = Focused(args)
    summary = {'complete': False}
    inputs = json.loads((bench.output / 'inputs.json').read_text())
    inputs.update(focused_driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  purpose='Follow-up after variable encrypted write medians in the full matrix',
                  queued_single_requests=256, durable_batch32_requests=32)
    save(bench.output / 'inputs.json', inputs)
    try:
        bench.start()
        bench.install()
        bench.stop()
        bench.start()
        bench.workloads()
        summary.update(complete=True, phases=bench.phases, full_values_and_reopen_verified=True)
    except BaseException as error:
        summary['error'] = str(error)
        raise
    finally:
        bench.stop()
        save(bench.output / 'samples.json', bench.samples)
        summary.update(source_revision=args.source_revision, config=args.config,
                       all_owned_processes_stopped=bench.process is None,
                       free_bytes_after=shutil.disk_usage(bench.root).free)
        save(bench.output / 'summary.json', summary)
        print(json.dumps({'complete': summary['complete'], 'output': str(bench.output)}), flush=True)


if __name__ == '__main__':
    main()
