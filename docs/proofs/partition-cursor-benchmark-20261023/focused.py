#!/usr/bin/env python3
"""Longer fully-warmed encrypted cursor follow-up, separate from the matrix."""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from paging import PagingBench
from benchmark import COUNT, key, save


class FocusedBench(PagingBench):
    def workloads(self):
        self.write('range', [0, 1, 2], 0, 'durable', 'delete')
        self.write('range', [3, 4], 0, 'durable', 'purge')
        self.flush()
        self.stop()
        self.start()
        self.read('range', {'HashKey': 'measure'}, list(range(COUNT)), COUNT)
        for _ in range(64):
            self.page(offset=144)
            self.page(cursor=key(127))
            self.page(cursor='')
            self.page(cursor=key(499))
        self.page_phase('partition_offset144_limit16', [{'offset': 144}] * 512)
        self.page_phase('partition_cursor127_limit16', [{'cursor': key(127)}] * 1024)
        self.page_phase('partition_cursor_after_empty', [{'cursor': ''}] * 1024)
        self.page_phase('partition_short_last_page', [{'cursor': key(499)}] * 1024)
        self.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'scratch', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--config', choices=['encrypted'], default='encrypted')
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
        summary.update(complete=True, phases=bench.phases, values_and_delete_visibility_verified=True)
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
