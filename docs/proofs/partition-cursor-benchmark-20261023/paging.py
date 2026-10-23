#!/usr/bin/env python3
"""Read/write matrix plus bounded partition offsets and exclusive cursor pages."""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'exact-prefix-benchmark-20261023'))
from benchmark import Bench, COUNT, SCHEMAS, file_hash, key, query, save, stats, values


class PagingBench(Bench):
    def __init__(self, args):
        super().__init__(args)
        inputs = json.loads((self.output / 'inputs.json').read_text())
        inputs['paging_driver_sha256'] = file_hash(Path(__file__))
        inputs['paging_integrity_checks'] = 'offsets, exclusive cursors, full walk, empty/NUL/Unicode ranges'
        save(self.output / 'inputs.json', inputs)

    def page(self, offset=0, cursor=None, limit=16, include_tombstones=False):
        request = query('range', {'HashKey': 'measure'}, limit)
        request['include_tombstones'] = include_tombstones
        if cursor is not None:
            request['cursor'] = {'hash': 'measure', 'range': cursor}
        else:
            request['offset'] = offset
        result, elapsed = self.request('/api/query', request)
        eligible = [n for n in range(COUNT)
                    if (include_tombstones or self.revisions['range'][n] is not None)
                    and (cursor is None or key(n) > cursor)]
        expected = eligible[offset:offset + limit]
        rows = result.get('results', [])
        actual = [row['key']['range'] for row in rows]
        if (actual != [key(n) for n in expected]
                or result.get('returned_count') != len(expected)
                or result.get('has_more') != (len(eligible) > offset + limit)):
            raise RuntimeError(f'Partition page differs: {request}; keys {actual}')
        for n, row in zip(expected, rows):
            revision = self.revisions['range'][n]
            if revision is not None and row['fields'] != values(n, revision):
                raise RuntimeError('Partition page live value differs')
        return elapsed

    def page_phase(self, name, requests):
        samples = [self.page(**request) for request in requests]
        self.phases[name] = stats(samples)
        self.samples.extend({'phase': name, 'ms': value} for value in samples)
        print(f'{name}: {self.phases[name]["median_ms"]:.3f} ms median', flush=True)

    def edge_partition(self):
        ranges = ['', '\0', 'a', 'a\0', 'aa', 'é', '中', '😀']
        expected = {}
        mutations = []
        for i, range_key in enumerate(ranges):
            fields = {**values(i, 0), 'bucket': 'edges', 'id': range_key}
            expected[range_key] = fields
            mutations.append(dict(type='mutation', schema=SCHEMAS['range'],
                mutation_type='create', durability='durable',
                key_value={'hash': 'edges', 'range': range_key}, fields_and_values=fields))
        self.call('/api/mutations/batch', {'mutations': mutations})
        self.flush()
        cursor = None
        ordered = sorted(ranges)
        for start in range(0, len(ordered), 3):
            request = query('range', {'HashKey': 'edges'}, 3)
            if cursor is not None:
                request['cursor'] = {'hash': 'edges', 'range': cursor}
            result, _ = self.request('/api/query', request)
            rows = result.get('results', [])
            wanted = ordered[start:start + 3]
            if ([row['key']['range'] for row in rows] != wanted
                    or result.get('has_more') != (start + 3 < len(ordered))):
                raise RuntimeError('Empty/NUL/Unicode partition order or cursor differs')
            for row in rows:
                if row['fields'] != expected[row['key']['range']]:
                    raise RuntimeError('Edge partition value differs')
            cursor = wanted[-1]

    def workloads(self):
        super().workloads()
        self.edge_partition()
        self.page_phase('partition_offset144_limit16', [{'offset': 144}] * 96)
        self.page_phase('partition_cursor127_limit16', [{'cursor': key(127)}] * 96)
        self.page_phase('partition_cursor_after_empty', [{'cursor': ''}] * 64)
        self.page_phase('partition_short_last_page', [{'cursor': key(499)}] * 64)
        # Walk the whole partition through the public exclusive-cursor route.
        cursor = None
        visible = [n for n in range(COUNT) if self.revisions['range'][n] is not None]
        for start in range(0, len(visible), 17):
            self.page(cursor=cursor, limit=17)
            cursor = key(visible[min(start + 17, len(visible)) - 1])
        self.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'scratch', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--config', choices=['plain', 'encrypted', 'preload'], required=True)
    parser.add_argument('--rustc', default='rustc 1.96.1')
    args = parser.parse_args()
    bench = PagingBench(args)
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
