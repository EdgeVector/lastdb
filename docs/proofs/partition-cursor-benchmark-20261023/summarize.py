#!/usr/bin/env python3
"""Summarize per-run medians and paired latency changes without pooling hosts."""
import argparse
import json
import math
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    index = json.loads((args.root / 'index.json').read_text())
    runs = {}
    for item in index:
        result = json.loads((args.root / item['name'] / 'summary.json').read_text())
        if not result['complete'] or not result['all_owned_processes_stopped']:
            raise RuntimeError('Incomplete benchmark run')
        runs[item['config'], item['pair'], item['label']] = result
    output = {}
    for config in sorted({config for config, _, _ in runs}):
        pairs = sorted({pair for mode, pair, _ in runs if mode == config})
        phases = runs[config, pairs[0], 'before']['phases']
        output[config] = {}
        for phase in phases:
            before = [runs[config, pair, 'before']['phases'][phase] for pair in pairs]
            after = [runs[config, pair, 'after']['phases'][phase] for pair in pairs]
            changes = [100 * (a['median_ms'] / b['median_ms'] - 1) for b, a in zip(before, after)]
            p95_changes = [100 * (a['p95_ms'] / b['p95_ms'] - 1) for b, a in zip(before, after)]
            row = {'before_median_ms': statistics.median(b['median_ms'] for b in before),
                   'after_median_ms': statistics.median(a['median_ms'] for a in after),
                   'paired_median_change_percent': statistics.median(changes),
                   'paired_changes_percent': changes,
                   'before_run_medians_ms': [b['median_ms'] for b in before],
                   'after_run_medians_ms': [a['median_ms'] for a in after],
                   'before_p95_ms': statistics.median(b['p95_ms'] for b in before),
                   'after_p95_ms': statistics.median(a['p95_ms'] for a in after),
                   'paired_p95_changes_percent': p95_changes,
                   'requests_per_run': before[0]['requests']}
            row['change_of_reported_medians_percent'] = 100 * (row['after_median_ms'] / row['before_median_ms'] - 1)
            row['paired_geomean_change_percent'] = 100 * (math.exp(statistics.mean(math.log(a['median_ms'] / b['median_ms']) for b, a in zip(before, after))) - 1)
            if 'records_per_second_through_flush' in before[0]:
                row.update(before_records_per_second=statistics.median(
                    b['records_per_second_through_flush'] for b in before),
                    after_records_per_second=statistics.median(
                    a['records_per_second_through_flush'] for a in after),
                    before_acknowledgements=[b['acknowledgements'] for b in before],
                    after_acknowledgements=[a['acknowledgements'] for a in after])
            output[config][phase] = row
    (args.root / 'comparison.json').write_text(json.dumps(output, indent=2) + '\n')
    for config, phases in output.items():
        print(config)
        for phase, row in phases.items():
            print(f"{phase:32s} {row['before_median_ms']:9.3f} {row['after_median_ms']:9.3f} "
                  f"{row['change_of_reported_medians_percent']:+7.1f}% {row['paired_changes_percent']}")


if __name__ == '__main__':
    main()
