#!/usr/bin/env python3
"""Summarize the eight ordinary before/after product benchmark receipts."""
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[math.ceil(fraction * len(ordered)) - 1] if ordered else None


def main():
    cases = []
    for variant in ('before', 'after'):
        for shape in ('serial', 'concurrent4'):
            for number in (1, 2):
                directory = ROOT / f'{variant}-{shape}-r{number}'
                receipt = json.loads((directory / 'benchmark-result.json').read_text())
                inputs = json.loads((directory / 'inputs.json').read_text())
                cases.append({'receipt_directory': str(directory), 'variant': variant,
                              'shape': shape, 'round': number, 'complete': receipt['complete'],
                              'error': receipt['error'], 'binary_sha256': inputs['binary_sha256'],
                              'result': receipt['result']})
    groups = []
    for variant in ('before', 'after'):
        for shape in ('serial', 'concurrent4'):
            selected = [case for case in cases if case['variant'] == variant and case['shape'] == shape]
            results = [case['result'] for case in selected]
            if any(result is None for result in results):
                groups.append({'variant': variant, 'shape': shape, 'complete': False,
                               'limitation': 'A case did not produce a complete timed result.'})
                continue
            acks = [value for result in results for value in result['acks_ms']]
            wall = sum(result['wall_seconds_first_request_through_final_flush'] for result in results)
            records = sum(result['accepted_records'] for result in results)
            payload = sum(result['accepted_serialized_four_field_payload_bytes'] for result in results)
            memory = [sample for result in results for sample in result['memory_samples']]
            groups.append({
                'variant': variant, 'shape': shape, 'complete': all(case['complete'] for case in selected),
                'rounds': len(results), 'requested_records': sum(result['requested_records'] for result in results),
                'accepted_records': records, 'total_wall_seconds': wall,
                'wall_seconds_by_round': [result['wall_seconds_first_request_through_final_flush'] for result in results],
                'logical_records_per_second_total_work_over_total_wall': records / wall,
                'serialized_four_field_payload_MB_per_second': payload / 1e6 / wall,
                'body_payload_MB_per_second': records * 1024 / 1e6 / wall,
                'ack_samples': len(acks), 'ack_p50_ms': statistics.median(acks) if acks else None,
                'ack_p95_nearest_rank_ms': percentile(acks, .95),
                'mutation_error_count': sum(result['mutation_error_count'] for result in results),
                'mutation_refusal_count': sum(result['mutation_refusal_count'] for result in results),
                'maximum_observed_accepted_batches': max(result['max_observed_accepted_batches'] for result in results),
                'multiple_accepted_requests_observed': any(result['multiple_accepted_requests_observed'] for result in results),
                'optimization_path_observed': any(result['optimization_path_observed'] for result in results),
                'all_rounds_optimization_path_observed': all(result['optimization_path_observed'] for result in results),
                'covered_rounds': [case['round'] for case in selected if case['result']['optimization_path_observed']],
                'all_requested_keys_checked_after_normal_reopen': all(result['normal_reopen_all_512_requested_keys_checked'] for result in results),
                'all_accepted_values_and_membership_match_after_normal_reopen': all(result['normal_reopen_values_and_accepted_membership_match'] for result in results),
                'sparse_process_rss_min_bytes': min(sample['process_rss_bytes'] for sample in memory),
                'sparse_process_rss_max_observed_bytes': max(sample['process_rss_bytes'] for sample in memory),
                'memory_limit': 'Sparse process RSS observations do not bound the memory peak.'})
    comparisons = []
    for shape in ('serial', 'concurrent4'):
        before = next(group for group in groups if group['variant'] == 'before' and group['shape'] == shape)
        after = next(group for group in groups if group['variant'] == 'after' and group['shape'] == shape)
        comparable = before['complete'] and after['complete']
        covered = before.get('all_rounds_optimization_path_observed', False) and after.get('all_rounds_optimization_path_observed', False)
        row = {'shape': shape, 'complete_identical_logical_work': comparable,
               'optimization_path_observed_in_both_variants': covered,
               'optimization_comparison_eligible': comparable and covered}
        rounds = []
        for number in (1, 2):
            prior = next(case for case in cases if case['variant'] == 'before' and case['shape'] == shape and case['round'] == number)
            later = next(case for case in cases if case['variant'] == 'after' and case['shape'] == shape and case['round'] == number)
            eligible = (prior['complete'] and later['complete'] and prior['result'] is not None and later['result'] is not None
                        and prior['result']['optimization_path_observed'] and later['result']['optimization_path_observed'])
            pair = {'round': number, 'optimization_comparison_eligible': eligible,
                    'before_optimization_path_observed': prior['result'] is not None and prior['result']['optimization_path_observed'],
                    'after_optimization_path_observed': later['result'] is not None and later['result']['optimization_path_observed']}
            if eligible:
                pair['throughput_change_percent'] = 100 * (later['result']['logical_records_per_second'] / prior['result']['logical_records_per_second'] - 1)
            rounds.append(pair)
        row['rounds'] = rounds
        if comparable and covered:
            row.update(
                throughput_change_percent=100 * (after['logical_records_per_second_total_work_over_total_wall'] / before['logical_records_per_second_total_work_over_total_wall'] - 1),
                ack_p50_change_percent=100 * (after['ack_p50_ms'] / before['ack_p50_ms'] - 1),
                ack_p95_change_percent=100 * (after['ack_p95_nearest_rank_ms'] / before['ack_p95_nearest_rank_ms'] - 1))
        comparisons.append(row)
    summary = {'cases': cases, 'groups': groups, 'comparisons': comparisons,
               'coverage_limit': 'A functional case can complete without multiple accepted requests. Its timings remain valid, but it does not establish this optimization path.',
               'limits': ['Each shape has two cases and sixteen successful ack samples when no request fails.',
                          'The filesystem cache and other host activity are uncontrolled.',
                          'The ordinary native-status observer adds work in both programs.',
                          'No case retries a refused mutation or changes its configuration.',
                          'Sparse memory observations are not memory peak bounds.']}
    (ROOT / 'comparison.json').write_text(json.dumps(summary, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'groups': groups, 'comparisons': comparisons}, sort_keys=True))


if __name__ == '__main__':
    main()
