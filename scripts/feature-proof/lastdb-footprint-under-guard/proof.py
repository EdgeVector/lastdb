#!/usr/bin/env python3
"""Validate complete, bounded evidence for the LastDB memory proof."""
import argparse
from collections import Counter
import json
import math
from pathlib import Path
import re


# Safe-upgrade gate. A 24-hour run stays a soak, not this gate.
UPGRADE_GATE_DURATION_SECS = 600
# Footprint-visible slack. Not malloc held-free, which counts bytes outside phys_footprint.
RETENTION_LIMIT_BYTES = 512 * 1024 * 1024
# A step that frees warm bytes must drop phys_footprint by at least this share.
FOOTPRINT_RESPONSE_RATIO = 0.25

SAMPLE_GAUGES = (
    'observed_at', 'sampled_at', 'process_start_ts', 'phys_footprint_bytes',
    'implied_footprint_multiplier', 'malloc_bytes_in_use',
    'malloc_bytes_held_free', 'warm_budget_bytes', 'effective_warm_budget_bytes',
    'eviction_events', 'file_blob_rehydrates', 'footprint_net_bytes',
    'warm_bytes_freed',
)


def proof_kind_for(duration):
    """Select the report kind from the measured duration.

    `long-memory-candidate` is a soak. It is not the merge blocker and not
    the upgrade blocker. `upgrade-gate` is the safe-upgrade gate.
    """
    if duration >= 86400:
        return 'long-memory-candidate'
    if duration >= UPGRADE_GATE_DURATION_SECS:
        return 'upgrade-gate'
    return 'harness-smoke'


def number(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def evaluate(evidence):
    """Fail closed on missing evidence; a smoke cannot prove a full allocator run."""
    failures = set()
    manifest = evidence.get('manifest', {})
    required_manifest = (
        'duration_secs', 'elapsed_secs', 'started_at', 'finished_at',
        'sample_interval_secs', 'max_sample_age_secs', 'candidate_version',
        'candidate_sha256', 'source_git_oid', 'workload_sha256',
        'workload_provenance', 'snapshot_id', 'required_calls',
        'process_start_ts', 'isolation_verified', 'status_request_ids', 'expected_allocator',
    )
    if not isinstance(manifest, dict) or any(k not in manifest for k in required_manifest):
        return {'ok': False, 'failures': ['manifest_incomplete'],
                'full_allocator_proof': False, 'proof_kind': 'unproven'}
    for key in ('duration_secs', 'elapsed_secs', 'started_at', 'finished_at',
                'sample_interval_secs', 'max_sample_age_secs', 'process_start_ts'):
        if not number(manifest[key]):
            failures.add('manifest_invalid')
    for key, length in [('candidate_sha256', 64), ('source_git_oid', 40),
                        ('workload_sha256', 64)]:
        if not re.fullmatch('[0-9a-f]{' + str(length) + '}', str(manifest[key])):
            failures.add('manifest_invalid')
    for key in ('candidate_version', 'workload_provenance', 'snapshot_id'):
        if not isinstance(manifest[key], str) or not manifest[key].strip():
            failures.add('manifest_invalid')
    if failures:
        return {'ok': False, 'failures': sorted(failures),
                'full_allocator_proof': False, 'proof_kind': 'unproven'}
    duration = manifest['duration_secs']
    interval = manifest['sample_interval_secs']
    max_age = manifest['max_sample_age_secs']
    start, finish = manifest['started_at'], manifest['finished_at']
    if duration <= 0 or interval <= 0 or max_age < interval:
        failures.add('manifest_invalid')
    if manifest['elapsed_secs'] < duration or finish - start < duration:
        failures.add('duration_incomplete')
    if manifest['isolation_verified'] is not True:
        failures.add('isolation_unproven')
    if manifest['expected_allocator'] not in ('mimalloc', 'system'):
        failures.add('allocator_identity_unproven')

    samples = evidence.get('samples', [])
    valid = []
    if not isinstance(samples, list) or not samples:
        failures.add('samples_missing')
        samples = []
    for sample in samples:
        if not isinstance(sample, dict) or any(key not in sample for key in SAMPLE_GAUGES):
            failures.add('missing_sample_gauge')
            continue
        if not all(number(sample[key]) for key in SAMPLE_GAUGES):
            failures.add('invalid_sample_gauge')
            continue
        valid.append(sample)
        if sample.get('allocator_name') != manifest['expected_allocator']:
            failures.add('allocator_identity_unproven')
        age = sample['observed_at'] - sample['sampled_at']
        if age < 0 or age > max_age:
            failures.add('stale_sample')
        if sample['process_start_ts'] != manifest['process_start_ts']:
            failures.add('process_changed')
        if not start <= sample['observed_at'] <= finish:
            failures.add('sample_outside_run')
        # Backstop, not the operating target.
        if sample['implied_footprint_multiplier'] >= 1.3:
            failures.add('multiplier_limit')
        if sample['malloc_bytes_in_use'] <= 0:
            failures.add('allocator_accounting_missing')
        # measured minus footprint_net. Held-free is not this bar.
        slack = sample['phys_footprint_bytes'] - sample['footprint_net_bytes']
        if slack > RETENTION_LIMIT_BYTES:
            failures.add('allocator_retention_limit')
        if sample['file_blob_rehydrates'] != 0:
            failures.add('file_blob_hydrated')
    if valid:
        times = [row['sampled_at'] for row in valid]
        if any(b < a for a, b in zip(times, times[1:])):
            failures.add('sample_clock_regressed')
        if times[0] < start - max_age or times[0] > start + max_age or times[-1] < finish - max_age:
            failures.add('sample_boundary_gap')
        if any(b - a > max_age for a, b in zip(times, times[1:])):
            failures.add('sample_gap')
        if len(set(times)) < max(2, math.ceil(duration / max(max_age, 1))):
            failures.add('sample_coverage_incomplete')
        evictions = [row['eviction_events'] for row in valid]
        if any(b < a for a, b in zip(evictions, evictions[1:])):
            failures.add('eviction_counter_reset')
        # A flat lifetime eviction counter is not a failure. Each increase in
        # warm_bytes_freed is one step. Zero freed is not that test. A step
        # that does not move phys_footprint fails; it is not skipped.
        for prev, row in zip(valid, valid[1:]):
            freed = row['warm_bytes_freed'] - prev['warm_bytes_freed']
            if freed < 0:
                failures.add('footprint_response_unproven')
                continue
            if freed == 0:
                continue
            drop = prev['phys_footprint_bytes'] - row['phys_footprint_bytes']
            if drop < FOOTPRINT_RESPONSE_RATIO * freed:
                failures.add('footprint_response_unproven')
    footprints = sorted(row['phys_footprint_bytes'] for row in valid)
    p99 = footprints[math.ceil(len(footprints) * 0.99) - 1] if footprints else None
    # Backstop, not the operating target.
    if p99 is None or p99 >= 12 * 1024**3:
        failures.add('physical_footprint_limit')

    counts = Counter()
    requests = evidence.get('requests', [])
    if not isinstance(requests, list):
        requests = []
    for request in requests:
        if not isinstance(request, dict):
            failures.add('request_evidence_missing')
            continue
        if (request.get('ok') is not True or not number(request.get('status'))
                or not 200 <= request['status'] < 300):
            failures.add('request_failed')
        if any(not number(request.get(key)) for key in
               ('observed_at', 'elapsed_ms', 'rows', 'min_rows')):
            failures.add('request_evidence_missing')
            continue
        if request['rows'] < request['min_rows']:
            failures.add('required_rows_missing')
        if not start <= request['observed_at'] <= finish:
            failures.add('request_outside_run')
        name = request.get('name')
        if not isinstance(name, str) or not name:
            failures.add('request_evidence_missing')
        else:
            counts[name] += 1
    required = manifest['required_calls']
    if not isinstance(required, dict) or not required:
        failures.add('workload_incomplete')
    else:
        for name, count in required.items():
            if not isinstance(count, int) or isinstance(count, bool) or count <= 0 or counts[name] < count:
                failures.add('workload_incomplete')

    observations = evidence.get('status_observations', [])
    issued = manifest['status_request_ids']
    if (not isinstance(issued, list) or len(issued) != 2
            or any(not isinstance(value, str) or not value for value in issued)
            or len(set(issued)) != 2):
        failures.add('status_zero_load_unproven')
        issued = []
    seen = set()
    if not isinstance(observations, list):
        observations = []
    for row in observations:
        if (not isinstance(row, dict) or not isinstance(row.get('request_id'), str)
                or row['request_id'] not in issued or row.get('kind') != 'status'
                or row.get('status') != 200 or row.get('cold_shard_loads') != 0
                or isinstance(row.get('cold_shard_loads'), bool)):
            failures.add('status_zero_load_unproven')
            continue
        seen.add(row['request_id'])
    if len(seen) < 2:
        failures.add('status_zero_load_unproven')
    return {
        'ok': not failures, 'failures': sorted(failures),
        'proof_kind': proof_kind_for(duration),
        # Baseline latency and the guard-recovery case are separate required bars.
        'full_allocator_proof': False,
        'duration_secs': duration, 'elapsed_secs': manifest['elapsed_secs'],
        'samples': len(samples), 'valid_samples': len(valid),
        'p99_phys_footprint_bytes': p99, 'request_counts': dict(counts),
        'status_zero_load_observations': len(seen),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evaluate', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    report = evaluate(json.loads(args.evaluate.read_text()))
    args.report.write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    print(json.dumps(report, allow_nan=False))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
