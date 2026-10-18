#!/usr/bin/env python3
"""Measure ordinary concurrent keyed reads, complete writes, and normal flushes."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
import concurrent_cohort as cohort
import range_profile as profile


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('before', 'after', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--read-rounds', type=int, default=40)
    parser.add_argument('--write-rounds', type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.read_rounds <= 80 or not 2 <= args.write_rounds <= 40:
        parser.error('Use 1..80 read rounds and 2..40 write rounds')
    args.records, args.body_bytes, args.start = 320, 1024, 32
    args.counts, args.shapes = [1, 16, 64], ['exact', 'range']
    return args


def preflush(node):
    node.update_cohort(1, 'queued')
    before = node.status('queued-ack-before-read')
    revision = node.read_batch('queued-ack-before-flush', 0)
    after = node.status('queued-ack-after-read')
    covered = (before['accepted_sequence'] > before['completed_sequence']
               and after['accepted_sequence'] > after['completed_sequence'])
    node.remember(operation='accepted-visibility', complete_revision=revision,
                  pending_before_read=before['accepted_sequence'] > before['completed_sequence'],
                  pending_after_read=after['accepted_sequence'] > after['completed_sequence'],
                  accepted_only_visibility_observed=covered)
    node.flush_phase('pre-concurrent')


def writer(node):
    try:
        for offset in range(node.run.args.write_rounds):
            if node.state.failed:
                return
            durability = 'queued' if offset < node.run.args.write_rounds // 2 else 'durable'
            node.update_cohort(offset + 2, durability)
    finally:
        node.state.update('done', True)


def reader(node):
    for repeat in range(node.run.args.read_rounds):
        target = 1 + repeat * node.run.args.write_rounds // node.run.args.read_rounds
        with node.state.changed:
            ready = node.state.changed.wait_for(lambda: node.state.acknowledged >= target
                                               or node.state.done or node.state.failed, timeout=180)
            if not ready:
                raise RuntimeError('The writer does not reach the next ordinary read revision')
            if node.state.failed:
                return
            if node.state.done and node.state.acknowledged < target:
                raise RuntimeError('The writer ends before the next ordinary read revision')
        node.read_batch('concurrent', repeat)


def flusher(node):
    for target in range(2, node.run.args.write_rounds + 2, 2):
        with node.state.changed:
            ready = node.state.changed.wait_for(lambda: node.state.acknowledged >= target
                                               or node.state.done or node.state.failed, timeout=180)
            if not ready:
                raise RuntimeError('The bounded cohort writer does not reach the next flush')
            if node.state.failed or node.state.done and node.state.acknowledged < target:
                return
        node.flush_phase('concurrent')


def concurrent_work(node):
    started = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=3) as workers:
        futures = [workers.submit(worker, node) for worker in (reader, writer, flusher)]
        errors = []
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as error:
                errors.append(f'{type(error).__name__}: {error}')
                node.state.update('failed', True)
        if errors:
            raise RuntimeError('; '.join(errors))
    node.remember(operation='concurrent-complete', started_nanos=started,
                  finished_nanos=time.perf_counter_ns(), acknowledged=node.state.acknowledged)
    coverage(node)
    node.status('concurrent-end')
    node.read_batch('after-last-ack', 0)


def coverage(node):
    rows = [row for row in node.run.rows if row['variant'] == node.variant]
    writes = [row for row in rows if row['operation'] == 'mutation' and row['revision'] >= 2]
    reads = [row for row in rows if row['operation'] == 'query-batch' and row['phase'] == 'concurrent']
    flushes = [row for row in rows if row['operation'] == 'flush' and row['phase'] == 'concurrent']
    def overlaps(row):
        return any(row['started_nanos'] < write['finished_nanos']
                   and write['started_nanos'] < row['finished_nanos'] for write in writes)
    read_overlaps, flush_overlaps = sum(map(overlaps, reads)), sum(map(overlaps, flushes))
    node.remember(operation='concurrent-coverage', query_batches=len(reads),
                  update_cohorts=len(writes), normal_flushes=len(flushes),
                  query_batches_during_update_requests=read_overlaps,
                  flushes_during_update_requests=flush_overlaps,
                  observed_revisions=sorted({row['revision'] for row in reads}),
                  overlap_scope='Client request intervals; storage locks may serialize work')
    if not read_overlaps or not flush_overlaps:
        raise RuntimeError('The ordinary workload does not expose read/write/flush request overlap')


def known_keys(node, phase):
    hashes = {}
    for first in (0, 256):
        numbers = list(range(first, min(first + 256, node.run.args.records)))
        for shape in ('exact', 'range'):
            selector = ({'HashRangeKeys': [['generated', profile.identifier(n)] for n in numbers]}
                        if shape == 'exact' else {'HashRangeRange': {
                            'hash': 'generated', 'start': profile.identifier(numbers[0]),
                            'end': profile.identifier(numbers[-1] + 1)}})
            result, elapsed, cost, request_id = node.call('/api/query', {
                'schema_name': profile.SCHEMA, 'fields': profile.FIELDS,
                'filter': selector, 'limit': len(numbers)}, True)
            profile.validate_query(result, numbers, node.expected, shape)
            if not isinstance(cost, dict):
                raise RuntimeError('The known-key read lacks its requested file receipt')
            hashes[(shape, first)] = profile.digest(result)
            node.remember(operation='query', phase=phase, shape=shape, batch=len(numbers),
                          repeat=first, request_id=request_id, elapsed_nanos=elapsed, cost=cost,
                          response_sha256=hashes[(shape, first)], expected_values_match=True,
                          returned_records=len(numbers))
    return hashes


def finish(node):
    revision = node.run.args.write_rounds + 1
    node.expected.update({n: cohort.generated_values(n, revision) for n in cohort.NUMBERS})
    node.flush_phase('final')
    before = known_keys(node, 'before-normal-stop')
    node.stop()
    node.start()
    if known_keys(node, 'normal-reopen') != before:
        raise RuntimeError('The complete known-key answers or metadata change after normal reopen')


def main():
    run, nodes, errors, success = profile.Run(arguments()), [], [], False
    try:
        nodes = [cohort.Node(run, variant, getattr(run.args, variant)) for variant in ('before', 'after')]
        run.inputs(nodes)
        profile.save(run.root / 'ownership.json', {'run_id': run.identity, 'uid': os.getuid(),
                     'root': str(run.root), 'homes': [str(node.home) for node in nodes],
                     'origin': 'generated-empty'})
        profile.save(run.root / 'concurrent-inputs.json', {
            'driver_sha256': profile.file_hash(Path(__file__)),
            'cohort_driver_sha256': profile.file_hash(Path(cohort.__file__)),
            'write_helper_sha256': profile.file_hash(Path(__file__).with_name('write_reuse.py')),
            'ownership_helper_sha256': profile.file_hash(Path(__file__).with_name('ownership.py')),
            'update_keys': [32, 95], 'write_rounds': run.args.write_rounds,
            'read_batches': run.args.read_rounds, 'query_items_per_batch': 6,
            'rule': 'One full known revision per shared query scope; at least the ack floor at call start',
            'flush_rule': 'Normal accepted-prefix flush after every second acknowledged cohort',
            'ceiling_rule': 'Submitted revision at query completion; later submissions may precede server capture',
            'snapshot_order': 'Validate revision boundaries, never metadata by completion order'})
        for node in nodes:
            node.seed()
        for node in nodes:
            node.start()
            preflush(node)
            concurrent_work(node)
            finish(node)
        success = True
    except Exception as error:
        errors.append(f'{type(error).__name__}: {error}')
    finally:
        for node in nodes:
            try:
                node.stop()
            except Exception as error:
                errors.append(f'{type(error).__name__}: {error}')
        stops = [row for row in run.rows if row['operation'] == 'owned-node-stop']
        refusals = [row for row in run.rows if row['operation'] == 'refusal']
        if success and (len(stops) != 6 or any(row['forced_kill'] or row['exit_code'] != 0 for row in stops)):
            errors.append('The run lacks six normal zero-exit owned-node stops')
        if refusals:
            errors.append('An ordinary product request was refused')
        run.summarize()
        profile.save(run.root / 'completion.json', {
            'success': success and not errors, 'errors': errors, 'finished_utc': profile.now(),
            'query_count': sum(row['operation'] == 'query' for row in run.rows),
            'normal_stops': len(stops), 'refusal_count': len(refusals),
            'receipt_sha256': profile.receipt_hashes(run, nodes)})
    if errors:
        raise RuntimeError('; '.join(errors))
    print(str(run.root / 'summary.json'), flush=True)


if __name__ == '__main__':
    main()
