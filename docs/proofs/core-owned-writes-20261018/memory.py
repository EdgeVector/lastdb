"""Observe ordinary concurrent reads and repeated batched writes under a byte limit."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time

import logical

MODEL_FIELDS = ('dirty_slots', 'dirty_molecules', 'dirty_definitions',
    'metadata_limit_bytes', 'metadata_current_bytes', 'metadata_used_bytes',
    'metadata_retained_bytes', 'metadata_peak_bytes', 'refused',
    'body_reserved_current_bytes', 'body_reserved_used_bytes',
    'body_reserved_retained_bytes', 'body_reserved_peak_bytes')


def observe(node, phase, model_field, candidate, active_readers=0):
    native = node.status(phase)
    model = native.get(model_field)
    if candidate and (not isinstance(model, dict) or any(name not in model for name in MODEL_FIELDS)):
        raise RuntimeError('The candidate lacks required core model memory observations')
    if isinstance(model, dict):
        used, current = model['metadata_used_bytes'], model['metadata_current_bytes']
        if used > model['metadata_limit_bytes'] or current > used:
            raise RuntimeError('The core metadata charge exceeds its allowance or total live charge')
        body_used = model['body_reserved_used_bytes']
        body_current = model['body_reserved_current_bytes']
        if (body_current > body_used or model['body_reserved_retained_bytes'] != body_used - body_current
                or body_used > model['body_reserved_peak_bytes']):
            raise RuntimeError('The direct body reservation counters disagree')
    result, _, _, identity = node.call('/api/status')
    status = result.get('data', result)['status']
    budget = status.get('memory_budget', {})
    used, limit = budget.get('deferred_persist_bytes'), budget.get('deferred_persist_cap_bytes')
    if not isinstance(used, int) or not isinstance(limit, int):
        raise RuntimeError('The product status lacks its existing deferred-memory byte counters')
    if used > limit:
        raise RuntimeError('The observed deferred-memory charge exceeds its reported cap')
    if isinstance(model, dict) and model['body_reserved_peak_bytes'] > limit:
        raise RuntimeError('The direct body reservation peak exceeds the configured allowance')
    row = {'phase': phase, 'active_client_read_calls': active_readers,
        'model_field': model_field, 'core_model': model,
        'deferred_persist_bytes': used, 'deferred_persist_cap_bytes': limit,
        'sampled_process_memory_bytes': status.get('rss_bytes'),
        'sampler_timestamp': status.get('sampled_at'), 'status_request_id': identity}
    node.remember(operation='core-owned-memory-observation', **row,
        scope='Native model counters are direct. /api/status uses the published sampler. Active client calls do not identify an exact internal snapshot instruction.')
    return row


def run(node, candidate, model_field):
    spec = logical.Spec('hashrange', 'CoreOwnedMemoryPressure')
    logical.install(node, spec)
    logical.read(node, spec, {999999: None}, 'pressure-empty-definitions-and-keys')
    numbers, body_bytes = list(range(16)), 32768
    logical.write(node, [logical.mutation(spec, number, 40, 'create', 'durable',
        body_bytes=body_bytes) for number in numbers], 'pressure-generated-create')
    logical.read(node, spec, dict.fromkeys(numbers, 40), 'pressure-first-value-read',
                 zero=True, body_bytes=body_bytes)
    logical.drained(node, 'pressure-seed-flushed')
    initial = observe(node, 'pressure-initial', model_field, candidate)
    barrier, finished = threading.Barrier(6), threading.Event()
    read_lock, active, observations, modes = threading.Lock(), {'calls': 0}, [], []

    def reader(reader_number):
        barrier.wait(timeout=30)
        for repeat in range(24):
            with read_lock:
                active['calls'] += 1
            try:
                logical.read(node, spec, dict.fromkeys(numbers, 46),
                    f'pressure-reader-{reader_number}-{repeat}', bounded=True,
                    body_bytes=body_bytes, allowed_revisions=list(range(40, 47)))
            finally:
                with read_lock:
                    active['calls'] -= 1
            if finished.is_set() and repeat >= 1:
                break

    def writer():
        barrier.wait(timeout=30)
        try:
            for revision in range(41, 47):
                result = logical.write(node, [logical.mutation(spec, number, revision,
                    full=True, body_bytes=body_bytes) for number in numbers],
                    'pressure-repeated-complete-overwrite')
                modes.append(result['durability'])
                with read_lock:
                    readers = active['calls']
                observations.append(observe(node, 'pressure-after-ack-' + str(revision),
                                            model_field, candidate, readers))
            node.flush()
        finally:
            finished.set()

    def observer():
        barrier.wait(timeout=30)
        for sample in range(48):
            with read_lock:
                readers = active['calls']
            observations.append(observe(node, 'pressure-overlap-' + str(sample),
                                        model_field, candidate, readers))
            if finished.is_set() and readers == 0:
                break
            time.sleep(.05)

    with ThreadPoolExecutor(max_workers=6) as pool:
        jobs = [pool.submit(writer), pool.submit(observer)]
        jobs += [pool.submit(reader, number) for number in range(4)]
        for job in jobs:
            job.result(timeout=180)
    logical.read(node, spec, dict.fromkeys(numbers, 46), 'pressure-final-values',
                 bounded=True, body_bytes=body_bytes)
    logical.drained(node, 'pressure-final-flush')
    final = observe(node, 'pressure-after-all-calls', model_field, candidate)
    if candidate:
        model = final['core_model']
        if any(model[name] != 0 for name in ('dirty_slots', 'dirty_molecules', 'dirty_definitions')):
            raise RuntimeError('The completed pressure cohort retains dirty core state')
        if ('queued' not in modes or not any(row['core_model']['body_reserved_used_bytes'] > 0
                                           for row in observations)):
            raise RuntimeError('The revised pressure cohort does not observe a charged queued write')
        if model['body_reserved_retained_bytes'] != 0:
            raise RuntimeError('The completed calls retain old or privately prepared body owners')
    logical.write(node, [logical.mutation(spec, 0, 47, full=True, body_bytes=body_bytes)],
                  'pressure-recovery-final-write')
    logical.read(node, spec, {0: 47}, 'pressure-recovery-first-read', zero=True,
                 body_bytes=body_bytes)
    logical.drained(node, 'pressure-recovery-flushed')
    positive = [row for row in observations if row['active_client_read_calls'] > 0
                and isinstance(row['core_model'], dict)
                and row['core_model'].get('metadata_retained_bytes', 0) > 0]
    body_positive = [row for row in observations if row['active_client_read_calls'] > 0
                    and isinstance(row['core_model'], dict)
                    and row['core_model'].get('body_reserved_retained_bytes', 0) > 0]
    node.remember(operation='core-owned-memory-cohort-complete', initial=initial, final=final,
        observations=len(observations), positive_retained_metadata_overlap_samples=len(positive),
        positive_retained_body_overlap_samples=len(body_positive), write_modes=modes,
        records=16, body_bytes=body_bytes,
        scope='The smaller batch targets a charged queued acknowledgement within 16MiB. Current owners can survive flush in clean core slots. Retained owners can include private preparation. Client overlap cannot identify an exact internal capture instruction.')
