"""Use batched ordinary logical writes and observe their first keyed reads."""
from concurrent.futures import ThreadPoolExecutor
import threading

import logical

SPECS = [logical.Spec('hash', 'CoreOwnedWritesHash'),
         logical.Spec('hashrange', 'CoreOwnedWritesHashRange')]


def parallel(actions):
    with ThreadPoolExecutor(max_workers=len(actions)) as pool:
        jobs = [pool.submit(action) for action in actions]
        return [job.result(timeout=180) for job in jobs]


def reads(node, expected, phase, zero=True):
    return parallel([lambda spec=spec: logical.read(node, spec, expected, phase,
        zero=zero) for spec in SPECS])


def first_ack(node, operations, expected, phase):
    logical.write(node, operations, phase)
    node.status(phase + '-after-ack-before-first-value-read')
    reads(node, expected, phase + '-first-value-read')
    logical.read(node, SPECS[1], expected, phase + '-bounded', zero=True, bounded=True)


def initial(node):
    parallel([lambda spec=spec: logical.install(node, spec) for spec in SPECS])
    reads(node, {999999: None}, 'empty-schema-definitions-and-keys', zero=False)
    node.remember(operation='core-owned-prime-scope',
        scope='Only empty-schema definitions and complete selected keys are primed. No target value is read before its create acknowledgement.')
    numbers = list(range(64))
    operations = [logical.mutation(spec, number, 1, 'create')
                  for spec in SPECS for number in numbers]
    first_ack(node, operations, dict.fromkeys(numbers, 1), 'new-complete-queued-batch')
    logical.drained(node, 'new-complete-queued-batch-flushed')
    operations = [logical.mutation(spec, number, 2) for spec in SPECS for number in numbers]
    first_ack(node, operations, dict.fromkeys(numbers, 2), 'warm-partial-update')
    logical.drained(node, 'warm-partial-update-flushed')


def mixed(node):
    jobs, expected = [], dict.fromkeys(range(64), 2)
    expected.update({0: 4, 1: 4, 2: None, 96: 4})
    for spec in SPECS:
        jobs += [logical.mutation(spec, 0, 4),
                 logical.mutation(spec, 1, 4),
                 logical.mutation(spec, 2, 0, 'delete'),
                 logical.mutation(spec, 96, 4, 'create')]
    first_ack(node, jobs, expected, 'mixed-independent-delete-update-create')
    jobs = [logical.mutation(spec, 0, 0, 'delete') for spec in SPECS]
    first_ack(node, jobs, {0: None}, 'separately-acknowledged-delete')
    jobs = [logical.mutation(spec, 0, 5, 'create') for spec in SPECS]
    first_ack(node, jobs, {0: 5}, 'reinsert-after-delete-ack')
    expected[0] = 5
    logical.drained(node, 'mixed-batch-flushed')
    node.stop()
    node.start()
    reads(node, expected, 'mixed-batch-normal-reopen', zero=False)


def no_prior_values(node):
    numbers = list(range(2000, 2016))
    operations = [logical.mutation(spec, number, 10, 'create', 'durable')
                  for spec in SPECS for number in numbers]
    first_ack(node, operations, dict.fromkeys(numbers, 10), 'new-complete-durable-batch')
    logical.drained(node, 'durable-batch-flushed')
    node.stop()
    node.start()
    reads(node, {999999: None}, 'reopen-definitions-and-keys-only', zero=False)
    operations = [logical.mutation(spec, number, 11, full=True)
                  for spec in SPECS for number in numbers]
    first_ack(node, operations, dict.fromkeys(numbers, 11), 'no-prior-value-complete-replacement')
    node.remember(operation='core-owned-replacement-scope',
        scope='The after-ack zero-file gate covers replacement of all four logical fields. Partial updates with unknown untouched fields can still fetch those fields.')
    jobs = [logical.mutation(spec, number, 0, 'delete')
            for spec in SPECS for number in numbers]
    first_ack(node, jobs, dict.fromkeys(numbers, None), 'complete-delete-before-explicit-flush')
    logical.drained(node, 'complete-delete-flushed')
    node.stop()
    node.start()
    reads(node, dict.fromkeys(numbers, None), 'delete-normal-reopen', zero=False)


def concurrent(node):
    spec, numbers = SPECS[1], list(range(3000, 3064))
    logical.write(node, [logical.mutation(spec, number, 20, 'create') for number in numbers],
                  'concurrent-initial-create')
    logical.read(node, spec, dict.fromkeys(numbers, 20), 'concurrent-initial-first-read', zero=True)
    for revision in (21, 22, 23):
        barrier = threading.Barrier(4)
        def write():
            barrier.wait(timeout=30)
            logical.write(node, [logical.mutation(spec, number, revision, full=True)
                                 for number in numbers], 'concurrent-complete-update')
        def read(reader):
            barrier.wait(timeout=30)
            for repeat in range(4):
                logical.read(node, spec, dict.fromkeys(numbers, revision),
                    f'concurrent-{revision}-{reader}-{repeat}', bounded=reader != 0,
                    allowed_revisions=[revision - 1, revision])
        parallel([write, *[lambda reader=reader: read(reader) for reader in range(3)]])
        logical.read(node, spec, dict.fromkeys(numbers, revision),
                     'new-read-after-concurrent-ack', zero=True, bounded=True)
        node.remember(operation='core-owned-concurrent-cohort', revision=revision,
            scope='Ordinary concurrent calls check permitted complete batch states. No call pauses at a chosen captured-view instruction.')
    logical.drained(node, 'concurrent-flushed')


def run(node):
    initial(node)
    mixed(node)
    no_prior_values(node)
    concurrent(node)
