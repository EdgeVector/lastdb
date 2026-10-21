"""Supported ordinary write routes for the combined data and notice implementation."""
from concurrent.futures import ThreadPoolExecutor
import threading

from operation_context import item


def modes(context, spec):
    for index, mode in enumerate((None, 'queued', 'durable')):
        name = 'omitted' if mode is None else mode
        context.write([item(spec, index, 1, mode=mode)], f'{spec.kind}-single-{name}', single=True)
        context.write([item(spec, 100 + index * 10 + offset, 1, mode=mode) for offset in range(2)],
                      f'{spec.kind}-batch-{name}')


def mixed_and_erasure(context, spec):
    context.write([item(spec, 400, 0), item(spec, 400, 1, 'update'), item(spec, 400, 0, 'delete')],
                  f'{spec.kind}-same-key-create-update-delete')
    context.write([item(spec, 400, 2)], f'{spec.kind}-reinsert-after-delete', single=True)
    context.write([item(spec, 400, 0, 'delete')], f'{spec.kind}-pure-delete', single=True)
    context.write([item(spec, 999, 0, 'delete')], f'{spec.kind}-absent-delete', single=True)


def derived(context, spec):
    context.write([item(spec, 500, 1, derived=True)], f'{spec.kind}-single-derived-key', single=True)
    context.write([item(spec, number, 1, derived=True) for number in (501, 502)],
                  f'{spec.kind}-batch-derived-key')


def conditions(context, spec):
    context.expected[spec.kind][999] = None
    context.refuse(item(spec, 999, 9, 'update', must_exist=True), 400,
                   f'{spec.kind}-must-exist-missing-update', 'update target not found')
    context.write([item(spec, 0, 2, 'update', mode='queued')],
                  f'{spec.kind}-valid-after-must-exist-refusal', single=True)
    context.refuse(item(spec, 0, 9, 'update', expected={
        'type': 'value', 'field': 'title', 'value': 'generated-value-that-is-not-current'}),
        409, f'{spec.kind}-wrong-current-field-value', 'cas_conflict')
    context.write([item(spec, 0, 3, 'update', mode='queued')],
                  f'{spec.kind}-valid-after-condition-refusal', single=True)
    context.write([item(spec, 0, 4, 'update', mode='durable', expected={
        'type': 'value', 'field': 'title', 'value': spec.values(0, 3)['title']})],
        f'{spec.kind}-valid-current-field-condition', single=True)


def concurrent(context):
    jobs = [(spec, [item(spec, 1000 + worker * 4 + offset, 10) for offset in range(4)])
            for spec in context.specs for worker in range(2)]
    for revision in (10, 11):
        entries = [[item(spec, entry['number'], revision,
                         'create' if revision == 10 else 'update', mode='queued')
                    for entry in batch] for spec, batch in jobs]
        barrier = threading.Barrier(len(entries))
        def send(batch):
            barrier.wait(timeout=30)
            return context.node.call('/api/mutations/batch', {'mutations': [entry['operation'] for entry in batch]})
        with ThreadPoolExecutor(max_workers=len(entries)) as pool:
            replies = list(pool.map(send, entries))
        wanted = {}
        for reply, batch in zip(replies, entries):
            wanted.update(context.accept_reply(reply, batch, f'parallel-revision-{revision}'))
        context.observe(wanted)
        context.read_all(f'parallel-revision-{revision}-after-all-acks')


def run(context):
    for spec in context.specs:
        modes(context, spec)
        mixed_and_erasure(context, spec)
        derived(context, spec)
        conditions(context, spec)
    concurrent(context)
