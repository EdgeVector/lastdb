#!/usr/bin/env python3
"""Independent product capture: native immutable versions, copy cancellation, and reopen."""
import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import sys
import time
from pathlib import Path
import subprocess

SOURCE = Path(__file__).resolve().parents[1] / 'atom-lifecycle-completion-20261022'
sys.path.insert(0, str(SOURCE))
spec = importlib.util.spec_from_file_location('lifecycle_product', SOURCE / 'product_run.py')
product = importlib.util.module_from_spec(spec)
spec.loader.exec_module(product)


class SafetyCapture(product.ProductRun):
    def __init__(self, args):
        super().__init__(args)
        self.maintain = args.maintain.resolve(strict=True)
        inputs = json.loads((self.output / 'inputs.json').read_text())
        inputs.update(independent_driver_sha256=product.base.file_hash(Path(__file__)),
                      maintain_sha256=product.base.file_hash(self.maintain),
                      scope='Independent safety use on generated data; no power-loss claim',
                      update_rounds=16, deleted=list(range(256,264)), recreated=list(range(256,260)))
        product.base.save(self.output / 'inputs.json', inputs)
        product.base.save(self.output / 'recipe.json', {
            'records': self.records, 'seed_batch': 48, 'seed_revision': 7,
            'writer_threads': 2, 'rounds_per_writer': 16, 'keys_per_writer_round': 8,
            'recent_copies': 9, 'interrupted_copies': 2, 'cancelled_callers': 1,
            'wide_key_bytes': 20012, 'large_body_bytes': 131072,
            'source_revision': args.source_revision,
            'driver_sha256': product.base.file_hash(Path(__file__)),
            'scope': 'Fresh generated product data; no primary, cloud, or power-loss claim'})
        self.oracles = []

    def oracle(self, phase):
        physical = self.home / 'data' / 'disk-pack'
        process = subprocess.Popen([str(self.maintain), 'records', '--home', str(physical)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        selected, total, counts, tail, groups = {}, 0, {}, None, set()
        for line in process.stdout:
            value = json.loads(line)
            if 'collection' not in value:
                tail = value
                continue
            total += 1
            collection = value['collection']
            groups.add((collection, value['group']))
            counts[collection] = counts.get(collection, 0) + 1
            if collection == 'atoms' or (collection == 'tips'
                    and value['key'].startswith(('mk:', 'mh:'))):
                coordinate = (collection, value['key'])
                if coordinate in selected:
                    raise RuntimeError('Native administrative stream repeats an exact key')
                selected[coordinate] = (value['batch'], value['operation'], value['body_sha256'],
                                        value['body_bytes'], value['group'])
        error = process.stderr.read()
        code = process.wait(timeout=60)
        if code or tail is None or tail.get('result', {}).get('records') != total:
            raise RuntimeError(f'Native record stream incomplete: {code}, {error[:500]}')
        if not selected:
            raise RuntimeError(f'No immutable atom/tip records in native stream: {counts}')
        digest = hashlib.sha256(json.dumps(sorted(selected.items()), separators=(',', ':')).encode()).hexdigest()
        receipt = {'phase': phase, 'native_records': total, 'by_collection': counts,
                   'selected_records': len(selected), 'selected_sha256': digest,
                   'selection': 'atoms collection and mk:/mh: molecule records in tips',
                   'generation': tail['result']['generation'],
                   'tuple': ['collection', 'key', 'batch', 'operation', 'body_sha256', 'body_bytes', 'group']}
        self.last_groups = groups
        receipt['native_groups'] = len(groups)
        self.oracles.append(receipt)
        self.remember('immutable-version-oracle', **receipt)
        return selected

    def compact_equal(self, phase, full_copy):
        before = self.oracle(phase + '-before')
        self.compact(full_copy)
        after = self.oracle(phase + '-after')
        if before != after:
            removed = set(before) - set(after)
            changed = [coordinate for coordinate in set(before).intersection(after)
                       if before[coordinate] != after[coordinate]]
            self.remember('oracle-difference', removed=len(removed), changed=len(changed),
                          added=len(set(after) - set(before)))
            raise RuntimeError(f'{phase}: native immutable version/body tuples changed')
        self.verify(phase)

    def cancel_caller(self):
        before = self.oracle('cancel-caller-before')
        connection = product.base.UnixConnection(self.socket)
        payload = json.dumps({'dry_run': False, 'grace_ms': 0, 'full_copy': True})
        connection.request('POST', '/api/db/compact', payload, {'Content-Type': 'application/json'})
        observed = None
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            state = self.call('/api/db/native-status')['native_disk']['parts']
            if state.get('compaction_phase') == 'copy':
                observed = state
                break
            time.sleep(0.01)
        connection.close()
        if observed is None:
            raise RuntimeError('Caller cancellation lacks a copy-phase observation')
        self.remember('cancelled-http-caller', observed=observed)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            state = self.call('/api/db/native-status')['native_disk']['parts']
            if state.get('compaction_phase') == 'idle':
                break
            time.sleep(0.05)
        else:
            raise RuntimeError('The detached compaction owner did not reach idle')
        if state.get('maintenance_runs', 0) <= observed.get('maintenance_runs', 0):
            raise RuntimeError('The cancelled caller lacks a published maintenance result')
        self.remember('cancelled-caller-published', final=state)
        if self.oracle('cancel-caller-after') != before:
            raise RuntimeError('Cancelled caller changed native immutable tuples')
        self.verify('cancelled-caller')

    def create_novel_schema(self):
        name = 'AtomSafetyNovel'
        fields = ['bucket', 'id', 'novel_body']
        self.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': name, 'descriptive_name': name,
            'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': fields, 'field_descriptions': {field: f'independent novel {field}' for field in fields}}})
        self.novel_expected = {f'novel-{number:03d}': {'bucket': 'novel', 'id': f'novel-{number:03d}',
            'novel_body': hashlib.sha256(f'novel-{number}'.encode()).hexdigest()} for number in range(32)}
        self.call('/api/mutations/batch', {'mutations': [{'type': 'mutation', 'schema': name,
            'mutation_type': 'create', 'durability': 'durable',
            'key_value': {'hash': 'novel', 'range': key}, 'fields_and_values': value}
            for key, value in self.novel_expected.items()]})

    def verify(self, phase):
        super().verify(phase)
        if not hasattr(self, 'novel_expected'):
            return
        result = self.call('/api/query', {'schema_name': 'AtomSafetyNovel',
            'fields': ['bucket', 'id', 'novel_body'], 'filter': {'HashRangeKeys':
                [['novel', key] for key in self.novel_expected]}, 'limit': 32})
        actual = {record['key']['range']: record['fields'] for record in result.get('results', [])
                  if record['key']['hash'] == 'novel'}
        if actual != self.novel_expected or result.get('returned_count') != 32:
            raise RuntimeError(f'{phase}: the new schema differs from its durable writes')
        self.remember('novel-schema-readback', phase=phase, records=len(actual),
                      sha256=hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest())

    def write_large(self):
        key = 'review-wide-' + 'w' * 20000
        fields = product.base.fields(9000, 900)
        fields['id'] = key
        fields['body'] = ''.join(hashlib.sha256(f'large-native-payload-{part}'.encode()).hexdigest()
                                 for part in range(2048))
        self.call('/api/mutations/batch', {'mutations': [{'type': 'mutation',
            'schema': product.base.SCHEMA, 'mutation_type': 'create', 'durability': 'durable',
            'key_value': {'hash': product.base.HASH, 'range': key}, 'fields_and_values': fields}]})
        self.expected[key] = fields
        self.long_keys.append(key)
        self.remember('wide-key-large-body', key_bytes=len(key.encode()),
                      body_bytes=len(fields['body'].encode()))

    def run(self):
        self.start()
        self.install()
        for first in range(0, self.records, 48):
            self.mutate('create', range(first, min(first + 48, self.records)), 7, durability='durable')
        self.write_long(3, 7)
        self.write_large()
        self.flush('initial')
        self.verify('initial')
        self.compact_equal('first-full', True)
        for revision in range(9):
            self.mutate('update', range(128 + revision * 8, 136 + revision * 8), 300 + revision,
                        durability='durable')
            self.write_long(4 + revision, 300 + revision)
            if revision == 2:
                self.mutate('delete', range(256, 264), 0, durability='durable')
            if revision == 5:
                self.mutate('create', range(256, 260), 305, durability='durable')
            self.compact_equal(f'recent-{revision}', False)
        def writer(offset):
            for revision in range(16):
                self.mutate('update', range(offset, offset + 8), 500 + revision, durability='durable')
        before_groups = set(self.last_groups)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(writer, offset) for offset in (16, 64)]
            copy = pool.submit(self.compact, True)
            for _ in range(1000):
                phase = self.call('/api/db/native-status')['native_disk']['parts']['compaction_phase']
                if phase == 'copy':
                    break
                if copy.done():
                    raise RuntimeError('No copy phase for the new-schema write')
                time.sleep(0.01)
            else:
                raise RuntimeError('The copy phase did not start')
            self.create_novel_schema()
            observed = self.call('/api/db/native-status')['native_disk']['parts']
            self.remember('new-schema-during-compaction', observed=observed)
            if observed['compaction_phase'] not in ('copy', 'catch_up'):
                raise RuntimeError('The new-schema writes lack a pre-handoff phase receipt')
            copy.result()
            for future in futures:
                future.result()
        self.oracle('novel-after-catch-up')
        added_groups = self.last_groups - before_groups
        self.remember('new-native-groups', count=len(added_groups))
        if not added_groups:
            raise RuntimeError('The new schema did not create a native group')
        self.verify('concurrent-catch-up')
        self.compact_equal('stable-after-concurrent', False)
        self.cancel_caller()
        self.flush('before-interruption')
        for iteration in range(2):
            before = self.oracle(f'interrupt-{iteration}-before')
            self.interrupt_copy()
            if self.oracle(f'interrupt-{iteration}-after') != before:
                raise RuntimeError('Interrupted private copy changed immutable tuples')
            self.write_long(30 + iteration, 800 + iteration)
            self.compact_equal(f'post-interrupt-{iteration}', True)
        self.backup_during_copy()
        self.restore_bundle()
        self.verify_counters()
        self.flush('final')
        product.base.save(self.output / 'oracles.json', self.oracles)
        product.base.save(self.output / 'compactions.json', self.compactions)
        if not any(receipt.get('catch_up_records', 0) for receipt in self.compactions):
            raise RuntimeError('No concurrent catch-up receipt')
        if not any(receipt.get('tier_boundary', 0) for receipt in self.compactions):
            raise RuntimeError('No recent-tier receipt')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('scratch', 'binary', 'recovery', 'maintain', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--records', type=int, default=2048)
    args = parser.parse_args()
    if not 1024 <= args.records <= 10000:
        parser.error('records must be 1024..10000')
    capture, complete, error = SafetyCapture(args), False, None
    try:
        capture.run()
        complete = True
    except Exception as failure:
        error = f'{type(failure).__name__}: {failure}'
        capture.remember('failure', error=error)
    finally:
        try:
            capture.stop()
        except Exception as failure:
            complete, error = False, f'{error or ""}; shutdown: {failure}'
        product.base.save(capture.output / 'summary.json', {'complete': complete, 'error': error,
            'scratch': str(capture.root), 'home': str(capture.home),
            'all_owned_processes_stopped': capture.process is None,
            'expected_live_records': len(capture.expected), 'data_retained_for_review': True,
            'scope': 'Independent generated product use; process interruption, not power loss'})
    print(json.dumps({'complete': complete, 'error': error, 'output': str(capture.output)}))
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
