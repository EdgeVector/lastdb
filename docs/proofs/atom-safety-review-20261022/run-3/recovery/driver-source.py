#!/usr/bin/env python3
"""Complete the product operation after the empty recent copy missed observation."""
import argparse
import hashlib
import importlib.util
import json
import sys
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('safety_capture', HERE / 'capture.py')
safety = importlib.util.module_from_spec(spec)
spec.loader.exec_module(safety)
from recover_run import owned_root, cleanup
base = safety.product.base


class Continuation(safety.SafetyCapture):
    def __init__(self, args):
        self.root, ownership = owned_root(args.run)
        original = cleanup.load(args.run / 'summary.json')
        if original.get('complete') is not False or original.get('all_owned_processes_stopped') is not True:
            raise ValueError('The preceding product operation must be incomplete and stopped')
        self.binary, self.recovery = args.binary.resolve(strict=True), args.recovery.resolve(strict=True)
        self.maintain = args.maintain.resolve(strict=True)
        self.output = args.run / 'recovery'
        self.output.mkdir(mode=0o700, exist_ok=False)
        base.save(self.output / 'ownership.json', ownership)
        base.save(self.output / 'inputs.json', {'source_revision': args.source_revision,
            'binary_sha256': base.file_hash(self.binary), 'recovery_sha256': base.file_hash(self.recovery),
            'maintenance_sha256': base.file_hash(self.maintain),
            'driver_sha256': base.file_hash(Path(__file__)),
            'safety_driver_sha256': base.file_hash(HERE / 'capture.py'), 'original_run_complete': False,
            'preceding_operation': 'Empty recent copy completed before payload observation'})
        self.identity = ownership['run_id']
        self.home = self.root / 'home'
        self.socket, self.full_socket = self.root / 'q.sock', self.root / 'f.sock'
        self.process, self.log, self.sequence = None, None, 0
        self.receipt_lock = threading.Lock()
        self.timings, self.compactions, self.oracles = [], [], []
        self.records = 4096
        self.expected = {base.key(number): base.fields(number, 7) for number in range(self.records)}
        for revision in range(9):
            for number in range(128 + revision * 8, 136 + revision * 8):
                self.expected[base.key(number)] = base.fields(number, 300 + revision)
        for number in range(256, 264):
            self.expected.pop(base.key(number))
        for number in range(256, 260):
            self.expected[base.key(number)] = base.fields(number, 305)
        for offset in (16, 64):
            for number in range(offset, offset + 8):
                self.expected[base.key(number)] = base.fields(number, 515)
        self.long_keys = []
        for number, revision in [(3, 7)] + [(4 + number, 300 + number) for number in range(9)]:
            key = f'long-{number}-' + 'x' * 1536
            value = base.fields(number, revision)
            value['id'] = key
            self.expected[key] = value
            self.long_keys.append(key)
        key = 'review-wide-' + 'w' * 3500
        value = base.fields(9000, 901)
        value['id'] = key
        value['body'] = ''.join(hashlib.sha256(f'large-native-payload-901:{part}'.encode()).hexdigest()
                                for part in range(2048))
        self.expected[key] = value
        self.long_keys.append(key)
        self.novel_expected = {f'novel-{number:03d}': {'bucket': 'novel', 'id': f'novel-{number:03d}',
            'novel_body': hashlib.sha256(f'novel-{number}'.encode()).hexdigest()} for number in range(32)}
        events = [json.loads(line) for line in (args.run / 'receipts.jsonl').read_text().splitlines()]
        self.previous_oracle = [event for event in events if event['operation'] == 'immutable-version-oracle'][-1]
        base.save(self.output / 'recipe.json', {
            'source_revision': args.source_revision, 'driver_sha256': base.file_hash(Path(__file__)),
            'preceding_selected_sha256': self.previous_oracle['selected_sha256'],
            'initial_main_records': len(self.expected), 'initial_novel_records': len(self.novel_expected),
            'explicit_full_copy_interruptions': 2, 'post_interrupt_new_keys': [30, 31],
            'remaining_operations': ['full-copy interruption and reopen',
                'new key and equal-tuple full copy', 'copy-phase backup', 'restore', 'complete counter tally']})

    def compact(self, full_copy=True):
        return super().compact(full_copy)

    def run(self):
        self.start()
        self.verify('continued-accepted-values')
        self.oracle('continued-native-state')
        if self.oracles[-1]['selected_sha256'] != self.previous_oracle['selected_sha256']:
            raise RuntimeError('The complete native tuple set differs after the missed observation')
        for iteration in range(2):
            before = self.oracle(f'interrupt-{iteration}-before')
            self.interrupt_copy()
            if self.oracle(f'interrupt-{iteration}-after') != before:
                raise RuntimeError('An interrupted full copy changed native immutable tuples')
            self.write_long(30 + iteration, 800 + iteration)
            self.compact_equal(f'post-interrupt-{iteration}', True)
        self.backup_during_copy()
        self.restore_bundle()
        self.verify_counters()
        self.flush('final')
        base.save(self.output / 'oracles.json', self.oracles)
        base.save(self.output / 'compactions.json', self.compactions)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    for name in ('binary', 'recovery', 'maintain'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    args = parser.parse_args()
    args.run = args.run.resolve(strict=True)
    capture, complete, error = Continuation(args), False, None
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
        base.save(capture.output / 'summary.json', {'complete': complete, 'error': error,
            'original_run_complete': False, 'scratch': str(capture.root),
            'all_owned_processes_stopped': capture.process is None,
            'expected_live_records': len(capture.expected), 'novel_live_records': len(capture.novel_expected),
            'data_retained_for_review': True,
            'scope': 'Independent generated product use and process interruption; no power-loss claim'})
    print(json.dumps({'complete': complete, 'error': error, 'output': str(capture.output)}))
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
