#!/usr/bin/env python3
"""Verify the complete accepted state after the oversized API key was refused."""
import argparse
import importlib.util
import json
import sys
import threading
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / 'atom-lifecycle-completion-20261022'
sys.path.insert(0, str(SOURCE))
from recover_run import owned_root, cleanup
from product_run import ProductRun, base


class RefusalRecovery(ProductRun):
    def __init__(self, args):
        self.root, ownership = owned_root(args.run)
        original = cleanup.load(args.run / 'summary.json')
        if original.get('complete') is not False or original.get('all_owned_processes_stopped') is not True:
            raise ValueError('The rejected capture must be incomplete and stopped')
        self.binary = args.binary.resolve(strict=True)
        self.output = args.run / 'recovery'
        self.output.mkdir(mode=0o700, exist_ok=False)
        base.save(self.output / 'ownership.json', ownership)
        base.save(self.output / 'inputs.json', {'source_revision': args.source_revision,
            'binary_sha256': base.file_hash(self.binary), 'driver_sha256': base.file_hash(Path(__file__)),
            'original_run_complete': False, 'state': '2048 seed records at revision 7; long-3 at revision 7'})
        self.identity = ownership['run_id']
        self.home = self.root / 'home'
        self.socket, self.full_socket = self.root / 'q.sock', self.root / 'f.sock'
        self.process, self.log, self.sequence = None, None, 0
        self.receipt_lock = threading.Lock()
        self.timings, self.compactions = [], []
        self.records = 2048
        self.expected = {base.key(number): base.fields(number, 7) for number in range(self.records)}
        long_key = 'long-3-' + 'x' * 1536
        value = base.fields(3, 7)
        value['id'] = long_key
        self.expected[long_key] = value
        self.long_keys = [long_key]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    args = parser.parse_args()
    args.run = args.run.resolve(strict=True)
    capture, complete, error = RefusalRecovery(args), False, None
    try:
        capture.start()
        capture.verify('all-accepted-values-after-format-refusal')
        capture.verify_counters()
        capture.flush('format-refusal-verified')
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
            'expected_live_records': len(capture.expected), 'data_retained_for_review': True})
    print(json.dumps({'complete': complete, 'error': error, 'output': str(capture.output)}))
    return 0 if complete else 1


if __name__ == '__main__':
    raise SystemExit(main())
