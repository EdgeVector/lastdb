#!/usr/bin/env python3
"""Capture real daemon writes and compaction on fresh generated homes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
PROOF = Path(__file__).resolve().parent
ROOT = PROOF.parents[2]
spec = importlib.util.spec_from_file_location(
    'integrated', PROOF.parent / 'durable-write-integrated-20261022/capture.py')
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
capture, schemas, save = old.capture, old.schemas, old.save


class Node(capture.Node):
    warm_bytes = 268435456

    def start(self):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(('LASTDB_', 'FOLDDB_'))
               and key not in ('OBS_SENTRY_DSN', 'SENTRY_DSN')}
        env.update(LASTDB_ISOLATED_COPY='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
                   FOLDDB_DISABLE_KEYCHAIN='1', LASTDB_UDS_WORKERS='8',
                   LASTDB_DEPENDENCY_HIGH_WATER=str(self.high_water),
                   LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS='0',
                   LASTDB_AUTO_MAINTENANCE=str(int(self.maintenance)),
                   LASTDB_SNAPSHOT_TIMING=str(int(self.profile)),
                   LASTDB_CORE_WARM_BYTES=str(self.warm_bytes),
                   RUST_MIN_STACK='67108864', RUST_LOG='warn')
        self.process = subprocess.Popen([str(self.binary), '--data-dir', str(self.home)],
                                        env=env, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f'owned daemon exited {self.process.returncode}')
            try:
                self.call('/api/db/native-status')
                return
            except (OSError, TimeoutError):
                time.sleep(.1)
        raise RuntimeError('owned daemon did not become ready')


def inventory(node):
    total, files, deepest = 0, 0, 0
    root = node.home / 'data/disk-pack'
    # Exact owned home only; traversal stops at six levels.
    for parent, directories, names in os.walk(root):
        depth = len(Path(parent).relative_to(root).parts)
        deepest = max(deepest, depth)
        if depth >= 6:
            directories.clear()
        for name in names:
            try:
                total += (Path(parent) / name).stat().st_size
                files += 1
            except FileNotFoundError:
                pass
    return {'time_us': time.time_ns() // 1000, 'bytes': total, 'files': files,
            'deepest': deepest, 'native': node.call('/api/db/native-status')}


def sustain(node, done, output):
    samples, compactions = [], []
    while not done.is_set():
        samples.append(inventory(node))
        receipt = capture.base.compact(node)
        compactions.append(receipt)
        save(output / 'compactions.json', compactions)
        save(output / 'inventory.json', samples)
        done.wait(1)
    samples.append(inventory(node))
    save(output / 'inventory.json', samples)


def recover(node, args, expected):
    old.fast.original_recovery(node, args, expected)
    rows = json.loads((args.output / 'writes.json').read_text())['samples']
    wanted = {identity for row in rows for identity in row['receipt']['mutation_ids']}
    events, cursor = [], None
    for _ in range((len(wanted) + 500) // 500 + 2):
        page = node.call('/api/app/changes', {'since_cursor': cursor, 'limit': 500})
        events.extend(page['changes'])
        cursor = page['next_cursor']
        if not page['has_more']:
            break
    else:
        raise RuntimeError('notice replay exceeded generated request bound')
    found = [event['mutation_id'] for event in events if event['mutation_id'] in wanted]
    if len(found) != len(wanted) or set(found) != wanted:
        raise RuntimeError('acknowledged mutation notices differ after forced stop')
    save(args.output / 'notice-count.json', {'expected': len(wanted), 'found': len(found),
                                           'all_once': True})
    (args.output / 'notices.json.gz').write_bytes(gzip.compress(
        json.dumps(events, separators=(',', ':')).encode(), mtime=0))


def run(binary, scratch, name, clients=1, batch=1, count=1000, profile=False, sustained=False):
    output = PROOF / name
    output.mkdir()
    home = scratch / ('h' + str(len(list(scratch.glob('h*')))))
    home.mkdir()
    args = argparse.Namespace(output=output, count=count, schemas=64, clients=clients, batch_size=batch)
    node = Node(binary, home, scratch / (name + '.log'), 4096, False, workers=8)
    node.profile, node.maintenance = profile, True
    save(output / 'inputs.json', {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
         'home': str(home), 'generated_data_only': True, 'schemas': 64, 'calls': count,
         'clients': clients, 'batch': batch, 'distinct_schemas_between_clients': True,
         'profile': profile, 'auto_maintenance': True, 'sustained_compaction': sustained,
         'durability': 'omitted; durable required', 'load_before': os.getloadavg()})
    done = threading.Event()
    try:
        node.start()
        schemas.seed(node, 64, output)
        if sustained:
            with ThreadPoolExecutor(max_workers=1) as pool:
                job = pool.submit(sustain, node, done, output)
                try:
                    expected = schemas.measured_calls(node, args)
                finally:
                    done.set()
                job.result(timeout=90)
        else:
            expected = schemas.measured_calls(node, args)
        values = schemas.check_values(node, expected)
        save(output / 'readback.json', values)
        if not values['all_values_match']:
            raise RuntimeError('acknowledged values differ')
        recover(node, args, expected)
        save(output / 'shutdown.json', {'exit_code': node.stop()})
        save(output / 'native-shutdown.json', json.loads((home / '.shutdown_flush_ready').read_text()))
    except Exception as error:
        save(output / 'failure.json', {'error': str(error)})
        raise
    finally:
        done.set()
        node.stop()
        node.log.close()
        old.preserve_log(scratch, name, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--mode', choices=('smoke', 'bench', 'sustain'), required=True)
    args = parser.parse_args()
    if os.environ.get('FOLD_HOST_CARGO_LOCK_HELD') != '1':
        raise RuntimeError('hold the compiler exclusion for the whole capture')
    scratch = args.scratch.resolve(strict=True)
    if scratch.stat().st_uid != os.getuid():
        raise RuntimeError('scratch owner differs')
    if args.mode == 'smoke':
        run(scratch / 'candidate', scratch, 'smoke', 4, 8, 128, True)
    elif args.mode == 'bench':
        for label in ('before', 'candidate'):
            run(scratch / label, scratch, label + '-single')
            run(scratch / label, scratch, label + '-concurrent', 4, 8)
        run(scratch / 'candidate', scratch, 'candidate-profile', 4, 8, 512, True)
        run(scratch / 'candidate', scratch, 'sustained', 4, 8, 4096, True, True)
        subprocess.run([sys.executable, str(PROOF / 'semantics.py'), '--scratch', str(scratch)], check=True)
    else:
        run(scratch / 'candidate', scratch, 'sustained', 4, 8, 4096, True, True)


if __name__ == '__main__':
    main()
