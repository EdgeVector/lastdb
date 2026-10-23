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
    disk_cap = None

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
                   RUST_MIN_STACK='67108864', RUST_LOG='warn,fold_db::compaction=debug')
        if self.disk_cap is not None:
            env['LASTDB_DISK_CAP_BYTES'] = str(self.disk_cap)
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
                                           'all_once': True, 'last_cursor': cursor})
    (args.output / 'notices.json.gz').write_bytes(gzip.compress(
        json.dumps(events, separators=(',', ':')).encode(), mtime=0))


def run(binary, scratch, name, clients=1, batch=1, count=1000, profile=False, sustained=False, disk_cap=None):
    output = PROOF / name
    output.mkdir()
    home = scratch / ('h' + str(len(list(scratch.glob('h*')))))
    home.mkdir()
    args = argparse.Namespace(output=output, count=count, schemas=64, clients=clients, batch_size=batch)
    node = Node(binary, home, scratch / (name + '.log'), 4096, False, workers=8)
    node.profile, node.maintenance, node.disk_cap = profile, True, disk_cap
    save(output / 'inputs.json', {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
         'home': str(home), 'generated_data_only': True, 'schemas': 64, 'calls': count,
         'clients': clients, 'batch': batch, 'distinct_schemas_between_clients': True,
         'profile': profile, 'auto_maintenance': True, 'sustained_compaction': sustained,
         'disk_cap_bytes': disk_cap,
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
        if name in ('groups-sustained', 'counts-sustained', 'barrier-sustained'):
            erase_after_compaction(node, output, expected)
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


def erase_after_compaction(node, output, expected):
    cursor = json.loads((output / 'notice-count.json').read_text())['last_cursor']
    names = [schemas.schema_name(n) for n in range(2)]
    body = {'mutations': [{'type': 'mutation', 'schema': name, 'mutation_type': operation,
            'key_value': {'hash': name, 'range': 'record-000'}, 'fields_and_values': {}}
            for name, operation in zip(names, ('delete', 'purge'))]}
    receipt = node.call('/api/mutations/batch', body)
    save(output / 'post-compaction-erase.json', receipt)
    if receipt.get('durability') != 'durable' or len(receipt['mutation_ids']) != 2:
        raise RuntimeError('post-compaction erase did not return two durable identities')
    save(output / 'post-erase-forced-stop.json', {'exit_code': node.stop(signal.SIGKILL)})
    node.start()
    for name in names:
        expected[name].pop('record-000')
    expected[names[0]]['sentinel'] = hashlib.sha256(b'after-restart').hexdigest()
    values = schemas.check_values(node, expected)
    queries = [{'schema_name': name, 'fields': ['value'],
                'filter': {'HashRangeKeys': [[name, 'record-000']]}, 'limit': 1} for name in names]
    absent = old.independent_queries(node, queries)
    notices = node.call('/api/app/changes', {'since_cursor': cursor, 'limit': 10})
    result = {'values': values, 'erased': absent, 'notices': notices}
    save(output / 'post-erase-recovery.json', result)
    if (not values['all_values_match'] or any(row['results'] for row in absent)
            or {row['mutation_id'] for row in notices['changes']} != set(receipt['mutation_ids'])):
        raise RuntimeError('post-compaction Delete/Purge recovery differs')


def verify_refused(scratch, name='capped-recovery'):
    output = PROOF / name
    output.mkdir()
    inputs = json.loads((PROOF / 'final-capped/inputs.json').read_text())
    writes = json.loads((PROOF / 'final-capped/writes.json').read_text())
    home = Path(inputs['home']).resolve(strict=True)
    home.relative_to(scratch)
    if home.stat().st_uid != os.getuid() or not inputs['generated_data_only']:
        raise RuntimeError('capped home ownership differs')
    node = Node(scratch / 'candidate', home, scratch / (name + '.log'),
                4096, False, workers=8)
    node.profile, node.maintenance, node.disk_cap = False, False, 1073741824
    rows = writes['samples']
    wanted = {identity for row in rows if row['ok'] for identity in row['receipt']['mutation_ids']}
    if any(row['status'] != 507 for row in rows if not row['ok']):
        raise RuntimeError('unexpected capped refusal')
    try:
        node.start()
        values = schemas.check_values(node, writes['expected_sha256'])
        events = old.notices(node)
        found = [event['mutation_id'] for event in events]
        failed = {row['schema'] for row in rows if not row['ok']} - writes['expected_sha256'].keys()
        queries = [{'schema_name': name, 'fields': ['value'],
                    'filter': {'HashRangeKeys': [[name, f'record-{n:03}'] for n in range(8)]},
                    'limit': 8} for name in failed]
        absent = old.independent_queries(node, queries)
        space = inventory(node)
        result = {'accepted_calls': sum(row['ok'] for row in rows),
                  'refused_calls': sum(not row['ok'] for row in rows),
                  'all_values_match': values['all_values_match'], 'wanted_notices': len(wanted),
                  'actual_notices': len(found), 'notices_exact': len(found) == len(wanted) and set(found) == wanted,
                  'failed_new_keys_absent': all(not r['results'] for r in absent),
                  'inventory': space, 'cap': node.disk_cap,
                  'binary_sha256': hashlib.sha256(node.binary.read_bytes()).hexdigest()}
        save(output / 'result.json', result)
        if not (result['all_values_match'] and result['notices_exact']
                and result['failed_new_keys_absent'] and space['bytes'] <= node.disk_cap):
            raise RuntimeError('capped refusal or acknowledged recovery differs')
        save(output / 'shutdown.json', {'exit_code': node.stop()})
    finally:
        node.stop()
        node.log.close()
        old.preserve_log(scratch, name, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--mode', choices=('smoke', 'bench', 'sustain', 'final', 'metadata', 'groups', 'counts', 'barrier'), required=True)
    args = parser.parse_args()
    if os.environ.get('FOLD_HOST_CARGO_LOCK_HELD') != '1':
        raise RuntimeError('hold the compiler exclusion for the whole capture')
    scratch = args.scratch.resolve(strict=True)
    if scratch.stat().st_uid != os.getuid():
        raise RuntimeError('scratch owner differs')
    if args.mode in ('groups', 'counts', 'barrier'):
        prefix = args.mode
        run(scratch / 'candidate', scratch, prefix + '-smoke', 4, 8, 128, True)
        run(scratch / 'candidate', scratch, prefix + '-sustained', 4, 8, 4096, True, True)
        run(scratch / 'candidate', scratch, prefix + '-single', 1, 1, 1000, True)
        run(scratch / 'before', scratch, prefix + '-before-single', 1, 1, 1000, True)
        run(scratch / 'candidate', scratch, prefix + '-concurrent', 4, 8, 1000, True)
        run(scratch / 'before', scratch, prefix + '-before-concurrent', 4, 8, 1000, True)
        subprocess.run([sys.executable, str(PROOF / 'semantics.py'), '--scratch', str(scratch),
                        '--name', prefix + '-semantics', '--home-name', 'hmo' if prefix == 'groups' else ('hcnt' if prefix == 'counts' else 'hbar')], check=True)
        old.boundaries(scratch / 'candidate', scratch, prefix + '-boundaries', 'hgd' if prefix == 'groups' else ('hct' if prefix == 'counts' else 'hbr'))
        verify_refused(scratch, prefix + '-capped-recovery')
    elif args.mode == 'metadata':
        run(scratch / 'candidate', scratch, 'metadata-smoke', 4, 8, 128, True)
        run(scratch / 'candidate', scratch, 'metadata-sustained', 4, 8, 4096, True, True)
        run(scratch / 'candidate', scratch, 'metadata-single', 1, 1, 1000, True)
        run(scratch / 'before', scratch, 'metadata-before-single', 1, 1, 1000, True)
        run(scratch / 'candidate', scratch, 'metadata-concurrent', 4, 8, 1000, True)
        run(scratch / 'before', scratch, 'metadata-before-concurrent', 4, 8, 1000, True)
        subprocess.run([sys.executable, str(PROOF / 'semantics.py'), '--scratch', str(scratch),
                        '--name', 'metadata-semantics', '--home-name', 'hmg'], check=True)
        old.PROOF = PROOF
        old.boundaries(scratch / 'candidate', scratch, 'metadata-boundaries', 'hc')
        verify_refused(scratch)
    elif args.mode == 'final':
        run(scratch / 'candidate', scratch, 'final-smoke', 4, 8, 128, True)
        run(scratch / 'before', scratch, 'repeat-before-single')
        run(scratch / 'candidate', scratch, 'final-single')
        run(scratch / 'candidate', scratch, 'final-concurrent', 4, 8)
        run(scratch / 'before', scratch, 'repeat-before-concurrent', 4, 8)
        run(scratch / 'candidate', scratch, 'final-profile', 4, 8, 512, True)
        run(scratch / 'before', scratch, 'repeat-before-single-profile', 1, 1, 512, True)
        run(scratch / 'candidate', scratch, 'final-single-profile', 1, 1, 512, True)
        run(scratch / 'candidate', scratch, 'final-sustained', 4, 8, 4096, True, True)
        subprocess.run([sys.executable, str(PROOF / 'semantics.py'), '--scratch', str(scratch),
                        '--name', 'final-semantics'], check=True)
        old.PROOF = PROOF
        old.boundaries(scratch / 'candidate', scratch, 'final-boundaries', 'hb')
        run(scratch / 'candidate', scratch, 'final-capped', 4, 8, 512, True, True, 1073741824)
    elif args.mode == 'smoke':
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
