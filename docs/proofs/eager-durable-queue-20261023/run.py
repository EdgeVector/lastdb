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
import tempfile

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parent
ROOT = SOURCE.parents[2]
PROOF = None
spec = importlib.util.spec_from_file_location(
    'integrated', SOURCE.parent / 'durable-write-integrated-20261022/capture.py')
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
capture, schemas, save = old.capture, old.schemas, old.save


def external_path(path):
    path = path.resolve()
    if path == ROOT or ROOT in path.parents:
        raise RuntimeError('generated output and daemon homes must stay outside the source tree')
    return path


def configure_output(path=None):
    global PROOF
    if path is None:
        state = Path(os.environ.get('XDG_STATE_HOME', str(Path.home() / '.local/state')))
        root = Path(os.environ.get('LAST_STACK_EVIDENCE_ROOT',
                    os.environ.get('RUNNER_TEMP', str(state / 'last-stack/artifacts/lastdb'))))
        root = external_path(root)
        root.mkdir(parents=True, exist_ok=True)
        path = Path(tempfile.mkdtemp(prefix='eager-durable-', dir=root))
    PROOF = external_path(path)
    PROOF.mkdir(parents=True, exist_ok=True)
    old.PROOF = PROOF
    capture.PROOF = PROOF
    return PROOF


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
        if node.compaction_mode != 'automatic':
            compactions.append(compact(node))
        save(output / 'compactions.json', compactions)
        save(output / 'inventory.json', samples)
        done.wait(1)
    samples.append(inventory(node))
    save(output / 'inventory.json', samples)


def compact(node):
    began = time.time_ns() // 1000
    connection = schemas.measure.UnixHTTP(node.socket)
    try:
        body = {'dry_run': False, 'grace_ms': 0, 'report_usage': True,
                'full_copy': node.compaction_mode == 'full'}
        status, receipt, _ = schemas.measure.request(connection, '/api/db/compact', body)
        return {'start_unix_us': began, 'end_unix_us': time.time_ns() // 1000,
                'status': status, 'receipt': receipt}
    except Exception as error:
        return {'start_unix_us': began, 'end_unix_us': time.time_ns() // 1000,
                'error': str(error)}
    finally:
        connection.close()


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


def run(binary, scratch, name, clients=1, batch=1, count=1000, profile=False,
        sustained=False, disk_cap=None, compaction_mode='full', high_water=4096):
    scratch = external_path(scratch)
    if PROOF is None:
        raise RuntimeError('configure an external output directory before the capture')
    external_path(PROOF)
    output = PROOF / name
    output.mkdir()
    home = scratch / ('h' + str(len(list(scratch.glob('h*')))))
    home.mkdir()
    args = argparse.Namespace(output=output, count=count, schemas=64, clients=clients, batch_size=batch)
    node = Node(binary, home, scratch / (name + '.log'), high_water, False, workers=8)
    node.profile, node.maintenance, node.disk_cap = profile, True, disk_cap
    node.compaction_mode = compaction_mode
    save(output / 'inputs.json', {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
         'home': str(home), 'generated_data_only': True, 'schemas': 64, 'calls': count,
         'clients': clients, 'batch': batch, 'distinct_schemas_between_clients': True,
         'profile': profile, 'auto_maintenance': True, 'sustained_compaction': sustained,
         'compaction_mode': compaction_mode, 'dependency_high_water': high_water,
         'disk_cap_bytes': disk_cap,
         'durability': 'omitted; durable required', 'load_before': os.getloadavg()})
    done = threading.Event()
    try:
        node.start()
        schemas.seed(node, 64, output)
        save(output / 'write-start.json', inventory(node))
        if sustained:
            with ThreadPoolExecutor(max_workers=1) as pool:
                job = pool.submit(sustain, node, done, output)
                try:
                    expected = schemas.measured_calls(node, args)
                    save(output / 'write-end.json', inventory(node))
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
        if sustained:
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


def compaction_progress(output, mode):
    writes = json.loads((output / 'writes.json').read_text())
    first = min(row['start_unix_us'] for row in writes['samples'])
    last = max(row['end_unix_us'] for row in writes['samples'])
    samples = [row for row in json.loads((output / 'inventory.json').read_text())
               if first <= row['time_us'] <= last]
    before = writes['before']['native_disk']['maintenance_runs']
    runs = max([row['native']['native_disk']['maintenance_runs'] for row in samples] + [before]) - before
    rows = json.loads((output / 'compactions.json').read_text())
    active = [row for row in rows if row.get('status') == 200
              and first <= row['start_unix_us'] <= row['end_unix_us'] <= last]
    reports = [row['receipt']['compact']['native_disk'] for row in active]
    removed = sum(row['bytes_removed'] for row in reports)
    result = {'maintenance_runs_observed_during_writes': runs,
              'explicit_completed_during_writes': len(active),
              'explicit_bytes_removed_during_writes': removed,
              'observed_inventory_peak_bytes': max((row['bytes'] for row in samples), default=None),
              'tier_publications': sum(row['tier_boundary'] > 0 for row in reports),
              'mode': mode, 'latency': writes['summary']}
    save(output / 'compaction-progress.json', result)
    if runs == 0 or (mode != 'automatic' and removed == 0):
        raise RuntimeError('CI capture does not establish active compaction and explicit space release')
    if mode == 'tier' and not result['tier_publications']:
        raise RuntimeError('CI tier capture contains no active tier publication')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output-root', type=Path)
    parser.add_argument('--mode', choices=('automatic', 'tier', 'full'), required=True)
    parser.add_argument('--calls', type=int, default=4096)
    parser.add_argument('--high-water', type=int, default=128)
    args = parser.parse_args()
    if (os.environ.get('GITHUB_ACTIONS') != 'true'
            or os.environ.get('RUNNER_ENVIRONMENT') != 'github-hosted'):
        raise RuntimeError('captures require GitHub-hosted Actions; local benchmarks are retired')
    if args.calls <= 0 or args.calls % 4 or args.high_water <= 0:
        raise RuntimeError('use a positive call count divisible by four and a positive high water')
    scratch = external_path(args.scratch.resolve(strict=True))
    if scratch.stat().st_uid != os.getuid():
        raise RuntimeError('scratch owner differs')
    output = configure_output(args.output_root)
    binary = args.binary.resolve(strict=True)
    save(output / 'source-identity.json', {'source_revision': args.source_revision,
         'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
         'command': sys.argv, 'generated_data_only': True})
    run(binary, scratch, args.mode, 4, 8, args.calls, True, True,
        compaction_mode=args.mode, high_water=args.high_water)
    compaction_progress(output / args.mode, args.mode)


if __name__ == '__main__':
    main()
