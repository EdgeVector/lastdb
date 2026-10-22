#!/usr/bin/env python3
"""Record real durable writes during explicit cleanup on a fresh generated home."""
import argparse
from concurrent.futures import ThreadPoolExecutor
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
SOURCE = Path(__file__).resolve().parent.parent / 'native-commit-barriers-20261022'
sys.path.insert(0, str(SOURCE))
SPEC = importlib.util.spec_from_file_location('node_owner', SOURCE/'capture.py')
owner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(owner)
import transport
import workload
save = workload.save


class Node(owner.Node):
    def start(self):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(('LASTDB_', 'FOLDDB_')) and k not in ('OBS_SENTRY_DSN', 'SENTRY_DSN')}
        env.update(LASTDB_ISOLATED_COPY='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
                   FOLDDB_DISABLE_KEYCHAIN='1', LASTDB_AUTO_MAINTENANCE='0',
                   LASTDB_RESIDENT_PERSIST_MS='500', LASTDB_UDS_WORKERS='8',
                   RUST_MIN_STACK='67108864', RUST_LOG='warn')
        self.process = subprocess.Popen([str(self.binary), '--data-dir', str(self.home)],
                                        env=env, stdout=self.log, stderr=self.log)
        until = time.monotonic()+90
        while time.monotonic() < until:
            if self.process.poll() is not None:
                raise RuntimeError('owned daemon exited before readiness')
            try:
                self.call('/api/db/native-status')
                return
            except (OSError, TimeoutError):
                time.sleep(.1)
        raise RuntimeError('owned daemon did not become ready')


def run(node, args):
    workload.seed(node, 64, args.output)
    expected = {}
    # This creates real old generations. A pack with a one-day grace retains
    # their files so the separate reclaim call has actual deletion work.
    for turn in range(args.backlog):
        name = workload.schema_name(turn % 64)
        body, values = workload.payload(name, turn, 8, 'create' if turn < 64 else 'update')
        for mutation in body['mutations']:
            mutation['durability'] = 'durable'
        receipt = node.call('/api/mutations/batch', body)
        if receipt.get('durability') != 'durable':
            raise RuntimeError('setup write did not complete on disk')
        expected[name] = values
        if (turn+1) % 64 == 0:
            print(json.dumps({'setup_batches': turn+1}), flush=True)
    save(args.output/'pack.json', node.call('/api/db/compact', {'dry_run': False, 'grace_ms': 86400000}))
    save(args.output/'before.json', node.call('/api/db/native-status'))
    barrier = threading.Barrier(5)
    maintenance = {}

    def cleanup():
        barrier.wait()
        maintenance['start_unix_us'] = time.time_ns()//1000
        maintenance['report'] = node.call('/api/db/compact',
            {'dry_run': False, 'reclaim_only': True, 'grace_ms': 0})
        maintenance['end_unix_us'] = time.time_ns()//1000

    def writer(client):
        names = [workload.schema_name(i) for i in range(client, 64, 4)]
        rows, values = [], {}
        connection = transport.UnixHTTP(node.socket)
        barrier.wait()
        # Let the cleanup call enter the server before the first write arrives.
        time.sleep(.02)
        try:
            for turn in range(args.count//4):
                name = names[turn % len(names)]
                body, record_values = workload.payload(name, 10000+4*turn+client, 8, 'update')
                for mutation in body['mutations']:
                    mutation['durability'] = 'durable'
                code, receipt, elapsed = transport.request(connection, '/api/mutations/batch', body)
                ok = code == 200 and receipt.get('durability') == 'durable' and len(receipt.get('mutation_ids', [])) == 8
                rows.append({'client': client, 'schema': name, 'latency_ms': elapsed, 'status': code,
                    'ok': ok, 'start_unix_us': connection.last_window[0],
                    'end_unix_us': connection.last_window[1], 'receipt': receipt})
                if not ok:
                    break
                values[name] = record_values
        finally:
            connection.close()
        return rows, values

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as pool:
        cleaning = pool.submit(cleanup)
        writes = list(pool.map(writer, range(4)))
        cleaning.result()
    rows = sorted([row for result in writes for row in result[0]], key=lambda row: row['start_unix_us'])
    expected.update({name: value for result in writes for name, value in result[1].items()})
    summary = workload.summarize(rows, time.perf_counter()-start, 8)
    summary['completed_during_cleanup'] = sum(maintenance['start_unix_us'] < row['end_unix_us'] < maintenance['end_unix_us'] for row in rows)
    save(args.output/'writes.json', {'summary': summary, 'maintenance': maintenance, 'samples': rows})
    print(json.dumps(summary), flush=True)
    if len(rows) != args.count or summary['errors']:
        raise RuntimeError('the real workload contains refused or missing requests')
    save(args.output/'after.json', node.call('/api/db/native-status'))
    result = workload.check_values(node, expected)
    save(args.output/'memory-read.json', result)
    if not result['all_values_match']:
        raise RuntimeError('known-key reads differ from acknowledged values')
    # Every measured call requested disk completion. No separate flush precedes the stop.
    save(args.output/'forced-stop.json', {'exit_code': node.stop(signal.SIGKILL), 'signal': 'SIGKILL'})
    node.start()
    result = workload.check_values(node, expected)
    save(args.output/'recovery.json', result)
    if not result['all_values_match']:
        raise RuntimeError('recovery differs from durable acknowledgments')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--home', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--mode', default='main')
    parser.add_argument('--count', type=int, default=1000)
    parser.add_argument('--backlog', type=int, default=64)
    args = parser.parse_args()
    if args.home.exists() or args.count < 64 or args.count % 4 or args.backlog < 64:
        raise RuntimeError('use a fresh generated home and valid counts')
    if len(str(args.home.resolve()/'data/folddb-full.sock').encode())+4 > 103:
        raise RuntimeError('use a shorter macOS socket path')
    args.output.mkdir(parents=True, exist_ok=False)
    args.home.mkdir()
    args.high_water, args.uds_workers = 4096, 8
    save(args.output/'inputs.json', {'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        'source_commit': args.source_commit, 'home': str(args.home.resolve()), 'generated_data': True,
        'count': args.count, 'backlog': args.backlog, 'schemas': 64, 'clients': 4,
        'records_per_batch': 8, 'durability_parameter': 'durable', 'automatic_maintenance': False,
        'memory_flush_ms': 500, 'disk_cap_bytes': None, 'load_before': os.getloadavg()})
    node = Node(args)
    try:
        node.start()
        run(node, args)
        save(args.output/'shutdown.json', {'exit_code': node.stop()})
        save(args.output/'shutdown-marker.json', json.loads((node.home/'.shutdown_flush_ready').read_text()))
    except Exception as error:
        save(args.output/'failure.json', {'type': type(error).__name__, 'message': str(error)})
        raise
    finally:
        node.stop()
        node.log.close()


if __name__ == '__main__':
    main()
