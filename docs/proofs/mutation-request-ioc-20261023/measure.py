#!/usr/bin/env python3
"""Measure single-mutation latency on fresh, generated local node homes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import statistics
import subprocess
import time

SCHEMA = 'c010ab517caf93bb988f2e961cd5f169e0584609718caf183d80b9f484e7864e'
COUNT = 300


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def stats(samples):
    ordered = sorted(samples)
    result = {'count': len(ordered), 'mean_ms': statistics.mean(ordered)}
    for name, fraction in (('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99)):
        result[name] = ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]
    return result


def host_load():
    result = subprocess.run(['ps', '-axo', 'pid,pcpu,comm'], capture_output=True,
                            text=True, check=True)
    processes = sorted(result.stdout.splitlines()[1:],
                       key=lambda line: float(line.split()[1]), reverse=True)[:10]
    return {'load_average': os.getloadavg(), 'top_cpu_processes': processes}


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=30)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


def request(path, route, body=None):
    payload = None if body is None else json.dumps(body, separators=(',', ':')).encode()
    connection = UnixConnection(path)
    started = time.perf_counter_ns()
    try:
        connection.request('GET' if body is None else 'POST', route, payload,
                           {'Content-Type': 'application/json',
                            'X-LastDB-Client': 'mutation-ioc-benchmark'})
        response = connection.getresponse()
        raw = response.read()
        elapsed = (time.perf_counter_ns() - started) / 1e6
        status = response.status
    finally:
        connection.close()
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = raw.decode()
    if status != 200:
        raise RuntimeError(f'{route}: HTTP {status}: {result}')
    return result, elapsed


def value(number):
    return f'{number:012d}:' + 'x' * 1011


def mutation(key, content, kind='update', expected=None):
    body = {'type': 'mutation', 'schema': SCHEMA, 'durability': 'durable',
            'fields_and_values': {'receipt_id': key, 'receipt_value': content},
            'key_value': {'hash': key, 'range': None}, 'mutation_type': kind}
    if expected is not None:
        body['expected'] = {'type': 'value', 'field': 'receipt_value', 'value': expected}
    return body


class Run:
    def __init__(self, args, name, label):
        self.args, self.name, self.label = args, name, label
        self.root = args.scratch / name
        self.root.mkdir(mode=0o700)
        self.home = self.root / 'h'
        self.home.mkdir()
        self.narrow, self.full = self.root / 'n', self.root / 'f'
        self.process, self.log = None, None
        self.samples, self.current = [], {}
        self.phases = {}
        self.deadline = None

    def start(self):
        environment = dict(os.environ, ENVIRONMENT='prod', EXEMEM_ENV='prod',
                           LASTDB_EPHEMERAL='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
                           LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                           OTEL_SDK_DISABLED='true', SENTRY_DSN='', FOLD_SENTRY_DSN='',
                           RUST_LOG='warn')
        for name in ('RUST_MIN_STACK', 'FOLD_SCHEMA_SERVICE_URL', 'LASTDB_SCHEMA_PRELOAD_SCHEMAS'):
            environment.pop(name, None)
        self.log = (self.args.output / (self.name + '.log')).open('a')
        self.process = subprocess.Popen([str(getattr(self.args, self.label)),
            '--data-dir', str(self.home), '--socket-path', str(self.narrow),
            '--full-socket-path', str(self.full)], env=environment,
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('Owned node exited before startup')
            if self.narrow.exists():
                try:
                    request(self.narrow, '/health')
                    break
                except (OSError, http.client.HTTPException):
                    pass
            time.sleep(.05)
        else:
            raise RuntimeError('Owned node startup deadline expired')
        loaded, _ = request(self.full, '/api/schemas/load', {'schemas': [SCHEMA]})
        if loaded.get('schemas_loaded_to_db') != 1 or loaded.get('failed_schemas'):
            raise RuntimeError(f'Published schema unavailable: {loaded}')

    def write(self, phase, body):
        if self.deadline is not None and time.monotonic() > self.deadline:
            raise RuntimeError('Measurement exceeded its 180-second deadline')
        result, elapsed = request(self.narrow, '/api/mutation', body)
        if result.get('success') is not True or result.get('durability') != 'durable':
            raise RuntimeError('Missing successful durable receipt')
        if result.get('operations', {}).get('total') != 1:
            raise RuntimeError('Unexpected mutation operation count')
        self.samples.append({'phase': phase, 'ms': elapsed})
        return body['fields_and_values']['receipt_value']

    def workloads(self):
        self.deadline = time.monotonic() + 180
        for number in range(64):
            key = f'key-{number:04d}'
            self.current[key] = self.write('warmup', mutation(key, value(number), 'create'))
        for number in range(64):
            key = f'key-{number:04d}'
            self.current[key] = self.write('warmup', mutation(key, value(number + 64)))
        for number in range(COUNT):
            self.write('create', mutation(f'create-{number:04d}', value(number), 'create'))
        for phase in ('update', 'conditional_update'):
            for number in range(COUNT):
                key = f'key-{number % 64:04d}'
                expected = self.current[key] if phase == 'conditional_update' else None
                self.current[key] = self.write(phase, mutation(key, value(number + 2000), expected=expected))
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(self.concurrent_writer, range(4)))
        concurrent_seconds = time.perf_counter() - started
        for number in range(COUNT):
            key = f'key-{number % 64:04d}'
            body = {'schema_name': SCHEMA, 'fields': ['receipt_value'], 'filter': {'HashKey': key}}
            result, elapsed = request(self.narrow, '/api/query', body)
            if result.get('returned_count') != 1 or result['results'][0]['fields']['receipt_value'] != self.current[key]:
                raise RuntimeError('Read by key differs from the acknowledged value')
            self.samples.append({'phase': 'read_control', 'ms': elapsed})
        for phase in ('create', 'update', 'conditional_update', 'concurrent4_update', 'read_control'):
            self.phases[phase] = stats([s['ms'] for s in self.samples if s['phase'] == phase])
        self.phases['concurrent4_update']['requests_per_second'] = 400 / concurrent_seconds

    def concurrent_writer(self, client):
        key = f'key-{client:04d}'
        for number in range(100):
            self.current[key] = self.write('concurrent4_update', mutation(key, value(10000 + number)))

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                raise RuntimeError('Owned node required a forced shutdown')
        if self.log is not None:
            self.log.close()
        if self.process is not None and self.process.returncode != 0:
            raise RuntimeError(f'Owned node exited {self.process.returncode}')

    def cleanup(self):
        resolved = self.root.resolve(strict=True)
        if (self.root.is_symlink() or resolved.parent != self.args.scratch.resolve()
                or resolved.stat().st_uid != os.getuid() or self.process.poll() is None):
            raise RuntimeError('Owned scratch cleanup refused')
        opened = subprocess.run(['lsof', '+D', str(resolved)], capture_output=True, text=True)
        if opened.returncode != 1 or opened.stdout.strip() or opened.stderr.strip():
            raise RuntimeError('Owned scratch still has an open file or an inspection error')
        shutil.rmtree(resolved)


def verify_values(run):
    queries = [{'schema_name': SCHEMA, 'fields': ['receipt_value'], 'filter': {'HashKey': key}}
               for key in run.current]
    response, _ = request(run.narrow, '/api/queries/batch', {'queries': queries})
    if response.get('count') != len(run.current):
        raise RuntimeError('Final batch read returned an unexpected count')
    for item, expected in zip(response['results'], run.current.values()):
        body = item['response']
        if item['status'] != 200 or body.get('returned_count') != 1:
            raise RuntimeError('Final batch read did not return one value per key')
        if body['results'][0]['fields']['receipt_value'] != expected:
            raise RuntimeError('Final batch read differs from the acknowledged value')
    return {'keys': len(run.current), 'all_values_match': True}


def execute(args, name, label):
    run = Run(args, name, label)
    summary = {'label': label, 'name': name, 'host_before': host_load(), 'complete': False}
    try:
        run.start()
        run.workloads()
        summary['batch_read_receipt'] = verify_values(run)
        summary['phases'] = run.phases
        summary['memory_kib_at_end'] = int(subprocess.check_output(
            ['ps', '-p', str(run.process.pid), '-o', 'rss='], text=True).strip())
        run.stop()
        run.start()
        summary['reopen_read_receipt'] = verify_values(run)
        summary['complete'] = True
    except BaseException as error:
        summary['error'] = str(error)
        raise
    finally:
        run.stop()
        summary.update(host_after=host_load(), node_exit=run.process.returncode)
        save(args.output / (name + '-samples.json'), run.samples)
        save(args.output / (name + '.json'), summary)
        run.cleanup()
    print(name, json.dumps(run.phases), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('before', 'after', 'scratch', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=4)
    args = parser.parse_args()
    args.scratch = args.scratch.resolve(strict=True)
    if args.scratch.parent != Path(os.environ['TMPDIR']).resolve() or not args.scratch.name.startswith('lastdb-ioc-bench.'):
        raise SystemExit('Use an owned mktemp directory under TMPDIR')
    args.output.mkdir(parents=True, exist_ok=True)
    receipt = {'rounds': args.rounds, 'value_bytes': 1024, 'schema': SCHEMA,
               'requests_per_phase': COUNT, 'warmup_writes': 128,
               'concurrent_clients': 4, 'concurrent_requests': 400,
               'profile': 'dev-release: opt-level=3, lto=off, codegen-units=16',
               'cloud': False, 'primary_data_used': False, 'default_worker_stack': True,
               'binaries': {}, 'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    for label in ('before', 'after'):
        path = getattr(args, label).resolve(strict=True)
        receipt['binaries'][label] = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'version': subprocess.check_output([str(path), '--version'], text=True).strip()}
    save(args.output / 'inputs.json', receipt)
    summaries = []
    for pair in range(args.rounds):
        for label in (('before', 'after') if pair % 2 == 0 else ('after', 'before')):
            summaries.append(execute(args, f'{pair + 1}-{label}', label))
            save(args.output / 'runs.json', summaries)
    comparison = {}
    for phase in summaries[0]['phases']:
        comparison[phase] = {}
        for metric in ('p50_ms', 'p95_ms', 'p99_ms'):
            medians = {label: statistics.median(s['phases'][phase][metric]
                for s in summaries if s['label'] == label) for label in ('before', 'after')}
            comparison[phase][metric] = dict(medians, change_percent=100 * (medians['after'] / medians['before'] - 1),
                                             change_ms=medians['after'] - medians['before'])
    save(args.output / 'comparison.json', comparison)
    print('Comparison:', json.dumps(comparison, indent=2), flush=True)


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise InterruptedError(f'Measurement interrupted by signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    main()
