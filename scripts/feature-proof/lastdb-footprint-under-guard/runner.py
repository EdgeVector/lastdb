#!/usr/bin/env python3
"""Run a memory workload on a new private copy of an explicit stopped snapshot."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import uuid

from proof import SAMPLE_GAUGES, evaluate, number


CLIENT = 'lru-memory-proof'
MAX_RESPONSE = 32 * 1024 * 1024


def check_isolation(path, primary):
    path, primary = path.resolve(), primary.resolve()
    if path == primary or path in primary.parents or primary in path.parents:
        raise ValueError('proof path overlaps the primary home')
    return path


def validate_owned_node(state, home, version):
    reported = str(state.get('binary_version', '')).split()
    if (state.get('running') is not True or not reported or reported[-1] != version
            or not isinstance(state.get('pid'), int) or state['pid'] <= 0
            or not isinstance(state.get('schemas'), int) or state['schemas'] <= 0
            or Path(state.get('dev_home', '/')).resolve() != home.resolve()):
        raise ValueError('private node ownership, candidate, or schema proof failed')


def validate_clone_errors(path):
    # The installed helper permits socket copy failures. All other copy
    # errors make this proof incomplete, even if selected reads still work.
    if not path.is_file():
        raise ValueError('clone error evidence is absent')
    if path.stat().st_size > 1024 * 1024:
        raise ValueError('clone error evidence exceeds the proof limit')
    for line in path.read_text().splitlines():
        if line.strip() and not line.endswith(': Operation not supported on socket'):
            raise ValueError('the private clone reported a non-socket copy error')


def validate_workload(document):
    if (not isinstance(document, dict) or not document.get('provenance')
            or not number(document.get('cycle_secs')) or document['cycle_secs'] <= 0
            or not isinstance(document.get('operations'), list) or not document['operations']):
        raise ValueError('explicit workload provenance, positive cycle_secs, and operations are required')
    names = set()
    for operation in document['operations']:
        name = operation.get('name')
        if not isinstance(name, str) or not name or name in names:
            raise ValueError('workload operation names must be unique')
        names.add(name)
        if operation.get('method') != 'POST' or operation.get('path') not in ('/api/query', '/api/mutation'):
            raise ValueError('workload permits only local query and mutation operations')
        body = operation.get('body')
        if not isinstance(body, dict) or not number(operation.get('min_rows')):
            raise ValueError('an operation requires a JSON body and min_rows')
        if operation['path'] == '/api/query':
            filter_value = body.get('filter')
            if (operation['min_rows'] < 1
                    or not body.get('schema_name') or not isinstance(body.get('fields'), list)
                    or not body['fields'] or not isinstance(filter_value, dict)
                    or len(filter_value) != 1
                    or next(iter(filter_value)) not in ('HashKey', 'HashRangeKey', 'HashRangePrefix', 'HashRange')
                    or not isinstance(body.get('limit'), int) or not 0 < body['limit'] <= 1000):
                raise ValueError('queries require explicit fields, an anchored filter, and a bounded limit')
        elif not all(key in body for key in ('type', 'schema', 'fields_and_values', 'key_value', 'mutation_type')):
            raise ValueError('mutation body lacks required wire keys')
    return document


def result_rows(response, path):
    if response.get('ok') is not True:
        raise ValueError('request did not return ok=true')
    if path == '/api/query':
        if not isinstance(response.get('results'), list):
            raise ValueError('query result rows are absent')
        return len(response['results'])
    return 0


def status_evidence(status, issued):
    return [
        {key: row.get(key) for key in ('request_id', 'kind', 'status', 'cold_shard_loads')}
        for row in status.get('request_ops', {}).get('recent', [])
        if row.get('request_id') in issued and row.get('client') == CLIENT
    ]


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=30)
        self.socket_path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.socket_path)


def request(sock, method, path, body=None, request_id=None):
    connection = UnixConnection(sock)
    try:
        headers = {'Content-Type': 'application/json', 'X-LastDB-Client': CLIENT}
        if request_id:
            headers['X-LastDB-Request-Id'] = request_id
        connection.request(method, path, json.dumps(body) if body is not None else None, headers)
        response = connection.getresponse()
        raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise ValueError('response exceeds the proof byte limit')
        return response.status, json.loads(raw)
    finally:
        connection.close()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def append_json(path, value):
    with path.open('a') as stream:
        stream.write(json.dumps(value, allow_nan=False) + '\n')


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def stop_group(process):
    """Stop only the process group this runner created, including helper children."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    # The helper can exit before a child. Kill the owned group even after the
    # leader exits; its PID cannot be reused while the group still exists.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.communicate()


def run_owned_command(command, environment, timeout):
    process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        output, _ = process.communicate(timeout=timeout)
        if process.returncode:
            raise RuntimeError(f'owned helper failed with exit {process.returncode}')
        return output
    except BaseException:
        stop_group(process)
        raise


def helper(command, environment, timeout=1800):
    # The helper can print mirrored environment values. Keep its raw output
    # out of proof artifacts and report only the command and exit status.
    return run_owned_command(['lastdb-dev', *command], environment, timeout)


def interrupted(signum, _frame):
    raise InterruptedError(f'proof interrupted by signal {signum}')


def sample(sock):
    code, response = request(sock, 'GET', '/api/status')
    if code != 200 or response.get('ok') is not True:
        raise ValueError('status read failed')
    status = response['status']
    budget = status['memory_budget']
    output = {key: status.get(key) for key in ('sampled_at', 'process_start_ts', 'phys_footprint_bytes')}
    output.update({key: budget.get(key) for key in (
        'implied_footprint_multiplier', 'malloc_bytes_in_use', 'malloc_bytes_held_free',
        'warm_budget_bytes', 'effective_warm_budget_bytes', 'eviction_events',
        'footprint_net_bytes', 'warm_bytes_freed')})
    output['file_blob_rehydrates'] = status.get('resident', {}).get('file_blob_rehydrates')
    output['allocator_name'] = budget.get('allocator_name')
    output['observed_at'] = time.time()
    return output


def await_sample(sock, max_age, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = sample(sock)
        if (all(number(row.get(key)) for key in SAMPLE_GAUGES)
                and row['malloc_bytes_in_use'] > 0
                and 0 <= row['observed_at'] - row['sampled_at'] <= max_age):
            return row
        time.sleep(5)
    raise ValueError('complete fresh sampler evidence is unavailable before the workload')


def collect_status_proof(sock, manifest, report_dir):
    # One fixed pair, after the measured workload. Do not retry positive
    # observations until a passing pair happens to appear.
    issued = {'status-' + uuid.uuid4().hex for _ in range(2)}
    manifest['status_request_ids'] = sorted(issued)
    manifest['status_phase'] = 'after-workload'
    for request_id in issued:
        code, _ = request(sock, 'GET', '/api/status', request_id=request_id)
        if code != 200:
            raise ValueError('status probe request failed')
    deadline = time.monotonic() + 240
    found = {}
    while time.monotonic() < deadline and len(found) != len(issued):
        code, response = request(sock, 'GET', '/api/status?recent=1')
        if code == 200:
            for row in status_evidence(response.get('status', {}), issued):
                found[row['request_id']] = row
            write_json(report_dir / 'status-proof-progress.json', {
                'issued': sorted(issued), 'found': list(found.values()),
                'sampled_at': response.get('status', {}).get('sampled_at'),
            })
        if len(found) != len(issued):
            time.sleep(5)
    return list(found.values())


def call_operation(sock, operation, sequence):
    started = time.monotonic()
    # Only the sequence token changes. No shell expansion or eval occurs.
    body = json.loads(json.dumps(operation['body']).replace('${sequence}', str(sequence)))
    event = {'name': operation['name'], 'min_rows': operation['min_rows'],
             'observed_at': time.time(), 'ok': False, 'status': 0, 'rows': 0}
    try:
        code, response = request(sock, operation['method'], operation['path'], body)
        event['status'] = code
        event['rows'] = result_rows(response, operation['path'])
        event['ok'] = 200 <= code < 300 and event['rows'] >= event['min_rows']
    except (OSError, ValueError, KeyError, http.client.HTTPException) as error:
        # Error payloads may contain record values. Retain only the error class.
        event['error_class'] = type(error).__name__
    event['elapsed_ms'] = (time.monotonic() - started) * 1000
    return event


def run(args):
    primary = Path.home() / '.lastdb'
    if args.snapshot_home.is_symlink():
        raise ValueError('snapshot home must not be a symlink')
    snapshot = check_isolation(args.snapshot_home, primary)
    if snapshot.is_symlink() or not (snapshot / 'identity.key').is_file() or not (snapshot / 'data').is_dir():
        raise ValueError('snapshot must be an existing isolated LastDB home')
    # These are root-only globs, not recursive walks. No copied backup
    # configuration may reconnect this proof to a production publisher.
    if list(snapshot.glob('cloud_sync.json*')) or list(snapshot.glob('.cloud_sync.json.tmp*')):
        raise ValueError('snapshot still contains cloud connection state')
    # Refuse a source that currently serves a socket. A snapshot is never booted.
    source_socket = snapshot / 'data/folddb.sock'
    if source_socket.exists():
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(1)
            probe.connect(str(source_socket))
        except (ConnectionRefusedError, FileNotFoundError):
            pass
        else:
            raise ValueError('snapshot source has a live listener')
        finally:
            probe.close()
    candidate = args.candidate.resolve(strict=True)
    if '/target/debug/' in str(candidate):
        raise ValueError('an explicit release candidate is required')
    version = subprocess.check_output([str(candidate), '--version'], text=True, timeout=10).strip().split()[-1]
    paired = subprocess.check_output([str(candidate.with_name('lastdb')), '--version'], text=True, timeout=10).strip().split()[-1]
    if version != paired or 'dirty' in version:
        raise ValueError('candidate pair must be clean and have equal versions')
    workload = validate_workload(json.loads(args.workload.read_text()))
    if args.duration_secs >= 86400 and not any(op['path'] == '/api/mutation' for op in workload['operations']):
        raise ValueError('the long factory-load proof requires explicit writes as well as reads')
    report_dir = check_isolation(args.report_dir, primary)
    check_isolation(report_dir, snapshot)
    report_dir.mkdir(parents=True, exist_ok=False)
    root = Path(tempfile.mkdtemp(prefix='lru-memory-proof-', dir='/tmp'))
    home = root / 'home'
    env = os.environ.copy()
    for key in list(env):
        if 'DSN' in key or key in ('LASTDB_HOME', 'FOLDDB_HOME', 'FOLD_SYNC_DEVICE_ID'):
            env.pop(key)
    env.update(LASTDB_DEV_HOME=str(home), LASTDB_DEV_STATE=str(report_dir / 'node-state'),
               LASTDB_DEV_PRIMARY_HOME=str(snapshot))
    evidence = {'manifest': {}, 'samples': [], 'requests': [], 'status_observations': []}
    manifest = evidence['manifest']
    manifest.update(candidate_version=version, candidate_sha256=sha256(candidate),
                    source_git_oid=args.source_git_oid, workload_sha256=sha256(args.workload),
                    workload_provenance=workload['provenance'], snapshot_id=str(snapshot),
                    duration_secs=args.duration_secs, sample_interval_secs=args.sample_secs,
                    max_sample_age_secs=150, isolation_verified=False,
                    expected_allocator=args.allocator)
    manifest['harness_sha256'] = {name: sha256(Path(__file__).with_name(name))
                                 for name in ('runner.py', 'proof.py')}
    marker = snapshot / 'laststore_high_water.json'
    if not marker.is_file():
        raise ValueError('snapshot high-water metadata is absent')
    # This binds the copy's durable metadata. It is not a full content hash.
    manifest['snapshot_high_water_sha256'] = sha256(marker)
    write_json(report_dir / 'manifest.json', manifest)
    report = {'ok': False, 'full_allocator_proof': False, 'reason': 'run did not finish'}
    try:
        print('phase=clone-and-boot', flush=True)
        helper(['up', '--bin', str(candidate)], env)
        validate_clone_errors(report_dir / 'node-state/clone.err')
        state = json.loads(helper(['status', '--json'], env, 60))
        validate_owned_node(state, home, version)
        manifest['isolation_verified'] = True
        write_json(report_dir / 'node.json', {key: state[key] for key in
                   ('pid', 'dev_home', 'socket', 'binary_version', 'schemas')})
        sock = home / 'data/folddb.sock'
        print('phase=workload-preflight', flush=True)
        for index, operation in enumerate(workload['operations']):
            event = call_operation(sock, operation, f'warmup-{index}')
            append_json(report_dir / 'preflight.jsonl', event)
            if not event['ok']:
                raise ValueError('workload preflight failed; inspect the numeric request record')
        print('phase=sampler-preflight', flush=True)
        first = await_sample(sock, manifest['max_sample_age_secs'])
        manifest['process_start_ts'] = first['process_start_ts']
        started = time.monotonic()
        manifest['started_at'] = time.time()
        cycle = workload['cycle_secs']
        manifest['required_calls'] = {op['name']: max(1, int(args.duration_secs // cycle))
                                      for op in workload['operations']}
        next_cycle = next_sample = started
        sequence = 0
        print('phase=workload', flush=True)
        while time.monotonic() - started < args.duration_secs:
            now = time.monotonic()
            if now >= next_sample:
                row = sample(sock)
                evidence['samples'].append(row)
                append_json(report_dir / 'samples.jsonl', row)
                write_json(report_dir / 'progress.json', {'elapsed_secs': now - started,
                           'sampled_at': row['sampled_at'], 'requests': len(evidence['requests']),
                           'phys_footprint_bytes': row['phys_footprint_bytes']})
                if row['process_start_ts'] != manifest['process_start_ts']:
                    raise ValueError('private node process changed')
                if not number(row['phys_footprint_bytes']) or row['phys_footprint_bytes'] >= 16 * 1024**3:
                    raise ValueError('private node reaches the physical safety limit')
                next_sample = now + args.sample_secs
            if now >= next_cycle:
                for operation in workload['operations']:
                    event = call_operation(sock, operation, sequence)
                    evidence['requests'].append(event)
                    append_json(report_dir / 'requests.jsonl', event)
                    if not event['ok']:
                        raise ValueError('workload request failed')
                sequence += 1
                next_cycle += cycle
            time.sleep(min(1, max(0, min(next_cycle, next_sample) - time.monotonic())))
        evidence['samples'].append(sample(sock))
        manifest['elapsed_secs'] = time.monotonic() - started
        manifest['finished_at'] = time.time()
        print('phase=status-proof', flush=True)
        evidence['status_observations'] = collect_status_proof(sock, manifest, report_dir)
        report = evaluate(evidence)
        if sha256(marker) != manifest['snapshot_high_water_sha256']:
            report['ok'] = False
            report.setdefault('failures', []).append('source_snapshot_metadata_changed')
    except (Exception, KeyboardInterrupt) as error:
        # Do not retain helper output, response bodies, or arbitrary error text.
        report = {'ok': False, 'full_allocator_proof': False,
                  'failure_class': type(error).__name__}
    finally:
        print('phase=owned-node-stop', flush=True)
        try:
            helper(['stop'], env, 120)
            state = json.loads(helper(['status', '--json'], env, 60))
            if state.get('running') is not False:
                raise RuntimeError('owned node still runs after stop')
            report['owned_node_stopped'] = True
        except (Exception, KeyboardInterrupt) as error:
            report.update(ok=False, owned_node_stopped=False,
                          cleanup_failure_class=type(error).__name__)
        write_json(report_dir / 'evidence.json', evidence)
        write_json(report_dir / 'report.json', report)
        write_json(report_dir / 'manifest.json', manifest)
        # Retain the isolated copy for failure analysis; never recursively remove
        # an operator-selected path. The report names this task-owned location.
        write_json(report_dir / 'retained-home.json', {'home': str(home)})
    print(json.dumps(report), flush=True)
    return 0 if report['ok'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--source-git-oid', required=True)
    parser.add_argument('--snapshot-home', type=Path, required=True)
    parser.add_argument('--workload', type=Path, required=True)
    parser.add_argument('--report-dir', type=Path, required=True)
    parser.add_argument('--duration-secs', type=float, default=1200)
    parser.add_argument('--sample-secs', type=float, default=10)
    parser.add_argument('--allocator', choices=('mimalloc', 'system'), default='mimalloc')
    args = parser.parse_args()
    if not number(args.duration_secs) or args.duration_secs <= 0 or not number(args.sample_secs) or args.sample_secs <= 0:
        parser.error('duration and sample interval must be positive finite numbers')
    if not re.fullmatch('[0-9a-f]{40}', args.source_git_oid):
        parser.error('source-git-oid must be a complete lowercase Git commit ID')
    if not shutil.which('lastdb-dev'):
        parser.error('the installed lastdb-dev helper is required')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    return run(args)


if __name__ == '__main__':
    raise SystemExit(main())
