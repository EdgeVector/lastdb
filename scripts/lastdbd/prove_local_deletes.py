#!/usr/bin/env python3
"""Prove exact Delete absence on a new, local-only Mini home."""
import argparse
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import uuid


class ProofError(Exception):
    pass


def require(condition, code):
    if not condition:
        raise ProofError(code)


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def query_body(schema, key, fields, tombstones):
    shape = {'HashRangeKey': key} if key['range'] is not None else {'HashKey': key['hash']}
    return dict(schema_name=schema, fields=fields, filter=shape,
                include_tombstones=tombstones, limit=2, offset=0)


def rows(document):
    result = document.get('results')
    require(document.get('ok') is True and isinstance(result, list), 'query_not_ok')
    expected = dict(returned_count=len(result), unresolved_rows=0, tombstoned_rows=0,
                    limit=2, offset=0, has_more=False, next_cursor=None)
    require(all(k in document and document[k] == v for k, v in expected.items()),
            'query_incomplete')
    require('key_form' in document and (document['key_form'] is None if not result
            else bool(document['key_form'])), 'query_key_form')
    return result


def content(document):
    result = rows(document)
    require(len(result) == 1, 'expected_one_record')
    return {'key': result[0]['key'], 'fields': result[0]['fields']}


def mutation(schema, key, fields, kind):
    return dict(type='mutation', schema=schema, key_value={k: v for k, v in key.items() if v is not None},
                fields_and_values=fields, mutation_type=kind, durability='durable')


def validate_receipt(receipt, operation):
    require(receipt.get('ok') is True and receipt.get('success') is True, 'mutation_failed')
    require(receipt.get('durability') == 'durable', 'mutation_not_durable')
    if operation == 'deleted':
        require(receipt.get('local_committed') is True, 'delete_not_locally_committed')
    require(receipt.get('operations', {}).get(operation) == 1
            and receipt['operations'].get('total') == 1, 'mutation_count_mismatch')
    require(isinstance(receipt.get('mutation_id'), str) and bool(receipt['mutation_id']),
            'mutation_identity_missing')


def fixture(kind):
    name = 'FastDelete' + kind
    fields = ['bucket', 'id', 'value']
    declaration = {'namespace': 'local_delete_proof', 'schema': {
        'name': name, 'descriptive_name': name, 'schema_type': kind,
        'key': {'hash_field': 'bucket', **({'range_field': 'id'} if kind == 'HashRange' else {})},
        'fields': fields, 'field_descriptions': {f: 'Delete proof ' + f for f in fields}}}
    records = []
    for role in ('deleted', 'retained'):
        key = {'hash': 'partition' if kind == 'HashRange' else role,
               'range': role if kind == 'HashRange' else None}
        records.append({'role': role, 'key': key, 'fields': {
            'bucket': key['hash'], 'id': role, 'value': kind + '-' + role + '-unique-payload'}})
    return declaration, records


def child_environment(home):
    return {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'LANG': 'C', 'LC_ALL': 'C',
            'HOME': str(home), 'TMPDIR': '/tmp', 'LASTDB_SUPPRESS_RECOVERY_PHRASE': '1',
            'FOLDDB_DISABLE_KEYCHAIN': '1', 'LASTDBD_STDIO_LOG_ROTATION': '0',
            'LASTDB_ENGINE': 'laststore'}


class CatalogHandler(BaseHTTPRequestHandler):
    """Serve only two synthetic schema definitions; no product data is mocked."""
    def do_GET(self):
        if self.path != '/v1/schemas/available':
            self.send_error(404)
            return
        body = json.dumps({'schemas': [dict(fixture(kind)[0]['schema'], system=False)
                                      for kind in ('Hash', 'HashRange')]}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class Proof:
    def __init__(self, args, skip_monitoring=False):
        self.args = args
        self.sample_interval = getattr(args, 'sample_interval', .5)
        self.started = time.monotonic()
        self.child = None
        self.home = None
        self.owner = str(uuid.uuid4())
        self.catalog = None
        self.output_owned = False
        self.done = threading.Event()
        self.fault = None
        self.rss_samples_collected = 0
        self.skip_monitoring = skip_monitoring
        self.result = {'schema': 'lastdb.local-delete-proof.v1', 'ok': False,
                       'counts': {'created': 0, 'deleted': 0, 'controls': 0},
                       'phases': [], 'processes': [], 'delete_receipts': [],
                       'cleanup': {'home_removed': False, 'all_children_exited': False},
                       'verifications': [], 'resources': {'peak_child_rss_bytes': 0, 'rss_samples_collected': 0},
                       'coverage': ['Hash', 'HashRange', 'both_tombstone_modes', 'two_cold_starts'],
                       'not_certified': ['atom_reclamation', 'disk_byte_return', 'cloud_recovery']}

    def save(self):
        target = self.args.output_dir / 'result.json'
        temporary = target.with_suffix('.tmp')
        temporary.write_text(json.dumps(self.result, sort_keys=True, indent=2) + '\n')
        temporary.replace(target)

    def check(self):
        require(not self.fault, self.fault)
        require(time.monotonic() - self.started < self.args.timeout_seconds, 'total_deadline')
        free = shutil.disk_usage(self.args.output_dir).free
        prior = self.result['resources'].get('min_free_disk_bytes', free)
        self.result['resources']['min_free_disk_bytes'] = min(prior, free)
        require(free >= self.args.min_free_bytes, 'disk_reserve')

    def monitor(self):
        while not self.done.wait(self.sample_interval):
            child = None
            try:
                self.check()
                child = self.child
                if child is not None and child.poll() is None:
                    raw = subprocess.check_output(['/bin/ps', '-o', 'ppid=,rss=', '-p', str(child.pid)],
                                                  timeout=3, stderr=subprocess.DEVNULL)
                    ppid, rss = map(int, raw.split())
                    require(ppid == os.getpid(), 'child_ownership')
                    self.result['resources']['peak_child_rss_bytes'] = max(
                        rss * 1024, self.result['resources']['peak_child_rss_bytes'])
                    self.rss_samples_collected += 1
                    require(rss * 1024 <= self.args.max_rss_bytes, 'child_rss_limit')
            except Exception as error:
                if child is not None and child.poll() is not None:
                    continue
                self.fault = str(error) if isinstance(error, ProofError) else 'resource_measurement_failed'
                if self.child is not None and self.child.poll() is None:
                    self.child.terminate()  # Unreaped direct Popen child; its PID cannot be reused.
                return

    def request(self, method, route, body=None, full=False):
        self.check()
        conn = http.client.HTTPConnection('localhost', timeout=10)
        conn.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        conn.sock.settimeout(10)
        try:
            conn.sock.connect(str(self.home / ('data/folddb-full.sock' if full else 'data/folddb.sock')))
            conn.request(method, route, None if body is None else json.dumps(body),
                         {'Content-Type': 'application/json', 'X-LastDB-Client': 'fast-delete-proof'})
            response = conn.getresponse()
            raw = response.read(2 * 1024 * 1024 + 1)
            if response.status != 200:
                self.result['failed_route'] = route
                self.result['http_status'] = response.status
                raise ProofError('http_' + str(response.status))
            require(len(raw) <= 2 * 1024 * 1024, 'response_limit')
            return json.loads(raw)
        except (ConnectionRefusedError, FileNotFoundError, OSError) as error:
            raise ProofError('cloud_enabled')
        finally:
            conn.close()

    def phase(self, name, function):
        start = time.monotonic()
        print(json.dumps({'phase': name, 'state': 'start'}), flush=True)
        function()
        self.result['phases'].append({'name': name, 'seconds': round(time.monotonic() - start, 3)})
        self.save()
        print(json.dumps({'phase': name, 'state': 'pass', **self.result['phases'][-1]}), flush=True)

    def boot(self):
        require(self.child is None, 'child_active')
        require(not (self.home / 'cloud_sync.json').exists(), 'cloud_config_present')
        environment = child_environment(self.home)
        environment['FOLD_SCHEMA_SERVICE_URL'] = self.catalog_url
        self.child = subprocess.Popen([str(self.args.lastdbd), '--data-dir', str(self.home)],
                                      env=environment, cwd=self.home,
                                      stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
        item = {'pid': self.child.pid, 'exit_code': None}
        self.result['processes'].append(item)
        deadline = time.monotonic() + 60
        while not (self.home / 'data/folddb.sock').exists():
            self.check()
            require(self.child.poll() is None, 'daemon_boot_exit')
            require(time.monotonic() < deadline, 'startup_deadline')
            time.sleep(.1)
        identity = self.request('GET', '/api/system/boot-identity')
        require(identity.get('pid') == self.child.pid, 'boot_pid_mismatch')
        item['boot_identity'] = identity
        self.local_status()

    def local_status(self):
        status = self.request('GET', '/api/status')
        require(status.get('ok') is True, 'status_not_ok')
        require(status['status']['sync']['enabled'] is False, 'cloud_enabled')
        require(status['status']['build']['version'] == self.result['binaries']['lastdbd']['version'],
                'daemon_version_changed')

    def stop(self):
        child = self.child
        if child is None:
            return
        if child.poll() is None:
            child.send_signal(signal.SIGTERM)
        try:
            code = child.wait(timeout=45)
        except subprocess.TimeoutExpired:
            child.kill()
            code = child.wait(timeout=10)
        self.result['processes'][-1]['exit_code'] = code
        self.child = None
        require(code == 0, 'daemon_unclean_exit')

    def seed(self):
        self.records = []
        for kind in ('Hash', 'HashRange'):
            declaration, records = fixture(kind)
            schema = declaration['schema']['name']
            declared = self.request('POST', '/api/schemas/load', {'schemas': [schema]}, full=True)
            require(declared.get('ok') is True and declared.get('schemas_loaded_to_db') == 1
                    and declared.get('failed_schemas') == [], 'load_fixture_schema_failed')
            require(isinstance(schema, str) and bool(schema), 'schema_identity_missing')
            for record in records:
                record.update(schema=schema, kind=kind)
                receipt = self.request('POST', '/api/mutation',
                                       mutation(schema, record['key'], record['fields'], 'create'))
                validate_receipt(receipt, 'created')
                record['expected'] = content(self.request('POST', '/api/query',
                    query_body(schema, record['key'], list(record['fields']), False)))
                require(record['expected']['key'] == record['key'], 'seed_key_mismatch')
                require(record['expected']['fields'] == record['fields'], 'seed_content_mismatch')
                self.records.append(record)
                self.result['counts']['created'] += 1
        self.result['counts']['controls'] = 2

    def delete(self):
        for record in self.records:
            if record['role'] != 'deleted':
                continue
            receipt = self.request('POST', '/api/mutation', mutation(record['schema'], record['key'], {}, 'delete'))
            validate_receipt(receipt, 'deleted')
            self.result['delete_receipts'].append({'kind': record['kind'], 'key': record['key'], 'receipt': receipt})
            self.result['counts']['deleted'] += 1
        self.verify()

    def verify(self):
        for record in self.records:
            for mode in (False, True):
                response = self.request('POST', '/api/query', query_body(
                    record['schema'], record['key'], list(record['fields']), mode))
                if record['role'] == 'deleted':
                    require(rows(response) == [], 'deleted_record_returned')
                else:
                    require(content(response) == record['expected'], 'control_changed')
        self.local_status()
        self.result['verifications'].append({'pid': self.child.pid, 'deleted_absent_both_modes': 2,
                                             'controls_match_both_modes': 2})

    def cleanup(self):
        stop_error = None
        try:
            self.stop()
        except ProofError as error:
            stop_error = error
        finally:
            self.done.set()
        if self.catalog is not None:
            self.catalog.shutdown()
            self.catalog.server_close()
        require(self.child is None, 'cleanup_child_active')
        self.result['cleanup']['all_children_exited'] = all(p['exit_code'] is not None for p in self.result['processes'])
        if self.home is not None:
            require(not self.home.is_symlink() and (self.home / '.proof-owner').read_text() == self.owner,
                    'cleanup_owner_mismatch')
            require(self.home.parent == self.args.output_dir, 'cleanup_path_mismatch')
            shutil.rmtree(self.home)
            self.result['cleanup']['home_removed'] = not self.home.exists()
        if stop_error is not None:
            raise stop_error

    def run(self):
        try:
            return self._run()
        except Exception as error:
            if not self.output_owned:
                raise
            self.result['error'] = str(error) if isinstance(error, ProofError) else type(error).__name__
            try:
                self.cleanup()
            except Exception as cleanup_error:
                self.result['cleanup_error'] = (str(cleanup_error) if isinstance(cleanup_error, ProofError)
                                                else type(cleanup_error).__name__)
            self.result['seconds'] = round(time.monotonic() - self.started, 3)
            self.save()
            return 1

    def _run(self):
        os.umask(0o077)
        require(not self.args.output_dir.exists(), 'output_dir_must_be_new')
        self.args.output_dir.mkdir(parents=True, mode=0o700)
        self.output_owned = True
        self.args.output_dir = self.args.output_dir.resolve()
        self.check()
        self.result['binaries'] = {}
        for name in ('lastdb', 'lastdbd'):
            path = getattr(self.args, name).resolve(strict=True)
            setattr(self.args, name, path)
            version = subprocess.check_output([str(path), '--version'], timeout=10,
                                             stderr=subprocess.DEVNULL).decode().strip()
            require(version.startswith(name + ' '), 'binary_version_format')
            self.result['binaries'][name] = {'path': str(path), 'sha256': digest(path),
                                            'version': version[len(name) + 1:]}
        require(self.result['binaries']['lastdb']['version'] == self.result['binaries']['lastdbd']['version'],
                'binary_pair_mismatch')
        self.home = Path(tempfile.mkdtemp(prefix='home-', dir=self.args.output_dir))
        (self.home / '.proof-owner').write_text(self.owner)
        require(len(str(self.home / 'data/folddb-full.sock').encode()) < 104, 'socket_path_too_long')
        self.result['home'] = str(self.home)
        self.catalog = HTTPServer(('127.0.0.1', 0), CatalogHandler)
        self.catalog_url = 'http://127.0.0.1:' + str(self.catalog.server_port)
        threading.Thread(target=self.catalog.serve_forever, kwargs={'poll_interval': .05}, daemon=True).start()
        self.result['schema_source'] = 'bundled_loopback_fixture_catalog'
        if not self.skip_monitoring:
            monitor = threading.Thread(target=self.monitor, daemon=True)
            monitor.start()
        try:
            self.phase('initial_boot', self.boot)
            self.phase('create_and_read', self.seed)
            self.phase('delete_and_read', self.delete)
            self.phase('initial_shutdown', self.stop)
            for number in (1, 2):
                self.phase('cold_boot_' + str(number), self.boot)
                self.phase('cold_verify_' + str(number), self.verify)
                self.phase('cold_shutdown_' + str(number), self.stop)
            require(len({p['pid'] for p in self.result['processes']}) == 3, 'distinct_processes_missing')
            for item in self.result['binaries'].values():
                require(digest(Path(item['path'])) == item['sha256'], 'binary_changed')
            self.check()
            self.result['resources']['rss_samples_collected'] = self.rss_samples_collected
            require(self.rss_samples_collected > 0, 'rss_samples_missing')
            self.result['ok'] = True
        except Exception as error:
            self.result['error'] = str(error) if isinstance(error, ProofError) else type(error).__name__
        finally:
            self.result['resources']['rss_samples_collected'] = self.rss_samples_collected
            try:
                self.phase('cleanup', self.cleanup)
            except Exception as error:
                self.result['ok'] = False
                self.result['cleanup_error'] = str(error) if isinstance(error, ProofError) else type(error).__name__
            self.result['seconds'] = round(time.monotonic() - self.started, 3)
            self.save()
        return 0 if self.result['ok'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lastdb', required=True, type=Path)
    parser.add_argument('--lastdbd', required=True, type=Path)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--timeout-seconds', type=int, default=180)
    parser.add_argument('--min-free-bytes', type=int, default=2 * 1024**3)
    parser.add_argument('--max-rss-bytes', type=int, default=2 * 1024**3)
    parser.add_argument('--sample-interval', type=float, default=.5, help='seconds between RSS samples')
    args = parser.parse_args()
    def interrupted(_signum, _frame):
        raise ProofError('interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        return Proof(args).run()
    except Exception as error:
        print(json.dumps({'ok': False, 'error': str(error) if isinstance(error, ProofError) else type(error).__name__}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
