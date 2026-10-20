#!/usr/bin/env python3
"""A/B warm set growth: normal cap vs a low LASTDB_RESIDENT_KEY_CAP on a fresh synthetic node.

The low cap must hold one read call: 256 keys x 4 molecules = 1,024 paths on
current main, so the default is 1,200 (the 2026-10-19 intern-keys run used 800
with 3 molecules per record).

Per run: fresh home, seed N records, flush, restart (empty warm set), sample,
read every record (all fields) twice, sample again. Growth = after - before for
the allocator in-use bytes (self-metrics JSONL) and the warm gauge (native-status).
Every read is checked against the generated values.
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

SCHEMA = 'WarmAbProbe'
FIELDS = ['title', 'body']


def ident(n):
    return f'record-{n:06d}'


def body(n, length):
    return ''.join(hashlib.sha256(f'WarmAb:{n}:{c}'.encode()).hexdigest()
                   for c in range((length + 63) // 64))[:length]


class Unix(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=300)
        self.unix = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.unix))


class Node:
    def __init__(self, binary, root, cap):
        self.binary, self.root, self.cap = binary, root, cap
        self.home = root / 'h'
        self.user = root / 'u'
        self.home.mkdir(parents=True)
        self.user.mkdir()
        self.sock, self.full = root / 'q.sock', root / 'f.sock'
        self.proc = None

    def call(self, path, value=None):
        conn = Unix(self.sock)
        try:
            raw = None if value is None else json.dumps(value)
            conn.request('GET' if value is None else 'POST', path, raw,
                         {'Content-Type': 'application/json', 'X-LastDB-Client': 'warm-ab'})
            resp = conn.getresponse()
            data = json.loads(resp.read())
            if resp.status != 200:
                raise RuntimeError(f'{path}: HTTP {resp.status} {str(data)[:400]}')
            return data
        finally:
            conn.close()

    def start(self):
        env = dict(os.environ, LASTDB_ISOLATED_COPY='1', ENVIRONMENT='dev',
                   LASTDB_SUPPRESS_RECOVERY_PHRASE='1', HOME=str(self.user),
                   LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                   LASTDB_SELF_METRICS_SAMPLE_SECS='5', RUST_MIN_STACK='16777216')
        for name in ['SENTRY_DSN', 'FOLD_SENTRY_DSN', 'RUST_LOG', 'LASTDB_RESIDENT_KEY_CAP']:
            env.pop(name, None)
        if self.cap:
            env['LASTDB_RESIDENT_KEY_CAP'] = str(self.cap)
        self.log = (self.root / 'node.log').open('a')
        self.proc = subprocess.Popen([str(self.binary), '--data-dir', str(self.home),
                                      '--socket-path', str(self.sock), '--full-socket-path', str(self.full)],
                                     env=env, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError('node exited before health')
            if self.sock.exists():
                try:
                    r = self.call('/health')
                    if r.get('status') == 'ok' or r.get('ok') is True:
                        return
                except (OSError, http.client.HTTPException, RuntimeError):
                    pass
            time.sleep(.1)
        raise RuntimeError('health deadline')

    def stop(self):
        if not self.proc:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=120)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None
        self.log.close()

    def metrics_file(self):
        found = [p for p in self.home.rglob('*.jsonl') if 'metric' in p.name]  # walk-ok: owned temp home, a few files
        return found[0] if found else None

    def sample(self, wait=13):
        """Wait for a fresh self-metrics line; return (malloc_in_use, gauge, paths)."""
        start = time.time()
        time.sleep(wait)
        line = None
        f = self.metrics_file()
        if f:
            for raw in f.read_text().splitlines():
                try:
                    row = json.loads(raw)
                except ValueError:
                    continue
                if 'malloc_bytes_in_use' in row and row.get('sampled_at', 0) >= int(start):
                    line = row
        status = self.call('/api/db/native-status')
        nd = find_key(status, 'core_warm_bytes')
        warm = find_key(status, 'core_warm') or {}
        return {'malloc': line and line.get('malloc_bytes_in_use'), 'gauge': nd and nd.get('used_bytes'),
                'gauge_parts': nd, 'warm_stats': warm_paths(status)}


def find_key(value, name):
    if isinstance(value, dict):
        if name in value:
            return value[name]
        for child in value.values():
            hit = find_key(child, name)
            if hit is not None:
                return hit
    elif isinstance(value, list):
        for child in value:
            hit = find_key(child, name)
            if hit is not None:
                return hit
    return None


def warm_paths(status):
    """The core warm counters from native-status (native_disk.core_warm_*)."""
    disk = find_key(status, 'native_disk') or {}
    return {k[len('core_warm_'):]: v for k, v in disk.items()
            if k.startswith('core_warm_') and isinstance(v, (int, float))}


def run(binary, records, size, cap, keep):
    root = Path(tempfile.mkdtemp(prefix='wab.', dir=os.environ.get('TMPDIR', '/tmp')))
    node = Node(binary, root, cap)
    expected = {n: {'title': f'title-{n}', 'body': body(n, size)} for n in range(records)}
    try:
        node.start()
        node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
            'source': 'system_seed', 'name': SCHEMA, 'descriptive_name': 'Warm AB Probe',
            'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
            'fields': ['bucket', 'id', 'title', 'body'],
            'field_descriptions': {'bucket': 'generated hash', 'id': 'generated range',
                                   'title': 'generated title', 'body': 'generated body'}}})
        for first in range(0, records, 64):
            muts = [{'type': 'mutation', 'schema': SCHEMA, 'mutation_type': 'create',
                     'key_value': {'hash': 'generated', 'range': ident(n)},
                     'fields_and_values': dict(expected[n], bucket='generated', id=ident(n)),
                     'durability': 'durable'} for n in range(first, min(first + 64, records))]
            r = node.call('/api/mutations/batch', {'mutations': muts})
            if r.get('count') != len(muts):
                raise RuntimeError('seed count')
        node.call('/api/db/flush', {})
        node.stop()
        node.start()
        before = node.sample()
        checked = 0
        for _ in range(2):
            for first in range(0, records, 256):
                nums = list(range(first, min(first + 256, records)))
                r = node.call('/api/query', {'schema_name': SCHEMA, 'fields': FIELDS,
                                             'filter': {'HashRangeKeys': [['generated', ident(n)] for n in nums]},
                                             'limit': len(nums)})
                got = {rec['key']['range']: {f: rec['fields'].get(f) for f in FIELDS} for rec in r['results']}
                want = {ident(n): expected[n] for n in nums}
                if got != want:
                    raise RuntimeError(f'value mismatch in batch {first}')
                checked += len(nums)
        after = node.sample()
        return {'cap': cap or 'normal', 'records': records, 'size': size, 'values_checked': checked,
                'before': before, 'after': after,
                'malloc_growth': after['malloc'] - before['malloc'],
                'gauge_growth': after['gauge'] - before['gauge']}
    finally:
        node.stop()
        if not keep:
            shutil.rmtree(root)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--binary', type=Path, required=True)
    ap.add_argument('--label', required=True)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--keep', action='store_true')
    ap.add_argument('--low-cap', type=int, default=1200)
    a = ap.parse_args()
    results = []
    for name, size in [('small', 200), ('medium', 8192)]:
        pair = {}
        for cap in [None, a.low_cap]:
            r = run(a.binary.resolve(), 3000, size, cap, a.keep)
            pair[r['cap']] = r
            print(json.dumps({k: r[k] for k in ['cap', 'size', 'malloc_growth', 'gauge_growth', 'values_checked']}
                             | {'paths': r['after']['warm_stats']}), flush=True)
        n, c = pair['normal'], pair[a.low_cap]
        extra = (n['after']['warm_stats'] or {}).get('paths', 0) - (c['after']['warm_stats'] or {}).get('paths', 0)
        dm, dg = n['malloc_growth'] - c['malloc_growth'], n['gauge_growth'] - c['gauge_growth']
        summary = {'label': a.label, 'shape': name, 'extra_paths': extra, 'alloc_diff': dm, 'gauge_diff': dg,
                   'alloc_per_path': dm / extra if extra else None, 'gauge_per_path': dg / extra if extra else None,
                   'ratio': dg / dm if dm else None}
        print('SUMMARY', json.dumps(summary), flush=True)
        results.append({'summary': summary, 'runs': pair})
    a.out.write_text(json.dumps(results, indent=1))


if __name__ == '__main__':
    sys.exit(main())
