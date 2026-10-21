#!/usr/bin/env python3
"""Shared helpers for the incremental-compaction run proofs.

These helpers start a real `lastdbd` on a fresh synthetic home, write through the
HTTP routes of the node, and read the files of a stopped home. They never touch a
default home: every path goes through `refuse_primary`, and every home is a fresh `mktemp` directory.

Set `LDBIN` to the directory that holds the built `lastdb`, `lastdbd`,
`lastdb-pack-maintain`, `lastdb-disk-home` and `lastdb-journal`.
"""
import atexit
import collections
import http.client
import json
import os
import shutil
import signal
import socket
import struct
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

BIN = Path(os.environ.get('LDBIN') or '.')  # scripts that start a node or a tool need LDBIN
LASTDBD = BIN / 'lastdbd'
LASTDB = BIN / 'lastdb'
PACK_MAINTAIN = BIN / 'lastdb-pack-maintain'

_HOME = Path(os.path.realpath(Path.home()))
_FORBIDDEN_PREFIXES = ('.lastdb', '.folddb', '.primary')


def refuse_primary(path):
    """Fail when `path` is inside a default home of the product or a directory next to it.

    A default home sits directly under the user home in a dot directory, so every dot directory there
    whose name starts with `.lastdb`, `.folddb` or `.primary` (a home, a compatibility link, an archive)
    is refused."""
    # Check the spelled path first, so that a refused path is never even looked up on disk.
    for candidate in (Path(os.path.abspath(path)), None):
        if candidate is None:
            candidate = Path(os.path.realpath(path))
        for base in (Path.home(), _HOME):
            if base in candidate.parents and candidate.relative_to(base).parts[0].startswith(_FORBIDDEN_PREFIXES):
                raise SystemExit(f'refusing a path inside a default home: {path}')
    return Path(os.path.realpath(path))


def remove_owned(path):
    """Delete one directory tree that this proof created: a `s1s2.*` entry directly under $TMPDIR.

    Refuses every other path, so a wrong argument cannot delete anything else."""
    real = Path(os.path.realpath(path))
    tmp = Path(os.path.realpath(os.environ.get('TMPDIR') or '/tmp'))
    if real.parent != tmp or not real.name.startswith('s1s2.'):
        raise SystemExit(f'refusing to delete a path that is not mine: {path}')
    refuse_primary(real)
    if real.is_dir():
        shutil.rmtree(real)


class UDS(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost')
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(900)
        self.sock.connect(self.path)


def new_home(tag):
    """A fresh synthetic home under $TMPDIR (never the primary)."""
    path = tempfile.mkdtemp(prefix=f's1s2.{tag}.', dir=os.environ.get('TMPDIR') or '/tmp')
    return refuse_primary(path)


class Node:
    """One `lastdbd` child process on a synthetic home, with EXEMEM_ENV=dev."""

    def __init__(self, home, extra_env=None):
        self.home = refuse_primary(home)
        self.sock = self.home / 'data' / 'folddb.sock'
        self.full = self.home / 'data' / 'folddb-full.sock'
        self.extra_env = extra_env or {}
        self.proc = None
        self.log = None

    def env(self):
        env = dict(os.environ)
        for key in ('LASTDB_DISK_PACK', 'LASTDB_ENGINE', 'LASTDB_DISK_CAP_BYTES', 'SENTRY_DSN',
                    'FOLD_SENTRY_DSN', 'RUST_LOG', 'LASTDB_RESIDENT_MODE'):
            env.pop(key, None)
        env.update(LASTDB_ISOLATED_COPY='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
                   HOME=str(self.home / 'home'), LASTDB_HOME=str(self.home),
                   FOLDDB_HOME=str(self.home), EXEMEM_ENV='dev', RUST_MIN_STACK='67108864')
        env.update(self.extra_env)
        return env

    def start(self, timeout=900):
        (self.home / 'home').mkdir(exist_ok=True)
        self.log = open(self.home / 'node.log', 'a')
        atexit.register(self.stop)
        self.proc = subprocess.Popen(
            [str(LASTDBD), '--data-dir', str(self.home)], env=self.env(),
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError('node exited; see node.log in the home')
            if self.full.exists() or self.sock.exists():
                try:
                    self.call('/health')
                    break
                except Exception:
                    pass
            time.sleep(0.1)
        else:
            raise RuntimeError('node health timeout')
        status = self.cli('status')
        if str(self.home) not in status:
            self.kill()
            raise RuntimeError('`lastdb status` does not name my temp home: ' + status[:300])

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(300)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        if self.log and not self.log.closed:
            self.log.close()

    def kill(self):
        """SIGKILL, only my own child process."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        if self.log and not self.log.closed:
            self.log.close()

    def call(self, path, value=None, headers=None, raw=False):
        sock = self.full if self.full.exists() else self.sock
        conn = UDS(sock)
        head = {'Content-Type': 'application/json', 'X-LastDB-Client': 's1s2-proof'}
        head.update(headers or {})
        began = time.perf_counter()
        conn.request('GET' if value is None else 'POST', path,
                     None if value is None else json.dumps(value, separators=(',', ':')), head)
        response = conn.getresponse()
        body = response.read()
        conn.close()
        elapsed = time.perf_counter() - began
        if raw:
            return response.status, body, elapsed
        try:
            decoded = json.loads(body)
        except Exception:
            decoded = body.decode('utf-8', 'replace')
        if response.status != 200:
            raise RuntimeError(f'{path}: HTTP {response.status}: {str(decoded)[:500]}')
        return decoded, elapsed

    def cli(self, *args, check=True, timeout=900, stdin=None):
        done = subprocess.run([str(LASTDB), '--data-dir', str(self.home), *args], env=self.env(),
                              capture_output=True, text=True, timeout=timeout, input=stdin)
        if check and done.returncode != 0:
            raise RuntimeError(f'lastdb {args}: {done.returncode}: {done.stderr[-800:]}')
        return done.stdout


def seed(node, name, fields=('bucket', 'id', 'f')):
    node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
        'source': 'system_seed', 'name': name, 'descriptive_name': name,
        'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
        'fields': list(fields), 'field_descriptions': {f: name + ' independent ' + f for f in fields}}})


def put(schema, bucket, rid, value, durable=True, typ='update'):
    request = {'type': 'mutation', 'schema': schema, 'mutation_type': typ,
               'key_value': {'hash': bucket, 'range': rid}}
    if durable:
        request['durability'] = 'durable'
    request['fields_and_values'] = ({'bucket': bucket, 'id': rid, 'f': value}
                                    if typ == 'update' else {})
    return request


def mutate(node, muts):
    return node.call('/api/mutations/batch', {'mutations': muts})


def read_keys(node, schema, keys):
    """Read value field `f` of the given (bucket, id) pairs; returns {(bucket, id): value}."""
    out = {}
    for i in range(0, len(keys), 100):
        part = keys[i:i + 100]
        result, _ = node.call('/api/query', {'schema_name': schema, 'fields': ['f'],
                                             'filter': {'HashRangeKeys': [list(k) for k in part]},
                                             'limit': 1000})
        for row in result['results']:
            out[(row['key']['hash'], row['key']['range'])] = row['fields'].get('f')
    return out


# ---- disk readers for a STOPPED synthetic home ----

def pack_root(home):
    return Path(home) / 'data' / 'disk-pack'


def current_gen(home):
    data = (pack_root(home) / 'CURRENT').read_bytes()
    return struct.unpack_from('<Q', data, 16)[0], struct.unpack_from('<Q', data, 32)[0]


PAGE = 16384
KIND = {1: 'directory', 2: 'locator', 3: 'map', 4: 'journal-data', 5: 'pack-data'}


def receipt(home, gen=None):
    """Decode the receipt of generation `gen` (default CURRENT).

    Returns (generation, count, Counter by kind, entries) where an entry is
    (kind, object uuid, batch, part, length, creation generation)."""
    root = pack_root(home)
    if gen is None:
        gen = current_gen(home)[0]
    base = root / 'generations' / str(gen)
    entries, page_no, count = [], 0, None
    while True:
        part, off = divmod(page_no, 512)
        with open(base / ('part-%08x' % part), 'rb') as handle:
            handle.seek(off * PAGE)
            page = handle.read(PAGE)
        payload = page[128:]
        if page_no == 0:
            count = struct.unpack_from('<Q', payload, 16)[0]
        first = 4096 if page_no == 0 else 0
        n = struct.unpack_from('<I', payload, 32)[0]
        data = payload[128 + first:128 + first + n * 80]
        for i in range(n):
            e = data[i * 80:(i + 1) * 80]
            entries.append((KIND.get(e[0], e[0]), uuid.UUID(bytes=e[8:24]),
                            struct.unpack_from('<Q', e, 24)[0], struct.unpack_from('<I', e, 4)[0],
                            struct.unpack_from('<Q', e, 32)[0], struct.unpack_from('<Q', e, 72)[0]))
        page_no += 1
        if len(entries) >= count:
            break
    return gen, count, collections.Counter(e[0] for e in entries), entries


def receipt_summary(home):
    gen, count, kinds, entries = receipt(home)
    locator_objects = collections.Counter(e[1] for e in entries if e[0] == 'locator')
    return {'generation': gen, 'parts': count, 'by_kind': dict(kinds),
            'locator_objects': len(locator_objects),
            'locator_parts_per_object_max': max(locator_objects.values(), default=0)}


def pct(values, q):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]
