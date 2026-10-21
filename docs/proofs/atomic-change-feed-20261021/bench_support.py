"""Buffered ordinary API receipts and exact benchmark input identities."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time
from types import SimpleNamespace

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
for directory in ('core-memory-state-20261018', 'core-owned-writes-20261018',
                  'core-clarity-20261018'):
    sys.path.insert(0, str(HERE.parent / directory))
from api import Node, measure
import default_node
import logical
import quiet_host


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def file_identity(path):
    path = path.resolve(strict=True)
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return {'path': str(path), 'sha256': digest.hexdigest(), 'bytes': path.stat().st_size}


def verified_source(path, binary):
    identity = file_identity(path)
    manifest = json.loads(Path(identity['path']).read_text())
    verification = manifest.get('final_verification', {})
    if (manifest.get('state') != 'release_verified'
            or manifest.get('binary_sha256') != binary['sha256']
            or manifest.get('build', {}).get('profile') != 'release'
            or verification.get('passed') is not True
            or verification.get('binary_sha256_matches') is not True
            or verification.get('binary_sha256') != binary['sha256']
            or verification.get('source_mismatches') != []):
        raise RuntimeError('The frozen source inventory lacks verified release identity for this binary')
    return dict(identity, state=manifest['state'], binary_sha256=manifest['binary_sha256'],
        source_inventory_sha256=manifest['source_inventory_sha256'],
        source_input_count=manifest['source_input_count'], head=manifest['head_at_capture'])


def certificate(value):
    raw = encoded(value)
    summary = {'parsed_json_sha256': hashlib.sha256(raw).hexdigest(), 'parsed_json_bytes': len(raw)}
    if isinstance(value, dict):
        summary['members'] = sorted(value)
        for name in ('count', 'returned_count', 'has_more', 'durability', 'revision',
                     'operations', 'resident_stages_us', 'mutation_ids', 'next_cursor', 'gap'):
            if name in value:
                summary[name] = value[name]
        for name in ('results', 'queries', 'mutations', 'changes'):
            if isinstance(value.get(name), list):
                summary[name + '_count'] = len(value[name])
    return summary


class BufferedRun(measure.Run):
    """Keep compact observations in memory until every timing run ends."""
    def remember(self, row):
        row = dict(row)
        for name in ('request', 'result'):
            if name in row:
                row[name] = certificate(row[name])
        with self.receipt_lock:
            self.rows.append(row)

    def finish(self, complete, error):
        self.outcome = {'complete': complete, 'error': error, 'finished_utc': measure.now()}

    def preserve(self):
        summary = dict(self.outcome, root=str(self.root), receipts=len(self.rows))
        for name in ('inputs.json', '.synthetic-seed-owner.json'):
            shutil.copy2(self.root / name, self.receipt_dir / name)
        with (self.receipt_dir / 'receipts.jsonl').open('x') as output:
            for row in self.rows:
                output.write(encoded(row).decode() + '\n')
        measure.profile.save(self.receipt_dir / 'summary.json', summary)
        stops = [row for row in self.rows if row.get('operation') == 'owned-node-stop']
        measure.profile.save(self.receipt_dir / 'retained-root.json', {
            'root': str(self.root), 'retained_for': 'supported cleanup after proof review',
            'all_owned_nodes_stopped': all(row.get('forced_kill') is False for row in stops),
            'owned_node_sessions': len(stops)})


def make_run(binary, source, output, label, samples):
    settings = SimpleNamespace(binary=binary, variant=label, root=None, receipt_dir=output / label,
        records_per_hash=samples * 161, hashes=2, body_bytes=1024, seed_batch=32,
        read_rounds=1, cap=10000, counts=[32], concurrent_width=1, extra_env={},
        reads_only=False, source_commit=source['sha256'])
    proof = BufferedRun(settings)
    proof.revision = {}
    proof.mutation_ids = set()
    proof.finish(False, 'The timing run did not start')
    path = proof.root / 'inputs.json'
    metadata = json.loads(path.read_text())
    metadata['unused_helper_argument_metadata'] = {key: metadata.pop(key) for key in (
        'schema', 'records_per_hash', 'hashes', 'range_recipe', 'range_bytes',
        'concurrent_queries', 'concurrent_keys_per_query', 'counts')}
    metadata.update(source_identity=source, records_per_schema=samples * 161,
        schemas={'AtomicNoticeHash': 'Hash', 'AtomicNoticeHashrange': 'HashRange'},
        mutation_count=samples * 161 * 4, fields=list(logical.FIELDS),
        record_recipe='logical.Spec values with deterministic 1 KiB bodies; distinct offsets per schema',
        receipt_capture='Compact parsed hashes, IDs, and original observations buffered until every timing run ends')
    measure.profile.save(path, metadata)
    proof.remember({'operation': 'source-identity', 'manifest': source})
    return proof, Node(proof)


class UncontrolledHost:
    """Explicit diagnostics never receive an accepted performance certificate."""
    def __init__(self):
        self.windows = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def set_owned_pids(self, _):
        return None

    @contextmanager
    def window(self, label, **_):
        value = {'label': label, 'accepted': False, 'uncontrolled': True,
                 'start_ns': time.monotonic_ns()}
        try:
            yield value
        finally:
            value['end_ns'] = time.monotonic_ns()
            self.windows.append(value)

    def evidence(self):
        return {'accepted_performance': False, 'uncontrolled': True, 'windows': self.windows}


class SharedHost(quiet_host.QuietHost):
    """Observe concurrent jobs without a quiet gate or a contention refusal."""
    @contextmanager
    def window(self, label, **_):
        value = {'label': label, 'accepted': False, 'scope': 'observed_shared_host',
                 'quiet_claim': False, 'start_ns': time.monotonic_ns(), 'started_utc': quiet_host.utc()}
        try:
            yield value
        finally:
            value.update(end_ns=time.monotonic_ns(), finished_utc=quiet_host.utc())
            deadline = time.monotonic() + 5
            with self.condition:
                while (not self.failure and self.io_process.poll() is None
                       and (not self.samples or self.samples[-1]['end_ns'] < value['end_ns'])
                       and time.monotonic() < deadline):
                    self.condition.wait(self.interval)
                selected = [index for index, row in enumerate(self.samples)
                            if row['end_ns'] >= value['start_ns'] and row['start_ns'] <= value['end_ns']]
                value.update(sample_indices=selected, observer_failure=self.failure,
                    contention_reasons=sorted({reason for index in selected
                                              for reason in self.samples[index]['reasons']}),
                    final_boundary_covered=bool(self.samples)
                        and self.samples[-1]['end_ns'] >= value['end_ns'])
            self.windows.append(value)

    def evidence(self):
        return dict(super().evidence(), scope='observed_shared_host', quiet_claim=False,
                    quiet_gate_enforced=False, contention_causes_rejection=False)


def imported_identities():
    modules = (sys.modules[__name__], sys.modules['api'], measure, logical, default_node,
               measure.profile, sys.modules['ownership'], quiet_host)
    paths = {Path(module.__file__).resolve() for module in modules}
    paths.update(HERE / name for name in ('benchmark.py', 'bench_cells.py', 'bench_notices.py'))
    return {str(path.relative_to(ROOT)): file_identity(path) for path in sorted(paths)}


def verify_files(identities):
    for expected in identities.values():
        if file_identity(Path(expected['path'])) != {name: expected[name] for name in ('path', 'sha256', 'bytes')}:
            raise RuntimeError('A frozen benchmark input changed')


def require_executable(path):
    identity = file_identity(path)
    if not os.access(identity['path'], os.X_OK):
        raise ValueError('Each benchmark binary must be executable')
    return identity
