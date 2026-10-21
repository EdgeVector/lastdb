#!/usr/bin/env python3
"""Create portable content certificates from explicit completed benchmark receipts."""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
PART_BYTES = 900 * 1024
CONTROL_KEYS = {'ok', 'error', 'count', 'returned_count', 'has_more', 'durability',
    'revision', 'convergence_pending', 'background_tasks_drained', 'status',
    'http_status', 'cost', 'costs', 'read_receipt', 'frontier', 'frontiers',
    'accepted_sequence', 'completed_sequence', 'resident_stages_us'}
SKIP_BRANCHES = {'fields', 'fields_and_values', 'metadata'}
HELPERS = ['durable-write-path-20261020/benchmark.py',
    'durable-write-path-20261020/focused.py',
    'durable-write-path-20261020/pack_receipts.py',
    'core-memory-state-20261018/api.py', 'schema-preload-product-20261017/measure.py',
    'graph-first-20261017/drivers/range_profile.py',
    'graph-first-20261017/drivers/ownership.py', 'core-clarity-20261018/default_node.py',
    'core-owned-writes-20261018/logical.py']


def compact(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('utf-8')


def identity(path):
    digest, size = hashlib.sha256(), 0
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
            size += len(block)
    return {'sha256': digest.hexdigest(), 'bytes': size}


def stamp(path):
    current = path.stat()
    return current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns


def control_fields(value, path=()):
    found = []
    if isinstance(value, dict):
        for name, child in value.items():
            at = (*path, name)
            if name in CONTROL_KEYS or name.endswith(('_frontier', '_sequence')):
                found.append({'path': at, 'value': child})
            elif name not in SKIP_BRANCHES:
                found.extend(control_fields(child, at))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(control_fields(child, (*path, index)))
    return found


def describe(value):
    summary = {'type': type(value).__name__}
    if isinstance(value, (dict, list, str)):
        summary['count'] = len(value)
    if isinstance(value, dict):
        summary['member_names'] = sorted(value)
        for name in ('mutations', 'queries', 'results', 'mutation_ids'):
            members = value.get(name)
            if not isinstance(members, list):
                continue
            item = {'count': len(members), 'json_sha256': hashlib.sha256(compact(members)).hexdigest()}
            if name == 'mutation_ids':
                item['first_ids'] = members[:8]
                item['last_ids'] = members[-8:] if len(members) > 8 else []
            elif name == 'mutations':
                item['mutation_types'] = dict(Counter(str(row.get('mutation_type'))
                    for row in members if isinstance(row, dict)))
                item['schemas'] = sorted({str(row.get('schema')) for row in members
                                          if isinstance(row, dict)})
            summary[name] = item
    controls = control_fields(value)
    if controls:
        summary['controls'] = controls
    return summary


def certificate(value):
    content = compact(value)
    return {'parsed_json_sha256': hashlib.sha256(content).hexdigest(),
            'parsed_json_bytes': len(content), 'summary': describe(value)}


class Parts:
    def __init__(self, directory):
        self.directory, self.handle = directory, None
        self.parts, self.total = [], hashlib.sha256()
        self.size, self.current_size = 0, 0

    def write(self, data):
        self.total.update(data)
        self.size += len(data)
        original = len(data)
        while data:
            if self.handle is None or self.current_size == PART_BYTES:
                if self.handle is not None:
                    self.handle.close()
                path = self.directory / f'receipts.jsonl.gz.part-{len(self.parts) + 1:03d}'
                self.handle, self.current_size = path.open('xb'), 0
                self.parts.append(path)
            width = min(len(data), PART_BYTES - self.current_size)
            self.handle.write(data[:width])
            self.current_size += width
            data = data[width:]
        return original

    def flush(self):
        if self.handle is not None:
            self.handle.flush()

    def close(self):
        if self.handle is not None:
            self.handle.close()

    def manifest(self):
        return {'sha256': self.total.hexdigest(), 'bytes': self.size,
            'part_max_bytes': PART_BYTES,
            'parts': [{'path': path.name, **identity(path)} for path in self.parts],
            'reassembly': 'Concatenate parts in listed order, verify archive SHA256, '
                          'then decompress the single gzip stream.'}


def portable_rows(raw, output):
    raw_hash, portable_hash = hashlib.sha256(), hashlib.sha256()
    raw_size, portable_size, count, operations = 0, 0, 0, Counter()
    sink = Parts(output)
    try:
        with raw.open('rb') as source, gzip.GzipFile(fileobj=sink, mode='wb',
                                                   filename='', mtime=0) as archive:
            for line in source:
                raw_hash.update(line)
                raw_size += len(line)
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError('Each receipt must be a JSON object')
                operations[str(row.get('operation'))] += 1
                for name in ('request', 'result'):
                    if name in row:
                        row[name] = certificate(row[name])
                encoded = compact(row) + b'\n'
                portable_hash.update(encoded)
                portable_size += len(encoded)
                archive.write(encoded)
                count += 1
    finally:
        sink.close()
    return {'rows': count, 'operation_counts': dict(operations),
        'raw': {'sha256': raw_hash.hexdigest(), 'bytes': raw_size},
        'portable_jsonl': {'sha256': portable_hash.hexdigest(), 'bytes': portable_size},
        'portable_archive': sink.manifest()}


def verify_original_gzip(path, raw_identity):
    digest, size = hashlib.sha256(), 0
    with gzip.open(path, 'rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
            size += len(block)
    if {'sha256': digest.hexdigest(), 'bytes': size} != raw_identity:
        raise ValueError('The retained original gzip does not reproduce the raw receipt file')


def pack(directory):
    raw, original_gzip = directory / 'receipts.jsonl', directory / 'receipts.jsonl.gz'
    result_path, input_path = directory / 'result.json', directory / 'inputs.json'
    originals = [raw, original_gzip, result_path, input_path]
    before = {path: stamp(path) for path in originals}
    result, inputs = json.loads(result_path.read_text()), json.loads(input_path.read_text())
    if not isinstance(result.get('complete'), bool):
        raise ValueError('A finished benchmark result must state complete as true or false')
    output = directory / 'portable'
    output.mkdir(exist_ok=False)
    manifest = portable_rows(raw, output)
    verify_original_gzip(original_gzip, manifest['raw'])
    manifest.update(format='lastdb-benchmark-parsed-receipts-v1',
        receipt_directory=str(directory.relative_to(HERE)), complete_result=result,
        source_inputs=inputs, result_identity=identity(result_path),
        inputs_identity=identity(input_path), original_gzip=identity(original_gzip),
        driver_helpers={name: identity(HERE.parent / name) for name in HELPERS},
        content_hash_contract='Sorted compact parsed JSON, ASCII escapes, UTF-8; '
                              'not original HTTP bytes or original JSON lexical bytes.',
        preserved_fields='All fields outside top-level request/result stay intact. '
                         'Certificates retain response controls, statuses, costs and frontiers. '
                         'Full local raw and original gzip files remain unchanged.')
    if any(stamp(path) != saved for path, saved in before.items()):
        raise RuntimeError('A source receipt changed during the portable capture')
    (output / 'manifest.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'directory': str(directory), 'rows': manifest['rows'],
        'workflow_complete': result['complete'],
        'portable_parts': len(manifest['portable_archive']['parts'])}), flush=True)


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directories', type=Path, nargs='+', required=True)
    directories = []
    for candidate in parser.parse_args().directories:
        actual = candidate.resolve(strict=True)
        try:
            relative = actual.relative_to(HERE)
        except ValueError:
            parser.error('Receipt directories must be inside this proof directory')
        if not actual.is_dir() or not 1 <= len(relative.parts) <= 2:
            parser.error('Receipt directories must be one or two levels below this proof directory')
        if actual in directories:
            parser.error('Each receipt directory must occur once')
        directories.append(actual)
    return directories


if __name__ == '__main__':
    for receipt_directory in arguments():
        pack(receipt_directory)
