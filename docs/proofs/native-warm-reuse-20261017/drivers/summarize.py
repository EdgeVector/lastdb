#!/usr/bin/env python3
"""Preserve fixed completed benchmark receipts and summarize their query costs."""
import argparse
from collections import Counter
import datetime
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import statistics

CASES = {
    'c': 'Same-binary repeat control; part of this run overlaps the release build.',
    'w': 'Actual before/after read reuse after ordinary writes.',
    'o': 'Actual before/after one-key range before and after a broad exact read.',
}
MAX_DIRECTORY_ENTRIES = 4096
MAX_FILES = 2048
MAX_SOURCE_BYTES = 512 * 1024 * 1024
MAX_BLOB_BYTES = 2 * 1024 * 1024
SECRET_PATTERNS = {
    'private-key': re.compile(rb'-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----'),
    'cloud-key': re.compile(rb'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b'),
    'bearer': re.compile(rb'(?i)authorization\s*[=:]\s*["\x27]?bearer\s+[A-Za-z0-9._~+/=-]{12,}'),
    'secret-field': re.compile(rb'(?i)["\x27](?:private_key|secret_access_key|access_token|refresh_token|password|secret_key|recovery_phrase|mnemonic)["\x27]\s*:\s*["\x27][^"\x27\r\n]{8,}'),
    'provider-token': re.compile(rb'\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{30,}|xox[baprs]-[A-Za-z0-9-]{20,}|sk-proj-[A-Za-z0-9_-]{20,})\b'),
    'recovery-words': re.compile(rb'(?i)(?:recovery phrase|mnemonic)\s*[:=]\s*[a-z]{3,}(?: [a-z]{3,}){4,}'),
}


def load(path):
    return json.loads(path.read_text())


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def distribution(values):
    ordered = sorted(values)
    return {'samples': len(ordered), 'median': statistics.median(ordered),
            'p95': ordered[math.ceil(.95 * len(ordered)) - 1] if len(ordered) >= 5 else None,
            'minimum': ordered[0], 'maximum': ordered[-1]}


def numeric_medians(rows):
    names = set.intersection(*(set(row) for row in rows))
    return {name: statistics.median(row[name] for row in rows) for name in sorted(names)
            if all(isinstance(row[name], (int, float)) and not isinstance(row[name], bool)
                   for row in rows)}


def query_costs(rows):
    hashes = {row['response_sha256'] for row in rows}
    costs = [row['cost'] for row in rows]
    if len(hashes) != 1 or not all(row.get('expected_values_match') is True for row in rows):
        raise ValueError('A query group lacks stable generated responses')
    return {'wall_ms': distribution([row['elapsed_nanos'] / 1e6 for row in rows]),
            'query_ms': distribution([row['cost']['query_storage_elapsed_nanos'] / 1e6
                                      for row in rows]),
            'cost_medians': numeric_medians(costs),
            'file_opens': distribution([cost['file_opens'] for cost in costs]),
            'logical_warm_hits': distribution([cost['logical_warm_hits'] for cost in costs]),
            'all_file_opens_zero': all(cost['file_opens'] == 0 for cost in costs),
            'all_file_reads_zero': all(cost['read_operations'] == 0 for cost in costs),
            'full_response_hash_stable': len(hashes) == 1,
            'response_sha256': sorted(hashes),
            'content_sha256': sorted({row['content_sha256'] for row in rows
                                      if 'content_sha256' in row}),
            'metadata_sha256': sorted({row['metadata_sha256'] for row in rows
                                       if 'metadata_sha256' in row}),
            'returned_records': distribution([row['returned_records'] for row in rows])}


def groups(rows):
    grouped = {}
    for row in rows:
        if row['operation'] == 'query':
            key = tuple(row[name] for name in ('variant', 'phase', 'shape', 'batch'))
            grouped.setdefault(key, []).append(row)
    result = []
    for key, selected in sorted(grouped.items()):
        group = dict(zip(('variant', 'phase', 'shape', 'batch'), key))
        group.update(query_costs(selected))
        group['is_separate_prime'] = key[1] == 'prime' or key[1].endswith('-prime')
        result.append(group)
    return result


def first_calls(rows):
    selected = []
    for row in rows:
        if row['operation'] != 'query':
            continue
        phase = row['phase']
        if phase == 'prime' or phase.endswith('-prime'):
            selected.append(row)
        elif phase in ('before-broad-read', 'after-broad-read') and row['repeat'] == 0:
            selected.append(row)
    return groups(selected)


def ratio(before, after):
    return {'before': before, 'after': after, 'delta': after - before,
            'after_over_before': after / before if before else None,
            'before_over_after': before / after if after else None,
            'percent_change': 100 * (after - before) / before if before else None}


def compare(left, right):
    return {'wall_ms': ratio(left['wall_ms']['median'], right['wall_ms']['median']),
            'query_ms': ratio(left['query_ms']['median'], right['query_ms']['median']),
            'file_opens': ratio(left['file_opens']['median'], right['file_opens']['median']),
            'logical_warm_hits': ratio(left['logical_warm_hits']['median'],
                                       right['logical_warm_hits']['median']),
            'before_samples': left['wall_ms']['samples'], 'after_samples': right['wall_ms']['samples']}


def program_comparisons(query_groups):
    by_key = {(row['variant'], row['phase'], row['shape'], row['batch']): row for row in query_groups}
    result = []
    for variant, phase, shape, batch in sorted(by_key):
        other = ('after', phase, shape, batch)
        if variant == 'before' and other in by_key:
            result.append({'phase': phase, 'shape': shape, 'batch': batch,
                           'is_first_call': by_key[(variant, phase, shape, batch)]['is_separate_prime'],
                           **compare(by_key[(variant, phase, shape, batch)], by_key[other])})
    return result


def order_comparisons(query_groups):
    by_key = {(row['variant'], row['phase'], row['shape'], row['batch']): row for row in query_groups}
    result = []
    for variant in ('before', 'after'):
        left = by_key.get((variant, 'before-broad-read', 'range', 1))
        right = by_key.get((variant, 'after-broad-read', 'range', 1))
        if left and right:
            result.append({'variant': variant, 'shape': 'range', 'batch': 1,
                           'left_phase': 'before-broad-read', 'right_phase': 'after-broad-read',
                           'meaning': 'Broad-read order effect within the same owned node.',
                           **compare(left, right)})
    return result


def case_summary(runtime, name):
    directory = runtime / name
    for relative in ('inputs.json', 'completion.json', 'receipts.jsonl', 'ownership.json',
                     'summary.json', 'completed.json'):
        info = (directory / relative).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise ValueError(f'Case {name} lacks an owned regular receipt: {relative}')
    inputs, completion = load(directory / 'inputs.json'), load(directory / 'completion.json')
    rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
    recorded = completion.get('receipt_sha256', {})
    for relative in ('inputs.json', 'summary.json', 'receipts.jsonl', 'ownership.json'):
        if recorded.get(relative) != sha256((directory / relative).read_bytes()):
            raise ValueError(f'Case {name} completion hash differs for {relative}')
    ownership = load(directory / 'ownership.json')
    if ownership.get('uid') != os.getuid() or ownership.get('origin') != 'generated-empty':
        raise ValueError(f'Case {name} lacks the current generated-data owner')
    starts = Counter(row['pid'] for row in rows if row['operation'] == 'owned-node-start')
    stops = [row for row in rows if row['operation'] == 'owned-node-stop']
    if starts != Counter(row['pid'] for row in stops):
        raise ValueError(f'Case {name} lacks a stop receipt for every owned start')
    same_binary = inputs['binaries']['before']['sha256'] == inputs['binaries']['after']['sha256']
    if (name == 'c') != same_binary:
        raise ValueError(f'Case {name} binary hashes conflict with its declared purpose')
    complete = completion.get('success') is True and (directory / 'completed.json').is_file()
    healthy = len(stops) == 6 and all(not row['forced_kill'] and row['exit_code'] == 0 for row in stops)
    refusals = sum(row['operation'] == 'refusal' for row in rows)
    if not complete or completion.get('errors') or not healthy or refusals:
        raise ValueError(f'Case {name} lacks complete success, six normal stops, or refusal-free requests')
    query_count = sum(row['operation'] == 'query' for row in rows)
    rounds = inputs['read_rounds']
    expected_queries = 80 * rounds + 144 if name == 'w' else 4 * rounds + 10
    if query_count != expected_queries or completion.get('query_count') != query_count:
        raise ValueError(f'Case {name} query receipts differ from its completed workload count')
    if load(directory / 'completed.json').get('expected_values_match') is not True:
        raise ValueError(f'Case {name} lacks completed generated-value confirmation')
    query_groups = groups(rows)
    result = {'name': name, 'purpose': CASES[name], 'complete': complete, 'all_six_stops_normal': healthy,
              'errors': completion.get('errors', []), 'same_binary': same_binary,
              'refusal_count': refusals, 'owned_node_starts': sum(starts.values()),
              'owned_node_stops': len(stops), 'expected_query_count': expected_queries,
              'binaries': inputs['binaries'], 'read_rounds': inputs['read_rounds'],
              'query_groups': query_groups, 'first_calls': first_calls(rows),
              'query_count': query_count,
              'root_has_no_database_file_reads': True}
    if name == 'c':
        result['interpretation'] = 'Same-binary control with release-build overlap; no before/after performance conclusion.'
        result['actual_before_after_comparisons'] = []
    elif complete and healthy:
        result['actual_before_after_comparisons'] = program_comparisons(query_groups)
    else:
        result['actual_before_after_comparisons'] = []
    if name == 'o':
        result['before_after_broad_by_variant'] = order_comparisons(query_groups)
        without_first = [row for row in rows if not (row['operation'] == 'query'
                         and row['phase'] in ('before-broad-read', 'after-broad-read') and row['repeat'] == 0)]
        result['broad_order_repeat_only_groups'] = groups(without_first)
        result['first_order_call_also_counts_in_main_group'] = True
    return result


def regular_files(directory, root, node=False):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError(f'Receipt directory is absent or a symlink: {directory.relative_to(root)}')
    entries = sorted(directory.iterdir())
    if len(entries) > MAX_DIRECTORY_ENTRIES:
        raise ValueError('Receipt directory exceeds its explicit entry cap')
    result = []
    for path in entries:
        allowed = path.suffix == '.json' if node else path.suffix in ('.json', '.jsonl')
        if node and path.name == 'node.log':
            allowed = True
        info = path.lstat()
        if allowed and stat.S_ISREG(info.st_mode):
            if info.st_uid != os.getuid():
                raise ValueError(f'Receipt has a different owner: {path.relative_to(root)}')
            result.append(path)
    return result


def receipt_paths(runtime):
    # Only these ten directories are enumerated. No data/home directory is entered.
    result = regular_files(runtime, runtime)
    for name in CASES:
        directory = runtime / name
        result.extend(regular_files(directory, runtime))
        for variant in ('b', 'a'):
            result.extend(regular_files(directory / variant, runtime, node=True))
    if len(result) > MAX_FILES or sum(path.stat().st_size for path in result) > MAX_SOURCE_BYTES:
        raise ValueError('Selected receipts exceed their bounded file/byte cap')
    return result


def preserve(runtime, output, paths):
    manifest = []
    for source in paths:
        relative = source.relative_to(runtime)
        data = source.read_bytes()
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(data):
                raise ValueError(f'Potential secret pattern {label} in receipt {relative}; value omitted')
        compressed = len(data) > 1024 * 1024 or source.name == 'node.log'
        stored = Path('raw') / relative
        if compressed:
            stored = stored.with_name(stored.name + '.gz')
        encoded = gzip.compress(data, mtime=0) if compressed else data
        if len(encoded) > MAX_BLOB_BYTES:
            raise ValueError(f'Preserved receipt exceeds the repo blob cap: {relative}')
        target = output / stored
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_symlink() or any(parent.is_symlink() for parent in target.parents if parent != output.parent):
            raise ValueError('Preservation target includes a symlink')
        if target.exists() and target.read_bytes() != encoded:
            raise ValueError(f'Existing preserved receipt differs: {stored}')
        target.write_bytes(encoded)
        manifest.append({'source': str(relative), 'stored': str(stored),
                         'compression': 'gzip' if compressed else None,
                         'source_bytes': len(data), 'source_sha256': sha256(data),
                         'stored_bytes': len(encoded), 'stored_sha256': sha256(encoded)})
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    runtime = args.runtime_root.resolve(strict=True)
    output = args.output.resolve()
    if output == runtime or runtime in output.parents:
        parser.error('Preserve receipts outside the runtime root')
    paths = receipt_paths(runtime)
    cases = [case_summary(runtime, name) for name in CASES]
    output.mkdir(parents=True, exist_ok=True)
    manifest = preserve(runtime, output, paths)
    write_json(output / 'receipt-manifest.json', {'root': str(runtime), 'database_files_included': False,
               'selection': 'Fixed regular JSON/JSONL receipts and direct b/a JSON/node.log only.',
               'files': manifest})
    write_json(output / 'summary.json', {
        'version': 1, 'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'driver_sha256': sha256(Path(__file__).read_bytes()), 'runtime_root': str(runtime),
        'all_cases_complete': all(case['complete'] for case in cases),
        'all_owned_stops_normal': all(case['all_six_stops_normal'] for case in cases),
        'preserved_files': len(manifest),
        'preserved_source_bytes': sum(row['source_bytes'] for row in manifest),
        'preserved_stored_bytes': sum(row['stored_bytes'] for row in manifest),
        'notes': ['No database directories, sockets, binaries, or user homes are read.',
                  'Case c uses the same binary and overlaps part of a release build.',
                  'Case c is a repeat control, never an actual before/after result.',
                  'Separate prime receipts describe first post-write calls.',
                  'The warm-order driver includes its first call in its main measured phase.',
                  'File-work counters do not establish device traffic or cold file-cache state.',
                  'A zero warm-hit counter does not prove an atom-content miss; its observer scope is incomplete.',
                  'The p95 uses nearest rank and needs at least five samples.',
                  'Query timers are wall intervals; they do not measure thread CPU time.'],
        'cases': cases})
    print(str(output / 'summary.json'))


if __name__ == '__main__':
    main()
