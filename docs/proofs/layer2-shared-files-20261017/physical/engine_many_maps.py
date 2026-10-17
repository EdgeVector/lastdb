#!/usr/bin/env python3
"""Many-map extension of the native-home product-command benchmark recipe."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import json
import math
import os
import platform
import re
from pathlib import Path
import statistics
import subprocess
import time

PARSER = argparse.ArgumentParser(description=__doc__)
PARSER.add_argument('--baseline', required=True, type=Path)
PARSER.add_argument('--candidate', required=True, type=Path)
PARSER.add_argument('--output', required=True, type=Path)
PARSER.add_argument('--read-rounds', type=int, default=31)
PARSER.add_argument('--write-rounds', type=int, default=31)
PARSER.add_argument('--body-bytes', type=int, default=1024)
PARSER.add_argument('--records', type=int, default=4096)
PARSER.add_argument('--only', choices=['reads', 'writes', 'all'], default='all')
PARSER.add_argument('--batches', nargs='+', type=int, choices=[1,16,64,256], default=[1,16,64,256])
PARSER.add_argument('--write-patterns', nargs='+', choices=['distinct','repeated'], default=['distinct','repeated'])
ARGS = PARSER.parse_args()
if min(ARGS.read_rounds, ARGS.write_rounds) < 1 or not 128 <= ARGS.body_bytes <= 1024 * 1024:
    PARSER.error('positive repeat counts and 128..1048576 body bytes required')
if not 4096 <= ARGS.records <= 10000:
    PARSER.error('records must be 4096..10000')
ROOT = ARGS.output.resolve()
ROOT.mkdir(parents=True, exist_ok=False)
BINARIES = {'baseline': ARGS.baseline.resolve(strict=True), 'candidate': ARGS.candidate.resolve(strict=True)}
COLLECTION = 'Layer2Map'

def collection(number):
    return f'{COLLECTION}{number // 16:03d}'
PREFIX = 'generated\0'
RECEIPTS = []
VALUES = {}


def key(number):
    return PREFIX + f'record-{number:06d}' + ('-' + 'x' * 193 if number % 64 == 0 else '')


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n')


def digest(value):
    return hashlib.sha256(json.dumps(value, separators=(',', ':')).encode()).hexdigest()


def command(variant, operation, home, input_path=None):
    args = [str(BINARIES[variant]), operation, '--home', str(home)]
    if input_path is not None:
        args += ['--input', str(input_path)]
    if operation == 'build':
        args += ['--fanout', '1']
    environment = dict(os.environ)
    environment.pop('LASTDB_DISK_CAP_BYTES', None)
    started = time.perf_counter_ns()
    process = subprocess.run(['/usr/bin/time', '-l'] + args, capture_output=True, text=True,
                             timeout=90, env=environment)
    elapsed = time.perf_counter_ns() - started
    if process.returncode:
        raise RuntimeError(f'{variant} {operation}: {process.stderr[:3000]}')
    result = json.loads(process.stdout)
    result['process_elapsed_nanos'] = elapsed
    memory = re.search(r'^\s*(\d+)\s+maximum resident set size\s*$', process.stderr, re.M)
    footprint = re.search(r'^\s*(\d+)\s+peak memory footprint\s*$', process.stderr, re.M)
    if memory is None:
        raise RuntimeError('macOS product-process memory receipt is absent')
    result['process_maximum_resident_bytes'] = int(memory.group(1))
    if footprint is not None:
        result['process_peak_memory_bytes'] = int(footprint.group(1))
    return result


def remember(result):
    RECEIPTS.append(result)
    with (ROOT / 'receipts.jsonl').open('a') as output:
        output.write(json.dumps(result, sort_keys=True, separators=(',', ':')) + '\n')


def metadata(home):
    parts = {}
    for kind in ['locator', 'directory']:
        directory = home / kind
        for object_path in directory.iterdir():
            for part in object_path.glob('part-*'):
                parts[str(part.relative_to(home))] = part.stat().st_size
    return parts


def home_size(home):
    # Bound the exact generated native-home inventory to six levels and a finite file count.
    stack = [(home, 0)]
    files = logical = allocated = 0
    while stack:
        parent, depth = stack.pop()
        if depth > 6:
            raise RuntimeError('Generated native home exceeds layout depth six')
        for path in parent.iterdir():
            if path.is_symlink():
                raise RuntimeError('Generated native home inventory refuses a symlink')
            if path.is_dir():
                stack.append((path, depth + 1))
            elif path.is_file():
                info = path.stat()
                files += 1
                logical += info.st_size
                allocated += info.st_blocks * 512
                if files > 262144:
                    raise RuntimeError('Generated native home exceeds the finite file budget')
    return {'files':files, 'logical_file_bytes':logical, 'allocated_file_bytes':allocated}


def check_read(result, numbers, expected):
    bodies = [entry.get('body_hex') for entry in result['results']]
    errors = [entry for entry in result['results'] if 'error' in entry]
    wanted = [expected.get(key(number)) for number in numbers]
    if errors or bodies != wanted:
        raise RuntimeError(f'Product result mismatch: {errors[:2]}')
    result['result_sha256'] = digest(bodies)
    result['result_count'] = len(bodies)
    result['absence_count'] = bodies.count(None)
    result['full_bodies_match_generated_input'] = True
    del result['results']


def build_pair(label):
    homes = {}
    for variant in BINARIES:
        home = ROOT / f'{label}-{variant}'
        result = command(variant, 'build', home, ROOT / 'initial.jsonl')
        result.update(variant=variant, case=label, operation='build')
        result['disk_size'] = home_size(home)
        remember(result)
        homes[variant] = home
    return homes


def read_measurements():
    homes = build_pair('reads')
    for pattern in ['contiguous', 'scattered', 'duplicates']:
        for count in ARGS.batches:
            numbers = {'contiguous': [1024 + n for n in range(count)],
                       'scattered': [(73 + n * 37) % 4096 for n in range(count)],
                       'duplicates': [[1111, 1112, 1111, 999999][n % 4] for n in range(count)]}[pattern]
            request = ROOT / 'read-request.json'
            save(request, {'coordinates': [{'collection': collection(n), 'key': key(n)} for n in numbers]})
            for repeat in range(ARGS.read_rounds):
                order = list(BINARIES) if repeat % 2 == 0 else list(reversed(BINARIES))
                for variant in order:
                    result = command(variant, 'read', homes[variant], request)
                    check_read(result, numbers, VALUES)
                    result.update(variant=variant, operation='read', pattern=pattern, batch=count, repeat=repeat)
                    remember(result)
            print(f'read {pattern} {count} complete', flush=True)


def write_measurements():
    for pattern in ARGS.write_patterns:
        for count in ARGS.batches:
            homes = build_pair(f'writes-{pattern}-{count}')
            expected = dict(VALUES)
            for repeat in range(ARGS.write_rounds):
                updates = []
                for number in range(count):
                    target = number * 16 if pattern == 'distinct' else 0
                    body = b''.join(hashlib.sha256(f'Layer2-final:{repeat}:{number}:{chunk}'.encode()).digest()
                                    for chunk in range(math.ceil(ARGS.body_bytes / 32)))[:ARGS.body_bytes]
                    updates.append({'collection': collection(target), 'key': key(target), 'body_hex': body.hex()})
                    expected[key(target)] = body.hex()
                request = ROOT / 'write-request.jsonl'
                request.write_text(''.join(json.dumps(update, separators=(',', ':')) + '\n' for update in updates))
                order = list(BINARIES) if repeat % 2 == 0 else list(reversed(BINARIES))
                for variant in order:
                    before = metadata(homes[variant])
                    result = command(variant, 'write', homes[variant], request)
                    after = metadata(homes[variant])
                    new = {path: size for path, size in after.items() if path not in before}
                    if result['operations'] != count or result['frontier'] != repeat + 1:
                        raise RuntimeError('Complete batch count or frontier differs')
                    result['new_locator_directory_pages'] = sum(new.values()) // 16384
                    result['new_locator_directory_parts'] = len(new)
                    result['retained_locator_directory_parts'] = len(after)
                    result.update(variant=variant, operation='write', pattern=pattern, batch=count, repeat=repeat)
                    remember(result)
            verify_batch = 1024 if ARGS.body_bytes > 8192 else ARGS.records
            for first in range(0, ARGS.records, verify_batch):
                numbers = list(range(first, min(ARGS.records, first + verify_batch)))
                read_path = ROOT / 'verify-request.json'
                save(read_path, {'coordinates': [{'collection': collection(n), 'key': key(n)} for n in numbers]})
                with ThreadPoolExecutor(max_workers=2) as pool:
                    calls = {variant:pool.submit(command, variant, 'read', homes[variant], read_path)
                             for variant in BINARIES}
                    for variant, call in calls.items():
                        result = call.result()
                        check_read(result, numbers, expected)
                        result.update(variant=variant, operation='read-after-write', pattern=pattern, batch=count,
                                      verification_first=first, verification_batch=len(numbers))
                        result['disk_size'] = home_size(homes[variant])
                        remember(result)
            print(f'write {pattern} {count} complete', flush=True)


def range_measurements():
    homes = build_pair('ranges')
    for count in ARGS.batches:
        request = ROOT / 'range-request.json'
        save(request, {'collection': COLLECTION, 'prefix': PREFIX,
                       'start': 'record-001024', 'end': 'record-002048',
                       'page_size': count, 'max_pages': 1})
        wanted = [(key(n), VALUES[key(n)]) for n in range(1024, 2048) if VALUES[key(n)] is not None][:count]
        for repeat in range(ARGS.read_rounds):
            order = list(BINARIES) if repeat % 2 == 0 else list(reversed(BINARIES))
            for variant in order:
                result = command(variant, 'range', homes[variant], request)
                rows = [row for page in result['pages'] for row in page['records']]
                actual = [(row['key'], row['body_hex']) for row in rows]
                if actual != wanted or result['exhausted']:
                    raise RuntimeError('Bounded product range differs from generated records')
                result['result_sha256'] = digest(actual)
                result['result_count'] = len(actual)
                result['full_bodies_match_generated_input'] = True
                del result['pages']
                result.update(variant=variant, operation='range', pattern='bounded', batch=count, repeat=repeat)
                remember(result)
        print(f'range bounded {count} complete', flush=True)


def summarize():
    summary = []
    for operation in ['read', 'range', 'write']:
        patterns = {'read':['contiguous', 'scattered', 'duplicates'], 'range':['bounded'], 'write':['distinct', 'repeated']}[operation]
        for pattern in patterns:
            for batch in [1, 16, 64, 256]:
                item = {'operation': operation, 'pattern': pattern, 'batch': batch}
                for variant in BINARIES:
                    rows = [r for r in RECEIPTS if r.get('operation') == operation and r.get('pattern') == pattern and r.get('batch') == batch and r['variant'] == variant]
                    if not rows:
                        continue
                    times = sorted(r['elapsed_nanos'] / 1e6 for r in rows)
                    result = {'runs': len(rows), 'median_ms': statistics.median(times), 'p95_ms': times[math.ceil(len(times) * .95) - 1]}
                    result['median_records_per_second'] = batch * 1000 / result['median_ms']
                    result['median_process_maximum_resident_bytes'] = statistics.median(r['process_maximum_resident_bytes'] for r in rows)
                    result['maximum_process_maximum_resident_bytes'] = max(r['process_maximum_resident_bytes'] for r in rows)
                    if all('process_peak_memory_bytes' in r for r in rows):
                        result['maximum_process_peak_memory_bytes'] = max(r['process_peak_memory_bytes'] for r in rows)
                    for field in ['file_opens', 'read_operations', 'format_probe_bytes', 'payload_bytes', 'temporary_bytes_peak']:
                        result[field] = statistics.median(r['disk'][field] for r in rows)
                    if operation == 'write':
                        result['new_locator_directory_pages'] = statistics.median(r['new_locator_directory_pages'] for r in rows)
                        result['initial_disk_size'] = next(r['disk_size'] for r in RECEIPTS if r.get('operation') == 'build' and r['variant'] == variant and r.get('case') == f'writes-{pattern}-{batch}')
                        result['after_writes_disk_size'] = next(r['disk_size'] for r in RECEIPTS if r.get('operation') == 'read-after-write' and r['variant'] == variant and r.get('pattern') == pattern and r.get('batch') == batch)
                    else:
                        for field in ['payload_blocks', 'map_pages', 'file_handles_peak', 'file_handle_reuses', 'batch_read_runs']:
                            if field in rows[0]['costs']:
                                result[field] = statistics.median(r['costs'][field] for r in rows)
                    item[variant] = result
                if 'candidate' not in item:
                    continue
                item['median_change_percent'] = (item['candidate']['median_ms'] / item['baseline']['median_ms'] - 1) * 100
                summary.append(item)
    save(ROOT / 'summary.json', summary)
    save(ROOT / 'receipts.json', RECEIPTS)


if ROOT.stat().st_uid != os.getuid() or ROOT.is_symlink():
    raise RuntimeError('Owned scratch root required')
with (ROOT / 'initial.jsonl').open('w') as output:
    for number in range(ARGS.records):
        body = b'' if number % 997 == 0 else b''.join(hashlib.sha256(f'Layer2-Oct16:{number}:{chunk}'.encode()).digest() for chunk in range(math.ceil(ARGS.body_bytes / 32)))[:ARGS.body_bytes]
        deleted = number % 251 == 0
        VALUES[key(number)] = None if deleted else body.hex()
        value = {'collection': collection(number), 'key': key(number)}
        value.update({'deleted': True} if deleted else {'body_hex': body.hex()})
        output.write(json.dumps(value, separators=(',', ':')) + '\n')
save(ROOT / 'inputs.json', {'records': ARGS.records, 'body_bytes': ARGS.body_bytes, 'read_rounds': ARGS.read_rounds, 'write_rounds': ARGS.write_rounds, 'hash_fanout': 1, 'initial_sha256': hashlib.sha256((ROOT / 'initial.jsonl').read_bytes()).hexdigest(), 'body_recipe': 'SHA-256 blocks from Layer2-Oct16:N:chunk, truncated to body_bytes; every997th body empty; every251st key deleted', 'binaries': {v: {'path':str(p), 'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for v, p in BINARIES.items()}, 'filesystem_pages':'unknown', 'load_start':os.getloadavg(), 'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'platform':platform.platform(), 'logical_cpu_count':os.cpu_count(), 'written_bytes':None, 'written_bytes_note':'not instrumented; new map pages and retained file size are separate measurements'})
input_receipt = json.loads((ROOT / 'inputs.json').read_text())
input_receipt.update(phase=ARGS.only, batches=ARGS.batches, write_patterns=ARGS.write_patterns,
                     disk_cap_bytes=None,
                     collection_count=256, keys_per_collection=16, key_recipe='generated NUL record-NNNNNN; every64th key adds hyphen and193 x bytes',
                     long_key_count=64, long_key_bytes=len(key(0).encode()),
                     update_body_recipe='SHA-256 bytes from Layer2-final:repeat:slot:chunk, truncated to body_bytes',
                     distinct_update_keys='16 times slot; one key per collection', repeated_update_key=0,
                     contiguous_read_keys='1024 plus slot',
                     scattered_read_keys='(73 plus 37 times slot) modulo 4096',
                     duplicate_read_keys=[1111,1112,1111,999999],
                     verification='complete known key batches; baseline and candidate verification calls run in parallel after timed calls',
                     driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
save(ROOT / 'inputs.json', input_receipt)
if ARGS.only in ['reads', 'all']:
    read_measurements()
if ARGS.only in ['writes', 'all']:
    write_measurements()
summarize()
save(ROOT / 'completed.json', {'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'load_end':os.getloadavg()})
print(str(ROOT / 'summary.json'), flush=True)
