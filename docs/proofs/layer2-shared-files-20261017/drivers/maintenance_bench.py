#!/usr/bin/env python3
"""Record real compact/reclaim costs on one owned generated native home."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

PARSER = argparse.ArgumentParser(description=__doc__)
PARSER.add_argument('--binary', required=True, type=Path)
PARSER.add_argument('--home', required=True, type=Path)
PARSER.add_argument('--owned-root', required=True, type=Path)
PARSER.add_argument('--output', required=True, type=Path)
PARSER.add_argument('--command-timeout-seconds',type=int,default=600)
ARGS = PARSER.parse_args()
BINARY = ARGS.binary.resolve(strict=True)
OWNED = ARGS.owned_root.resolve(strict=True)
NATIVE_HOME = ARGS.home.resolve(strict=True)
OUTPUT = ARGS.output.resolve()
if NATIVE_HOME == OWNED or OWNED not in NATIVE_HOME.parents or OWNED.stat().st_uid != os.getuid():
    PARSER.error('home must be inside the exact owned benchmark root')
if not (NATIVE_HOME / 'laststore-pack-home-v1').is_file():
    PARSER.error('explicit native generated home required')
OUTPUT.mkdir(parents=True, exist_ok=False)
RECEIPTS = []


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n')


def command(operation, extra=None):
    arguments = [str(BINARY), operation, '--home', str(NATIVE_HOME)] + (extra or [])
    environment = dict(os.environ)
    environment.pop('LASTDB_DISK_CAP_BYTES', None)
    started = time.perf_counter_ns()
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=ARGS.command_timeout_seconds, env=environment)
    elapsed = time.perf_counter_ns() - started
    if result.returncode:
        raise RuntimeError(f'Product {operation} failed: {result.stderr[:500]}')
    parsed = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    if operation == 'records':
        summary = parsed.pop()
        ordered = hashlib.sha256()
        for record in parsed:
            ordered.update(json.dumps(record, sort_keys=True, separators=(',', ':')).encode())
            ordered.update(b'\n')
        value = {'summary':summary, 'records':len(parsed), 'ordered_record_digest':ordered.hexdigest()}
    else:
        if len(parsed) != 1:
            raise RuntimeError(f'Product {operation} returned an unexpected record count')
        value = parsed[0]
    receipt = {'operation':operation, 'process_elapsed_nanos':elapsed, 'receipt':value}
    RECEIPTS.append(receipt)
    with (OUTPUT / 'receipts.jsonl').open('a') as output:
        output.write(json.dumps(receipt, sort_keys=True, separators=(',', ':')) + '\n')
    return value


def files():
    # Bound this owned native-home walk to its documented depth and finite file cap.
    stack = [(NATIVE_HOME, 0)]
    count = logical = allocated = 0
    by_kind = {}
    while stack:
        parent, depth = stack.pop()
        if depth > 6:
            raise RuntimeError('Owned native layout exceeds depth six')
        for path in parent.iterdir():
            if path.is_symlink():
                raise RuntimeError('Owned native file inventory refuses a symlink')
            if path.is_dir():
                stack.append((path, depth + 1))
            elif path.is_file():
                stat = path.stat()
                count += 1
                logical += stat.st_size
                allocated += stat.st_blocks * 512
                kind = path.relative_to(NATIVE_HOME).parts[0]
                entry = by_kind.setdefault(kind, {'files':0,'logical_file_bytes':0,'allocated_file_bytes':0})
                entry['files'] += 1
                entry['logical_file_bytes'] += stat.st_size
                entry['allocated_file_bytes'] += stat.st_blocks * 512
                if count > 262144:
                    raise RuntimeError('Owned native file inventory exceeds its finite file cap')
    return {'files':count, 'logical_file_bytes':logical, 'allocated_file_bytes':allocated,
            'by_top_level_kind':by_kind}


save(OUTPUT / 'inputs.json', {'binary':str(BINARY), 'binary_sha256':hashlib.sha256(BINARY.read_bytes()).hexdigest(),
                              'home':str(NATIVE_HOME), 'owned_root':str(OWNED), 'primary_data':False,
                              'grace_ms':0, 'filesystem_pages':'unknown', 'load_start':os.getloadavg()})
reference = None
try:
    before_records = command('records')
    before = files()
    reference = command('reference', ['--purpose','backup'])['result']['reference']
    compact = command('compact', ['--grace-ms','0'])
    compacted = files()
    retained_reclaim = command('reclaim', ['--grace-ms','0'])
    held = files()
    command('release', ['--reference',reference])
    reference = None
    released_reclaim = command('reclaim', ['--grace-ms','0'])
    released = files()
    after_records = command('records')
    if (before_records['ordered_record_digest'], before_records['records']) != (
            after_records['ordered_record_digest'], after_records['records']):
        raise RuntimeError('Product records changed across compact/reclaim')
    save(OUTPUT / 'summary.json', {'before':before, 'after_compact':compacted,
                                   'after_reclaim_with_backup_hold':held,
                                   'after_reclaim_without_hold':released,
                                   'compact':compact, 'retained_reclaim':retained_reclaim,
                                   'released_reclaim':released_reclaim,
                                   'records':after_records['records'],
                                   'record_digest_preserved':True,
                                   'record_digest':after_records['ordered_record_digest']})
finally:
    if reference is not None:
        command('release', ['--reference',reference])
    save(OUTPUT / 'receipts.json', RECEIPTS)
print(str(OUTPUT / 'summary.json'), flush=True)

