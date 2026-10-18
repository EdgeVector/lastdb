#!/usr/bin/env python3
"""Measure the native disk CLI on fresh synthetic OPE-shaped tip keys."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import tempfile
import time

WORKTREE = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).resolve().parent
BINARY = WORKTREE / 'target/release/lastdb-disk-home'


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    scratch = Path(tempfile.mkdtemp(prefix='schema-range-disk-', dir=os.environ['TMPDIR']))
    scratch.chmod(0o700)
    info = scratch.stat()
    save(scratch / '.synthetic-seed-owner.json', {
        'version': 1, 'kind': 'synthetic-trial', 'owner_uid': os.getuid(),
        'device': info.st_dev, 'inode': info.st_ino, 'creator_pid': os.getpid()})
    (scratch / '.synthetic-seed-owner.json').chmod(0o600)
    preserved = scratch / 'lastdb-disk-home'
    shutil.copy2(BINARY, preserved)
    molecule = 'm' * 43
    encoded_hash = 'h' * 22
    prefix = f'mk:{molecule}:{encoded_hash}\0'
    source_key = bytes(range(32))  # Public synthetic input, not a real encryption key.
    results = []
    receipt = {
        'kind': 'current native disk component, not a complete schema preload',
        'scratch': str(scratch), 'owner_uid': os.getuid(),
        'source_binary': str(BINARY), 'binary_sha256': sha(preserved),
        'driver_sha256': sha(Path(__file__)),
        'conditions': {'hash_fanout': 1, 'repeats': 3,
            'ope': 'actual per-position v1 transform with public synthetic bytes',
            'payload': '318-byte plain JSON tip, no value decryption or core admission',
            'device_cache': 'uncontrolled; generated files likely remain in OS cache',
            'elapsed': 'CLI internal read/open/build time, excludes final stdout emission',
            'not_measured': ['real schema discovery', 'generation merge', 'delete barriers',
                             'tip decryption', 'directory publication', 'the proposed packed-key format']},
        'results': results}
    save(OUTPUT / 'disk-component.json', receipt)
    try:
        for count in [1000, 3000]:
            for range_bytes in [24, 54]:
                name = f'keys-{count}-range-{range_bytes}'
                case = scratch / name
                case.mkdir()
                home = case / 'home'
                seed = case / 'seed.jsonl'
                highs = []
                for position in range(range_bytes):
                    message = b'rk|ope|v1\0' + molecule.encode() + b'\0' + position.to_bytes(4, 'big')
                    highs.append(int.from_bytes(hmac.new(source_key, message, hashlib.sha256).digest()[:4], 'big') & 0xffffff00)
                with seed.open('w') as output:
                    for index in range(count):
                        plain = ('r' * (range_bytes - 10) + f'{index:010d}').encode()
                        encoded = ''.join(f'{high | value:08x}' for high, value in zip(highs, plain))
                        entry = {'entry': {'atom_uuid': f'{index:064x}',
                            'written_at': 1700000000000000000, 'logical_counter': 1234567,
                            'mutation_uuid': 'm' * 36, 'device_id': 'd' * 44, 'origin_device': 'o' * 36}}
                        tip = json.dumps(entry, separators=(',', ':')).encode()
                        output.write(json.dumps({'collection': 'tips', 'key': prefix + encoded,
                                                 'body_hex': tip.hex()}, separators=(',', ':')) + '\n')
                built = subprocess.run([str(preserved), 'build', '--home', str(home),
                    '--input', str(seed), '--fanout', '1'], capture_output=True, text=True, timeout=120)
                build_result = json.loads(built.stdout) if built.returncode == 0 else {'error': built.stderr}
                item = {'name': name, 'keys': count, 'original_range_bytes': range_bytes,
                    'stored_key_bytes': len(prefix.encode()) + range_bytes * 8,
                    'tip_json_bytes': len(tip), 'build': build_result, 'reads': []}
                results.append(item)
                if built.returncode != 0:
                    save(OUTPUT / 'disk-component.json', receipt)
                    continue
                request = case / 'range.json'
                save(request, {'collection': 'tips', 'prefix': prefix, 'start': '', 'end': None,
                    'page_size': 256, 'max_pages': 100,
                    'budget': {'pages': 4096, 'blocks': 16384, 'bytes': 268435456, 'keys': 10000}})
                for repeat in range(3):
                    started = time.perf_counter_ns()
                    read = subprocess.run([str(preserved), 'range', '--home', str(home),
                        '--input', str(request)], capture_output=True, text=True, timeout=120)
                    wall = time.perf_counter_ns() - started
                    if read.returncode != 0:
                        item['reads'].append({'repeat': repeat, 'wall_nanos': wall, 'error': read.stderr})
                        continue
                    response = json.loads(read.stdout)
                    rows = [row for page in response['pages'] for row in page['records']]
                    if repeat == 0:
                        (OUTPUT / (name + '-first.json')).write_text(read.stdout)
                    item['reads'].append({'repeat': repeat, 'wall_nanos': wall,
                        'elapsed_nanos': response['elapsed_nanos'], 'record_count': len(rows),
                        'exhausted': response['exhausted'], 'costs': response['costs'],
                        'disk': response['disk'], 'stdout_sha256': hashlib.sha256(read.stdout.encode()).hexdigest(),
                        'first_key': rows[0]['key'] if rows else None,
                        'last_key': rows[-1]['key'] if rows else None})
                elapsed = [row['elapsed_nanos'] for row in item['reads'] if 'elapsed_nanos' in row]
                item['median_elapsed_ms'] = statistics.median(elapsed) / 1e6 if elapsed else None
                item['all_complete'] = all(row.get('record_count') == count and row.get('exhausted')
                                           for row in item['reads']) and len(item['reads']) == 3
                save(OUTPUT / 'disk-component.json', receipt)
                print(json.dumps({'case': name, 'median_ms': item['median_elapsed_ms'],
                                  'all_complete': item['all_complete']}), flush=True)
    finally:
        save(OUTPUT / 'disk-component.json', receipt)
        clean = subprocess.run(['lastdb-dev', 'reclaim', '--home', str(scratch), '--exact-home'],
                               capture_output=True, text=True, timeout=60)
        save(OUTPUT / 'disk-cleanup.json', {'root': str(scratch), 'returncode': clean.returncode,
            'stdout': clean.stdout, 'stderr': clean.stderr, 'exists_after': scratch.exists()})
        print(json.dumps({'cleanup_returncode': clean.returncode, 'scratch_exists': scratch.exists()}), flush=True)


if __name__ == '__main__':
    main()
