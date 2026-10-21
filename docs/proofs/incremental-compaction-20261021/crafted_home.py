#!/usr/bin/env python3
"""A crafted synthetic home with the record shapes that a pack must keep exactly.

  LDBIN=<bin dir> crafted_home.py [--seed 7] [--out FILE]

1. A real node writes a tiny home (schema, one write) and stops. That gives a native home with the current
   feature bits.
2. `lastdb-disk-home write` then appends complete batches straight into that stopped home, with explicit
   collections, keys and bodies. Every batch is a separate journal batch, so the home has many batches and
   the keys of a group come from many of them.

Shapes: overwrites (also of big bodies by small ones), tombstones, a key written again after its tombstone,
empty bodies, records of exactly 4095, 4096, 4097, 8191, 8192 and 8193 bytes, 1 MiB bodies (random and
compressible, 24 MiB in one batch, so records cross a part boundary of 8 MiB), keys of 1,000 to 60,000
bytes (long keys use overflow pages), keys with a long common prefix, and many small batches of mixed work.
The node never opens this home again; the proof tools read it at the storage layer.
Prints the home path as the last line.
"""
import argparse
import hashlib
import json
import os
import random
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--seed', type=int, default=7)
ap.add_argument('--batches', type=int, default=40, help='small mixed batches at the end')
args = ap.parse_args()
rng = random.Random(args.seed)
DISK_HOME = ldc.BIN / 'lastdb-disk-home'

home = ldc.new_home('craft')
node = ldc.Node(home, {'LASTDB_AUTO_MAINTENANCE': '0'})
node.start()
ldc.seed(node, 'Cap')
ldc.mutate(node, [ldc.put('Cap', 'b00', 'k0', 'v', durable=True)])
node.stop()
pack = str(ldc.pack_root(home))
live = {}          # (collection, key) -> True for a live record, False for a tombstone
model = {}         # (collection, key) -> SHA-256 of the latest body, or None after a tombstone
batch_no = [0]


def body(size, kind):
    if size == 0:
        return b''
    if kind == 'rand':
        return rng.randbytes(size)
    if kind == 'zero':
        return bytes(size)
    text = (b'the quick brown fox %d ' % rng.randrange(10 ** 6)) * (size // 20 + 1)
    return text[:size]


def put(collection, key, size, kind='text'):
    live[(collection, key)] = True
    data = body(size, kind)
    model[(collection, key)] = hashlib.sha256(data).hexdigest()
    return {'collection': collection, 'key': key, 'body_hex': data.hex()}


def delete(collection, key):
    live[(collection, key)] = False
    model[(collection, key)] = None
    return {'collection': collection, 'key': key, 'deleted': True}


def write(rows):
    batch_no[0] += 1
    path = Path(str(home) + f'.batch{batch_no[0]}.jsonl')
    ldc.refuse_primary(path)
    with open(path, 'w') as handle:
        for row in rows:
            line = json.dumps(row, separators=(',', ':'))
            assert len(line) < 2_200_000, 'line too long'
            handle.write(line + '\n')
    done = subprocess.run([str(DISK_HOME), 'write', '--home', pack, '--input', str(path)],
                          capture_output=True, text=True)
    path.unlink()
    if done.returncode != 0:
        raise SystemExit('write failed: ' + done.stderr[-400:])


collections = ['craft_a', 'craft_b', 'craft_c', 'craft_d']
# 1. small records in four collections
write([put(rng.choice(collections), 'k%06d' % i, rng.randrange(0, 300), rng.choice(['rand', 'text']))
       for i in range(3000)])
# 2. overwrite half, delete some, add new keys
keys = sorted(k for k, v in live.items() if v)
rows = {}
for key in rng.sample(keys, 1500):
    rows[key] = put(key[0], key[1], rng.choice([0, 1, 50, 700, 5000]), rng.choice(['rand', 'text']))
for key in rng.sample([k for k in keys if k not in rows], 300):
    rows[key] = delete(*key)
for i in range(3000, 3500):
    key = (rng.choice(collections), 'k%06d' % i)
    rows[key] = put(key[0], key[1], rng.randrange(0, 200))
write(list(rows.values()))
# 3. record sizes around the 4096 byte block boundary: record bytes = 96 + len(key) + body
rows = []
for target in (4095, 4096, 4097, 8191, 8192, 8193, 12287, 12288, 12289):
    for pad in (0, 1, 14):
        key = 'edge%d_%03d' % (target, len(rows)) + 'x' * pad
        rows.append(put('craft_edge', key, target - 96 - len(key), 'rand'))
write(rows)
# 4. 1 MiB bodies, 24 MiB in one batch: records cross the 8 MiB part boundary
write([put('craft_big', 'big%02d' % i, 1 << 20, 'rand' if i % 2 == 0 else 'zero') for i in range(24)])
# 5. long keys (over 1,024 bytes use overflow pages) and a long common prefix
rows = []
for length in (1000, 1024, 1025, 3000, 20000, 60000):
    rows.append(put('craft_long', ('L%d_' % length).ljust(length, 'k'), rng.randrange(0, 20000)))
for i in range(30):
    rows.append(put('craft_long', 'p' * 2000 + '%04d' % i, rng.randrange(0, 3000)))
write(rows)
# 6. big bodies replaced by small ones, tombstones of big bodies, a key written again after its tombstone
big = [('craft_big', 'big%02d' % i) for i in range(24)]
rows = [put(c, k, 10) for c, k in big[:6]] + [delete(c, k) for c, k in big[6:10]]
tomb = [k for k, v in live.items() if not v and k[0] in collections]
rows += [put(c, k, 77) for c, k in rng.sample(tomb, min(20, len(tomb)))]
write(rows)
write([put(c, k, 5000, 'rand') for c, k in big[6:8]])   # big keys written again after tombstones
# 7. many small mixed batches
for _ in range(args.batches):
    rows = {}
    pool = list(live)
    for _ in range(rng.randrange(20, 60)):
        action = rng.random()
        key = rng.choice(pool)
        if action < 0.5:
            rows[key] = put(key[0], key[1], rng.choice([0, 1, 30, 400, 3000, 9000]), rng.choice(['rand', 'text']))
        elif action < 0.7:
            rows[key] = delete(*key)
        else:
            new = (rng.choice(collections), 'n%06d' % rng.randrange(10 ** 6))
            rows[new] = put(new[0], new[1], rng.randrange(0, 500))
    write(list(rows.values()))
expected = Path(str(home) + '.expected.json')
expected.write_text(json.dumps([[c, k, v] for (c, k), v in sorted(model.items())]))
print(json.dumps({'ev': 'crafted', 'expected': str(expected), 'batches': batch_no[0], 'logical_keys': len(live),
                  'tombstones': sum(1 for v in live.values() if not v), 'receipt': ldc.receipt_summary(home)}))
print(home)
