#!/usr/bin/env python3
"""Record oracle for a STOPPED synthetic home.

  LDBIN=<bin dir> oracle.py HOME [--out FILE] [--root]

Runs `lastdb-pack-maintain records` (one JSON line for every stored record, read through the
supported administrative stream, which checks every record digest) and prints one JSON summary:
the record count, the count for each collection, and an order-independent hash of the multiset of
(collection, key, body SHA-256, batch, operation). `digest_full` also covers the hash group and the
body length. A pack must not change either digest. The hash is the sum, modulo 2^256, of the SHA-256 of
each canonical row, so the order of the lines does not matter.
"""
import argparse
import collections
import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('home')
ap.add_argument('--out', default='')
ap.add_argument('--root', action='store_true', help='HOME is already a native root (holds CURRENT)')
args = ap.parse_args()

home = ldc.refuse_primary(args.home)
mod = 1 << 256
digest4 = 0
digest_full = 0
count = 0
per_collection = collections.Counter()
pack = home if args.root else ldc.pack_root(home)
proc = subprocess.Popen([str(ldc.PACK_MAINTAIN), 'records', '--home', str(pack)],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1 << 20)
tail = None
for line in proc.stdout:
    try:
        row = json.loads(line)
    except ValueError:
        continue
    if 'collection' not in row:
        tail = row
        continue
    core = '\x1f'.join((row['collection'], row['key'], row['body_sha256'],
                        str(row['batch']), str(row['operation'])))
    full = core + '\x1f' + str(row['group']) + '\x1f' + str(row['body_bytes'])
    digest4 = (digest4 + int.from_bytes(hashlib.sha256(core.encode()).digest(), 'big')) % mod
    digest_full = (digest_full + int.from_bytes(hashlib.sha256(full.encode()).digest(), 'big')) % mod
    per_collection[row['collection']] += 1
    count += 1
err = proc.stderr.read()
code = proc.wait()
result = {'home': str(home), 'exit': code, 'records': count, 'tool_records': (tail or {}).get('result', {}).get('records'),
          'by_collection': dict(sorted(per_collection.items())),
          'digest': format(digest4, '064x'), 'digest_full': format(digest_full, '064x')}
if code != 0:
    result['stderr'] = err[-400:]
print(json.dumps(result, sort_keys=True))
if args.out:
    with open(args.out, 'w') as handle:
        handle.write(json.dumps(result, sort_keys=True) + '\n')
sys.exit(0 if code == 0 and count == result['tool_records'] else 1)
