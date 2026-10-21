#!/usr/bin/env python3
"""Pack a clone of an `ingest.py` home, start a node on it, and read every acknowledged key back.

  LDBIN=<bin dir of the node> readback.py --home SNAPSHOT --ops N --pack-bin DIR [--stride 1]

`ingest.py` wrote operation i as key (bucket `b%03d % (i % 256)`, id `r%09d % i`) with the value of 100
bytes `v`. The script clones SNAPSHOT with `cp -c`, packs the clone with the `lastdb-pack-maintain` of
`--pack-bin`, starts the node of `LDBIN` on the packed clone (with automatic maintenance off), and reads
the keys of operations 0, stride, 2*stride ... N-1 through `/api/query`, 100 keys for each call. Every key
must come back with its value. Prints one JSON line and exits non-zero on any miss.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--home', required=True)
ap.add_argument('--ops', type=int, required=True)
ap.add_argument('--pack-bin', required=True)
ap.add_argument('--stride', type=int, default=1)
ap.add_argument('--buckets', type=int, default=256)
args = ap.parse_args()

source = ldc.refuse_primary(args.home)
clone = ldc.clone_path('rb')
subprocess.run(['cp', '-cRp', str(source), str(clone)], check=True)
began = time.time()
packed = subprocess.run([str(Path(args.pack_bin) / 'lastdb-pack-maintain'), 'compact', '--home',
                         str(ldc.pack_root(clone)), '--grace-ms', '0'], capture_output=True, text=True)
pack_s = time.time() - began
parts = ldc.receipt_summary(clone)['parts']
node = ldc.Node(clone, {'LASTDB_AUTO_MAINTENANCE': '0'})
node.start(timeout=1800)
indices = range(0, args.ops, args.stride)
keys = [('b%03d' % (i % args.buckets), 'r%09d' % i) for i in indices]
began = time.time()
got = ldc.read_keys(node, 'Cap', keys)
read_s = time.time() - began
missing = [k for k in keys if got.get(k) != 'v' * 100]
node.stop()
print(json.dumps({'ev': 'readback', 'pack_exit': packed.returncode, 'pack_s': round(pack_s, 1), 'parts_after_pack': parts,
                  'keys_read': len(keys), 'missing_or_wrong': len(missing), 'read_s': round(read_s, 1),
                  'first_missing': missing[:3]}), flush=True)
ldc.remove_owned(clone)
sys.exit(0 if packed.returncode == 0 and not missing else 1)
