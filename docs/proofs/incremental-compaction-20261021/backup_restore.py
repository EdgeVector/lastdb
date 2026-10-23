#!/usr/bin/env python3
"""Backup and restore of a never-packed home that has one locator object.

  LDBIN=<bin dir> backup_restore.py [--ops 4000] [--oracle-bin DIR]

1. A node writes a fresh home: new keys, overwrites and purges (so the home has tombstones and several
   versions), with automatic maintenance off, then stops.
2. The record oracle reads the live home.
3. `lastdb-disk-recovery backup` makes a bundle from the live home and `restore` makes a new root from the
   bundle, without the source.
4. The record oracle reads the restored root and must equal the live home. The files that CURRENT names
   (`lastdb-pack-maintain inventory`: path and size) must be the same list in both roots.
Prints one JSON line with the result and exits non-zero on any difference.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

here = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument('--ops', type=int, default=4000)
ap.add_argument('--oracle-bin', default='')
ap.add_argument('--keep', action='store_true')
args = ap.parse_args()
oracle_bin = args.oracle_bin or str(ldc.BIN)
recovery = ldc.BIN / 'lastdb-disk-recovery'

home = ldc.new_home('backup')
node = ldc.Node(home, {'LASTDB_AUTO_MAINTENANCE': '0'})
node.start()
ldc.seed(node, 'Cap')


def writer(client):
    for i in range(args.ops // 500 // 4):
        base = (client * 1000 + i) * 500
        ldc.mutate(node, [ldc.put('Cap', 'b%02d' % ((base + j) % 64), 'r%09d' % (base + j), 'v%d' % (j % 50),
                                  durable=False) for j in range(500)])


threads = [threading.Thread(target=writer, args=(c,)) for c in range(4)]
[t.start() for t in threads]
[t.join() for t in threads]
# overwrites and purges, durable
keys = [('b%02d' % ((j) % 64), 'r%09d' % j) for j in range(0, 400)]
ldc.mutate(node, [ldc.put('Cap', b, r, 'second-' + r, durable=True) for b, r in keys[:300]])
ldc.mutate(node, [ldc.put('Cap', b, r, '', durable=True, typ='purge') for b, r in keys[300:400]])
time.sleep(3)
node.stop()
live_summary = ldc.receipt_summary(home)


def oracle(path, root=False):
    done = subprocess.run([sys.executable, str(here / 'oracle.py'), str(path)] + (['--root'] if root else []),
                          env=dict(os.environ, LDBIN=oracle_bin), capture_output=True, text=True)
    row = json.loads(done.stdout.strip().splitlines()[-1])
    row['ok'] = done.returncode == 0
    return row


def inventory(root):
    done = subprocess.run([str(ldc.PACK_MAINTAIN), 'inventory', '--home', str(root)], capture_output=True, text=True)
    files = json.loads(done.stdout)['result']['files']
    return sorted((f['path'], f['bytes']) for f in files)


root = ldc.pack_root(home)
live = oracle(home)
bundle = Path(f'{home}.bundle')
restored = Path(f'{home}.restored')
ldc.refuse_primary(bundle)
ldc.refuse_primary(restored)
began = time.time()
backup = subprocess.run([str(recovery), 'backup', '--home', str(root), '--backup', str(bundle)],
                        capture_output=True, text=True)
backup_s = time.time() - began
began = time.time()
restore = subprocess.run([str(recovery), 'restore', '--home', str(restored), '--backup', str(bundle)],
                         capture_output=True, text=True)
restore_s = time.time() - began
result = {'ev': 'backup-restore', 'live_parts': live_summary['parts'], 'live_locator_objects': live_summary['locator_objects'],
          'records': live['records'], 'backup_exit': backup.returncode, 'restore_exit': restore.returncode,
          'backup_s': round(backup_s, 1), 'restore_s': round(restore_s, 1)}
ok = backup.returncode == 0 and restore.returncode == 0
if ok:
    after = oracle(restored, root=True)
    result['restored_records'] = after['records']
    result['records_equal'] = (after['ok'] and after['records'] == live['records']
                               and after['by_collection'] == live['by_collection']
                               and after['digest'] == live['digest'] and after['digest_full'] == live['digest_full'])
    result['inventory_equal'] = inventory(root) == inventory(restored)
    ok = result['records_equal'] and result['inventory_equal']
result['equal'] = ok
print(json.dumps(result), flush=True)
if not args.keep:
    for path in (home, bundle, restored):
        ldc.remove_owned(path)
sys.exit(0 if ok else 1)
