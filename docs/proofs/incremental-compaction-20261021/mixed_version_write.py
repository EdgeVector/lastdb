#!/usr/bin/env python3
"""Mixed-version writer test: main writes, then new continues on same home.

  LDBIN=<new bin dir> mixed_version_write.py [--main-bin DIR]

Tests S2 safety when a new-build writer continues writes on a home created by main.
This validates the empty-tree assumption in locator_forest.rs: if main-written
locator objects don't have empty page zero, the new build must detect this and
create a new object rather than corrupting the existing one.

The test:
1. Writes 1,000 operations with main binaries (never-packed home, old locator layout)
2. Continues with 1,000 more operations using new binaries (mixed version)
3. Verifies record oracle matches before and after
4. Packs with new binaries and verifies oracle again
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

here = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument('--main-bin', required=True, help='Directory with main-build binaries')
ap.add_argument('--keep', action='store_true')
args = ap.parse_args()

main_bin = Path(args.main_bin).resolve()
new_bin = ldc.BIN

print(json.dumps({'ev': 'start', 'main_bin': str(main_bin), 'new_bin': str(new_bin)}), flush=True)

# Phase 1: Write with main
home = ldc.new_home('mixed-version')
print(json.dumps({'ev': 'home', 'home': str(home)}), flush=True)

old_ldbin = ldc.BIN
try:
    ldc.BIN = main_bin
    node = ldc.Node(home, {'LASTDB_AUTO_MAINTENANCE': '0'})
    node.start()
    ldc.seed(node, 'MixedTest')
    
    for i in range(0, 1000, 100):
        ops = [ldc.put('MixedTest', 'b%02d' % (j % 8), 'k%05d' % j, 'main-v%d' % j) for j in range(i, i + 100)]
        ldc.mutate(node, ops)
    
    node.stop()
    print(json.dumps({'ev': 'main_write_done', 'ops': 1000}), flush=True)
finally:
    ldc.BIN = old_ldbin

# Oracle before mixed writes
done = subprocess.run([sys.executable, str(here / 'oracle.py'), str(home)],
                     env=dict(os.environ, LDBIN=str(main_bin)), capture_output=True, text=True)
oracle_before = json.loads(done.stdout.strip().splitlines()[-1])
print(json.dumps({'ev': 'oracle_before', 'records': oracle_before['records'], 
                  'digest': oracle_before['digest']}), flush=True)

# Phase 2: Continue writes with new binaries
node = ldc.Node(home, {'LASTDB_AUTO_MAINTENANCE': '0'})
node.start()

for i in range(1000, 2000, 100):
    ops = [ldc.put('MixedTest', 'b%02d' % (j % 8), 'k%05d' % j, 'new-v%d' % j) for j in range(i, i + 100)]
    ldc.mutate(node, ops)

# Also overwrite some main keys
for i in range(0, 500, 50):
    ops = [ldc.put('MixedTest', 'b%02d' % (j % 8), 'k%05d' % j, 'overwrite-%d' % j) for j in range(i, i + 50)]
    ldc.mutate(node, ops)

node.stop()
print(json.dumps({'ev': 'new_write_done', 'ops': 2000}), flush=True)

# Oracle after mixed writes
done = subprocess.run([sys.executable, str(here / 'oracle.py'), str(home)],
                     env=dict(os.environ, LDBIN=str(main_bin)), capture_output=True, text=True)
oracle_after = json.loads(done.stdout.strip().splitlines()[-1])
print(json.dumps({'ev': 'oracle_after', 'records': oracle_after['records'],
                  'digest': oracle_after['digest']}), flush=True)

# Phase 3: Pack and verify
clone = ldc.new_home('mixed-pack-clone')
subprocess.run(['cp', '-c', '-r', str(ldc.pack_root(home)), str(clone)], check=True)
subprocess.run([str(new_bin / 'lastdb-pack-maintain'), 'compact', '--home', str(clone),
                '--grace-ms', '0'], check=True, capture_output=True)
print(json.dumps({'ev': 'packed'}), flush=True)

done = subprocess.run([sys.executable, str(here / 'oracle.py'), str(clone)],
                     env=dict(os.environ, LDBIN=str(main_bin)), capture_output=True, text=True)
oracle_packed = json.loads(done.stdout.strip().splitlines()[-1])
print(json.dumps({'ev': 'oracle_packed', 'records': oracle_packed['records'],
                  'digest': oracle_packed['digest']}), flush=True)

# Verify
success = (oracle_after['exit'] == 0 and 
           oracle_packed['exit'] == 0 and
           oracle_after['digest'] == oracle_packed['digest'] and
           oracle_packed['records'] > oracle_before['records'])

result = {
    'ev': 'result',
    'success': success,
    'records_before': oracle_before['records'],
    'records_after': oracle_after['records'],
    'records_packed': oracle_packed['records'],
    'digest_stable': oracle_after['digest'] == oracle_packed['digest']
}
print(json.dumps(result), flush=True)

if not args.keep:
    ldc.remove_owned(home)
    ldc.remove_owned(clone)

sys.exit(0 if success else 1)
