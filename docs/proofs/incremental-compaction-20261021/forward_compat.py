#!/usr/bin/env python3
"""Forward compatibility test: main reader on new-build home.

  LDBIN=<main bin dir> forward_compat.py [--new-bin DIR]

Tests forward compatibility: can a main-build reader open and read a home
written by the new build (which uses S2: one locator object, 4 parts per batch)?

The test:
1. Writes 1,000 operations with new binaries (S2 layout: one locator object)
2. Reads all keys back with main binaries
3. Runs oracle with main binaries to verify record integrity
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
ap.add_argument('--new-bin', required=True, help='Directory with new-build binaries')
ap.add_argument('--keep', action='store_true')
args = ap.parse_args()

main_bin = ldc.BIN
new_bin = Path(args.new_bin).resolve()

print(json.dumps({'ev': 'start', 'main_bin': str(main_bin), 'new_bin': str(new_bin)}), flush=True)

# Phase 1: Write with new build
home = ldc.new_home('forward-compat')
print(json.dumps({'ev': 'home', 'home': str(home)}), flush=True)

old_ldbin = ldc.BIN
try:
    ldc.BIN = new_bin
    node = ldc.Node(home, {'LASTDB_AUTO_MAINTENANCE': '0'})
    node.start()
    ldc.seed(node, 'ForwardTest')
    
    acked_keys = []
    for i in range(0, 1000, 100):
        ops = [ldc.put('ForwardTest', 'b%02d' % (j % 8), 'k%05d' % j, 'value-%d' % j) for j in range(i, i + 100)]
        ldc.mutate(node, ops)
        acked_keys.extend([('b%02d' % (j % 8), 'k%05d' % j, 'value-%d' % j) for j in range(i, i + 100)])
    
    node.stop()
    print(json.dumps({'ev': 'new_write_done', 'keys': len(acked_keys)}), flush=True)
finally:
    ldc.BIN = old_ldbin

# Phase 2: Read back with main binaries
node = ldc.Node(home, {})
node.start()

missing = []
wrong = []
for bucket, key, expected in acked_keys:
    try:
        resp = ldc.read(node, 'ForwardTest', bucket, key)
        if resp != expected:
            wrong.append((bucket, key, expected, resp))
    except Exception as error:
        missing.append((bucket, key, str(error)))

node.stop()
print(json.dumps({'ev': 'readback', 'missing': len(missing), 'wrong': len(wrong),
                  'checked': len(acked_keys)}), flush=True)

# Phase 3: Oracle with main binaries
done = subprocess.run([sys.executable, str(here / 'oracle.py'), str(home)],
                     env=dict(os.environ, LDBIN=str(main_bin)), capture_output=True, text=True)
oracle = json.loads(done.stdout.strip().splitlines()[-1])
print(json.dumps({'ev': 'oracle', 'records': oracle['records'], 'exit': oracle['exit'],
                  'digest': oracle['digest']}), flush=True)

# Verify
success = (len(missing) == 0 and len(wrong) == 0 and oracle['exit'] == 0 and
           oracle['records'] == len(acked_keys))

result = {
    'ev': 'result',
    'success': success,
    'keys_written': len(acked_keys),
    'keys_missing': len(missing),
    'keys_wrong': len(wrong),
    'oracle_records': oracle['records'],
    'oracle_ok': oracle['exit'] == 0
}
print(json.dumps(result), flush=True)

if missing:
    print(json.dumps({'ev': 'missing_keys', 'sample': missing[:5]}), flush=True)
if wrong:
    print(json.dumps({'ev': 'wrong_keys', 'sample': wrong[:5]}), flush=True)

if not args.keep:
    ldc.remove_owned(home)

sys.exit(0 if success else 1)
