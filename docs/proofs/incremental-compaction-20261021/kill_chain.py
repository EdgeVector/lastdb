#!/usr/bin/env python3
"""SIGKILL chain on a synthetic home.

  LDBIN=<bin dir> kill_chain.py [--rounds 20] [--maintenance off|frequent] [--oracle-bin DIR]

Each round: three clients write durable requests (new keys, overwrites and purges) for a random time,
the script kills the node with SIGKILL while the writes run, reads the killed home with the record oracle
(`lastdb-pack-maintain records` verifies the digest of every record), restarts the node and reads back every
key. A key must read as its last acknowledged value. A request that was in flight at the kill may also have
been applied, so its value is accepted too. After the last round the node stops, the script packs a clone of
the home and checks that the record multiset does not change, and runs reclaim on the home itself.

--maintenance off       automatic maintenance is off: the home keeps all its parts (the shape of a home that
                        was never packed).
--maintenance frequent  the trigger is 250 parts, so a pack and a reclaim run all the time and the kills land
                        in them.
--oracle-bin            binaries for the oracle reads (default: LDBIN).
"""
import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

here = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument('--rounds', type=int, default=20)
ap.add_argument('--maintenance', choices=['off', 'frequent'], default='off')
ap.add_argument('--oracle-bin', default='')
ap.add_argument('--seed', type=int, default=5)
ap.add_argument('--min-secs', type=float, default=6.0)
ap.add_argument('--max-secs', type=float, default=20.0)
ap.add_argument('--keep', action='store_true')
args = ap.parse_args()

random.seed(args.seed)
env = ({'LASTDB_AUTO_MAINTENANCE': '0'} if args.maintenance == 'off' else
       {'LASTDB_DEPENDENCY_HIGH_WATER': '250', 'LASTDB_ORPHAN_MIN_AGE_MS': '1000',
        'LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS': '0'})
oracle_bin = args.oracle_bin or str(ldc.BIN)
home = ldc.new_home('chain')
print(json.dumps({'ev': 'home', 'home': str(home), 'maintenance': args.maintenance}), flush=True)

node = ldc.Node(home, env)
node.start()
ldc.seed(node, 'Cap')

lock = threading.Lock()
acked = {}       # (bucket, id) -> last acknowledged value; None after an acknowledged purge
pending = {}     # (bucket, id) -> values of requests that were in flight (may have been applied)
counter = [0]
errors = []


def request(muts, keys, values):
    """Send one request and record acknowledged and in-flight values."""
    with lock:
        for key, value in zip(keys, values):
            pending.setdefault(key, set()).add(value)
    ldc.mutate(node, muts)
    with lock:
        for key, value in zip(keys, values):
            acked[key] = value
            pending.pop(key, None)


def worker(stop):
    while not stop.is_set():
        with lock:
            base = counter[0]
            counter[0] += 5
        keys = [('b%02d' % (i % 32), 'i%08d' % i) for i in range(base, base + 5)]
        try:
            request([ldc.put('Cap', b, r, 'v1-%d' % base) for b, r in keys], keys, ['v1-%d' % base] * 5)
            request([ldc.put('Cap', b, r, 'v2-%d' % base) for b, r in keys], keys, ['v2-%d' % base] * 5)
            if base % 3 == 0:
                gone = keys[:2]
                request([ldc.put('Cap', b, r, '', typ='purge') for b, r in gone], gone, [None, None])
        except Exception as error:
            if node.proc.poll() is None and not stop.is_set():
                errors.append(str(error)[:200])
            return


def oracle(path, label):
    done = subprocess.run([sys.executable, str(here / 'oracle.py'), str(path)],
                          env=dict(os.environ, LDBIN=oracle_bin), capture_output=True, text=True)
    row = json.loads(done.stdout.strip().splitlines()[-1])
    row['ok'] = done.returncode == 0
    row['label'] = label
    return row


def verify():
    """Read back every key; returns the list of mismatches and reconciles to what was read."""
    keys = sorted(set(acked) | set(pending))
    bad = []
    got = ldc.read_keys(node, 'Cap', keys)
    for key in keys:
        value = got.get(key)
        allowed = {acked.get(key)} | pending.get(key, set())
        if key not in acked:
            allowed.add(None)          # first write was in flight
        if value not in allowed:
            bad.append((key, acked.get(key), sorted(map(str, pending.get(key, set()))), value))
        acked[key] = value
        pending.pop(key, None)
    return bad


records_seen = 0
all_bad = []
for rnd in range(1, args.rounds + 1):
    stop = threading.Event()
    threads = [threading.Thread(target=worker, args=(stop,)) for _ in range(3)]
    [t.start() for t in threads]
    time.sleep(random.uniform(args.min_secs, args.max_secs))
    node.kill()
    stop.set()
    [t.join() for t in threads]
    gen, frontier = ldc.current_gen(home)
    killed_parts = ldc.receipt_summary(home)['parts']
    killed_oracle = oracle(home, 'killed')
    began = time.time()
    node = ldc.Node(home, env)
    node.start()
    restart_s = time.time() - began
    bad = verify()
    all_bad.extend(bad)
    summary = ldc.receipt_summary(home)
    monotonic = killed_oracle['records'] >= records_seen
    records_seen = killed_oracle['records']
    print(json.dumps({'ev': 'round', 'round': rnd, 'keys': len(acked), 'killed_generation': gen,
                      'killed_parts': killed_parts, 'killed_records': killed_oracle['records'],
                      'killed_oracle_ok': killed_oracle['ok'], 'records_never_fall': monotonic,
                      'restart_s': round(restart_s, 1), 'parts_after_restart': summary['parts'],
                      'locator_objects': summary['locator_objects'], 'mismatches': len(bad)}), flush=True)
    for item in bad[:3]:
        print('MISMATCH', item, flush=True)
    if not killed_oracle['ok'] or not monotonic:
        all_bad.append(('oracle', rnd))
time.sleep(5)
node.stop()
final = oracle(home, 'final')
clone = Path(f'{home}.packed')
subprocess.run(['cp', '-cRp', str(home), str(clone)], check=True)
packed = subprocess.run([ldc.PACK_MAINTAIN, 'compact', '--home', str(ldc.pack_root(clone)), '--grace-ms', '0'],
                        capture_output=True, text=True)
after = oracle(clone, 'after-pack')
same = (packed.returncode == 0 and after['ok'] and final['ok'] and final['records'] == after['records']
        and final['digest'] == after['digest'] and final['digest_full'] == after['digest_full'])
reclaim = subprocess.run([ldc.PACK_MAINTAIN, 'reclaim', '--home', str(ldc.pack_root(home)), '--grace-ms', '0'],
                         capture_output=True, text=True)
pending_files = [x for x in os.listdir(ldc.pack_root(home)) if '.pending-' in x]
print(json.dumps({'ev': 'final', 'rounds': args.rounds, 'keys': len(acked), 'mismatches': len(all_bad),
                  'request_errors': len(errors), 'records': final['records'], 'pack_keeps_record_set': same,
                  'receipt_after_chain': ldc.receipt_summary(home), 'reclaim_exit': reclaim.returncode,
                  'pending_files_after_reclaim': len(pending_files)}), flush=True)
for item in errors[:3]:
    print('ERR', item, flush=True)
ldc.remove_owned(clone)
if not args.keep:
    ldc.remove_owned(home)
sys.exit(0 if not all_bad and same and not pending_files and reclaim.returncode == 0 else 1)
