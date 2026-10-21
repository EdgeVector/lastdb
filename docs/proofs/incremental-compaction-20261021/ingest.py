#!/usr/bin/env python3
"""Build a never-packed synthetic home with real writes through a running node.

  LDBIN=<bin dir> ingest.py --stages 14000,140000,200000 [--ops 500] [--clients 8] [--buckets 256]

Each logical operation is one default-acknowledgement `update` of 100 bytes. A logical operation
makes about 7 physical records. Automatic maintenance is off, so the home is never packed.
The node writes in requests of `--ops` operations from `--clients` threads over `--buckets` hash groups.
After each stage the node stops cleanly, the home is cloned with `cp -c` into `<home>.stage<N>`
(a copy-on-write clone of my own synthetic home) and the next stage continues on the live home.
Prints one JSON line for each stage.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--stages', required=True, help='cumulative logical operation counts, comma separated')
ap.add_argument('--ops', type=int, default=500)
ap.add_argument('--clients', type=int, default=8)
ap.add_argument('--buckets', type=int, default=256)
ap.add_argument('--home', default='')
ap.add_argument('--tag', default='ingest')
args = ap.parse_args()

stages = [int(x) for x in args.stages.split(',')]
H = ldc.refuse_primary(args.home) if args.home else ldc.new_home(args.tag)
env = {'LASTDB_AUTO_MAINTENANCE': '0'}
done = [0]
lock = threading.Lock()


def ingest(node, upto):
    def worker():
        while True:
            with lock:
                if done[0] >= upto:
                    return
                base = done[0]
                done[0] += args.ops
            muts = [ldc.put('Cap', 'b%03d' % ((base + j) % args.buckets), 'r%09d' % (base + j),
                            'v' * 100, durable=False) for j in range(args.ops)]
            for _ in range(30):
                try:
                    ldc.mutate(node, muts)
                    break
                except Exception:
                    time.sleep(1)
            else:
                raise SystemExit('a request kept failing')
    threads = [threading.Thread(target=worker) for _ in range(args.clients)]
    [t.start() for t in threads]
    [t.join() for t in threads]


print(json.dumps({'ev': 'home', 'home': str(H)}), flush=True)
node = ldc.Node(H, env)
node.start()
if not args.home:
    ldc.seed(node, 'Cap')
for stage in stages:
    began = time.time()
    ingest(node, stage)
    time.sleep(2)
    seconds = time.time() - began
    node.stop()
    summary = ldc.receipt_summary(H)
    size_kb = int(subprocess.run(['du', '-sk', str(H)], capture_output=True, text=True).stdout.split()[0])
    clone = f'{H}.stage{stage}'
    subprocess.run(['cp', '-cRp', str(H), clone], check=True)
    print(json.dumps({'ev': 'stage', 'logical_ops': stage, 'ingest_s': round(seconds), 'home_MB': size_kb // 1024,
                      'snapshot': clone, **summary}), flush=True)
    node = ldc.Node(H, env)
    node.start()
node.stop()
print(json.dumps({'ev': 'done', 'home': str(H)}), flush=True)
