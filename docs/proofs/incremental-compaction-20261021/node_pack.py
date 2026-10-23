#!/usr/bin/env python3
"""Gate hold of the automatic pack inside a running node, with writers.

  LDBIN=<bin dir> node_pack.py --home SNAPSHOT [--writers 4] [--ops 100] [--timeout 1800]

Clones SNAPSHOT (a stopped synthetic home, never changed) with `cp -c`, starts a node on the clone with
automatic maintenance on (the first run starts 5 seconds after the start), starts the writers (default
acknowledgement, new keys, `--ops` operations for each request), and polls the `Storage parts` line of
`lastdb status` until the first run ends. Prints one JSON line:
  run_ms                   the node's own time for the run (pack and its first slice of reclaim)
  pack_millis_logged       the pack time that the node logs for the run (the gate is held for it)
  longest_gap_s            the longest time between two completed writes while the run was in progress
  failed                   requests that did not return 200
The writers stay on after the run, for `--tail` seconds, to see that writes resume.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--home', required=True)
ap.add_argument('--writers', type=int, default=4)
ap.add_argument('--ops', type=int, default=100)
ap.add_argument('--timeout', type=int, default=1800)
ap.add_argument('--tail', type=int, default=20)
args = ap.parse_args()

source = ldc.refuse_primary(args.home)
clone = ldc.clone_path('np')
subprocess.run(['cp', '-cRp', str(source), str(clone)], check=True)
node = ldc.Node(clone, {'LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS': '5'})
began = time.time()
node.start(timeout=3600)
ready_s = time.time() - began
t0 = time.time()
stop = threading.Event()
done = []        # (end time, duration, status)
lock = threading.Lock()
counter = [0]


def writer():
    while not stop.is_set():
        with lock:
            counter[0] += args.ops
            base = counter[0]
        muts = [ldc.put('Cap', 'b%03d' % ((base + j) % 256), 'x%09d' % (900000000 + base + j), 'v' * 100,
                        durable=False) for j in range(args.ops)]
        started = time.time()
        try:
            status, _, _ = node.call('/api/mutations/batch', {'mutations': muts}, raw=True)
        except Exception:
            status = -1
        with lock:
            done.append((time.time(), time.time() - started, status))


def storage_line():
    try:
        for line in node.cli('status', timeout=300).splitlines():
            if line.startswith('Storage parts'):
                return line
    except Exception:
        pass
    return ''


threads = [threading.Thread(target=writer, daemon=True) for _ in range(args.writers)]
[t.start() for t in threads]
first_running = None
finished = None
line = ''
while time.time() - t0 < args.timeout:
    line = storage_line()
    runs = re.search(r'runs (\d+)', line)
    if first_running is None and ('maintenance running' in line or (runs and int(runs.group(1)) > 0)):
        first_running = time.time()
    if runs and int(runs.group(1)) > 0 and 'maintenance running' not in line:
        finished = time.time()
        break
    time.sleep(3)
time.sleep(args.tail)
stop.set()
[t.join(timeout=150) for t in threads]
with lock:
    rows = sorted(done)
ok = [r for r in rows if r[2] == 200]
window_start = first_running or t0
window_end = finished or time.time()
gaps = []
prev = window_start
for end, _, _ in ok:
    if end >= window_start:
        gaps.append(end - prev)
        prev = end
        if end > window_end:
            break
run_ms = re.search(r'parts in (\d+) ms', line)
# The node logs the pack and reclaim time of each automatic run (tracing fields, with colour codes).
log = re.sub(r'\x1b\[[0-9;]*m', '', (clone / 'node.log').read_text(errors='replace'))
logged = [l for l in log.splitlines() if 'reached the dependency high-water mark' in l]
pack_millis = re.search(r'pack_millis=(\d+)', logged[0]) if logged else None
reclaim_millis = re.search(r'reclaim_millis=(\d+)', logged[0]) if logged else None
print(json.dumps({'ready_s': round(ready_s, 1), 'writers': args.writers, 'ops': args.ops,
                  'run_started_after_s': round(window_start - t0, 1) if first_running else None,
                  'run_ms': int(run_ms.group(1)) if run_ms else None,
                  'pack_millis_logged': int(pack_millis.group(1)) if pack_millis else None,
                  'reclaim_millis_logged': int(reclaim_millis.group(1)) if reclaim_millis else None,
                  'writes_ok_total': len(ok), 'failed': len(rows) - len(ok),
                  'longest_gap_s': round(max(gaps), 1) if gaps else None,
                  'status_line': line[:300]}), flush=True)
node.stop()
ldc.remove_owned(clone)
