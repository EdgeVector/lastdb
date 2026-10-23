#!/usr/bin/env python3
"""Time `lastdb-pack-maintain compact --grace-ms 0` on a clone of a STOPPED synthetic home.

  pack_speed.py --home SNAPSHOT --bins NAME=DIR[,NAME=DIR...] [--runs N] [--order alternate]

Every run clones SNAPSHOT with `cp -c` (a copy-on-write clone of my own synthetic home), runs the
compaction of the named binary directory on the clone, and deletes the clone. Prints one JSON line
for each run: wall time, the time until CURRENT named the packed root (the time that a node would hold
its write gate for the copy), user and system CPU, peak resident memory, and the tool result.
After a run the clone is deleted. SNAPSHOT is never changed.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--home', required=True)
ap.add_argument('--bins', required=True)
ap.add_argument('--runs', type=int, default=1)
ap.add_argument('--warm', action='store_true',
                help='read every journal file of the clone before the timed pack, so that the pack meets a warm file cache')
ap.add_argument('--keep-last', action='store_true', help='keep the clone of the last run')
args = ap.parse_args()

source = ldc.refuse_primary(args.home)
bins = [item.split('=', 1) for item in args.bins.split(',')]
order = [(name, Path(path)) for _ in range(args.runs) for name, path in bins]


def tool_cpu(stderr):
    out = {}
    match = re.search(r'([\d.]+)\s+real\s+([\d.]+)\s+user\s+([\d.]+)\s+sys', stderr)
    if match:
        out['real_s'], out['user_s'], out['sys_s'] = (float(x) for x in match.groups())
    match = re.search(r'(\d+)\s+maximum resident set size', stderr)
    if match:
        out['max_rss_mb'] = round(int(match.group(1)) / 1048576, 1)
    match = re.search(r'(\d+)\s+block input operations', stderr)
    if match:
        out['block_input_ops'] = int(match.group(1))
    match = re.search(r'(\d+)\s+peak memory footprint', stderr)
    if match:
        out['peak_footprint_mb'] = round(int(match.group(1)) / 1048576, 1)
    return out


for run, (name, bindir) in enumerate(order, 1):
    clone = Path(f'{source}.speed{run}')
    ldc.refuse_primary(clone)
    subprocess.run(['cp', '-cRp', str(source), str(clone)], check=True)
    current = ldc.pack_root(clone) / 'CURRENT'
    before = current.stat().st_mtime
    warm_s = None
    if args.warm:
        # A clone starts with a cold file cache. A node packs soon after it wrote the journal, so the
        # journal files are in the cache. Reading them here (not timed) gives the same state.
        began = time.time()
        for base, _dirs, files in os.walk(ldc.pack_root(clone) / 'journal'):  # walk-ok: my own temp clone
            for file_name in files:
                with open(os.path.join(base, file_name), 'rb') as handle:
                    while handle.read(1 << 20):
                        pass
        warm_s = round(time.time() - began, 1)
    began = time.time()
    done = subprocess.run(['/usr/bin/time', '-l', str(bindir / 'lastdb-pack-maintain'), 'compact',
                           '--home', str(ldc.pack_root(clone)), '--grace-ms', '0'],
                          capture_output=True, text=True)
    wall = time.time() - began
    published = current.stat().st_mtime - began
    try:
        result = json.loads(done.stdout.strip().splitlines()[-1])['result']
    except Exception:
        result = {'raw': done.stdout[-300:], 'stderr': done.stderr[-300:]}
    row = {'bin': name, 'run': run, 'warm': args.warm, 'warm_read_s': warm_s, 'exit': done.returncode, 'wall_s': round(wall, 1),
           'published_after_s': round(published, 1) if current.stat().st_mtime > before else None,
           **tool_cpu(done.stderr), 'result': result}
    print(json.dumps(row), flush=True)
    if not (args.keep_last and run == len(order)):
        ldc.remove_owned(clone)
    else:
        print(json.dumps({'kept': str(clone)}), flush=True)
