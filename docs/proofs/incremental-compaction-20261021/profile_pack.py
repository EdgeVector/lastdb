#!/usr/bin/env python3
"""Profile the pack of a clone of a STOPPED synthetic home with /usr/bin/sample.

  profile_pack.py --home SNAPSHOT --bin DIR --out FILE [--after 8] [--seconds 20]

Clones SNAPSHOT with `cp -c`, starts `lastdb-pack-maintain compact` from DIR, samples the process after
`--after` seconds for `--seconds` seconds, writes the "top of stack" table to FILE and prints its first
rows, then deletes the clone. A sample counts one thread stack each millisecond.
"""
import argparse
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument('--home', required=True)
ap.add_argument('--bin', required=True)
ap.add_argument('--out', required=True)
ap.add_argument('--after', type=float, default=8)
ap.add_argument('--seconds', type=int, default=20)
args = ap.parse_args()

source = ldc.refuse_primary(args.home)
clone = Path(f'{source}.profile')
ldc.refuse_primary(clone)
subprocess.run(['cp', '-cRp', str(source), str(clone)], check=True)
proc = subprocess.Popen([str(Path(args.bin) / 'lastdb-pack-maintain'), 'compact', '--home',
                         str(ldc.pack_root(clone)), '--grace-ms', '0'],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(args.after)
raw = tempfile.mktemp(prefix='s1s2.sample.', dir=os.environ.get('TMPDIR') or '/tmp')
subprocess.run(['/usr/bin/sample', str(proc.pid), str(args.seconds), '-file', raw],
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
alive = proc.poll() is None
if alive:
    proc.wait()
text = Path(raw).read_text(errors='replace') if Path(raw).exists() else ''
match = re.search(r'Sort by top of stack, same collapsed.*?\n(.*?)\n\nBinary Images', text, re.S)
table = match.group(1) if match else text[-2000:]
rows = [line.strip() for line in table.splitlines() if line.strip()]
Path(args.out).write_text('\n'.join(rows[:40]) + '\n')
print('process still running at the end of the sample:', alive)
print('\n'.join(rows[:25]))
Path(raw).unlink(missing_ok=True)
ldc.remove_owned(clone)
