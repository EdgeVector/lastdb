#!/usr/bin/env python3
"""Record oracle across a pack: the multiset of records must not change.

  oracle_pack.py --home SNAPSHOT --oracle NAME=DIR --pack NAME=DIR[,NAME=DIR...] [--reader NAME=DIR,...]

SNAPSHOT is a stopped synthetic home that is never changed. The script runs `oracle.py` on SNAPSHOT with the
`--oracle` binaries (the independent reader: use the binaries of main, so the reader does not share the code
under test). Then, for each `--pack` binary, it clones SNAPSHOT with `cp -c`, packs the clone with that binary,
reads the clone again with every `--oracle` and `--reader` binary, and compares the count, the count of each
collection and both digests with the values from before the pack. It also checks that the clone has fewer
parts. It prints one JSON line for each comparison and exits non-zero when any comparison differs.
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

here = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument('--home', required=True)
ap.add_argument('--oracle', required=True, help='NAME=DIR of the independent reader')
ap.add_argument('--pack', required=True)
ap.add_argument('--reader', default='', help='more readers, NAME=DIR,NAME=DIR')
ap.add_argument('--keep', action='store_true', help='keep the packed clones')
ap.add_argument('--expected', default='', help='model file of crafted_home.py: also read every key back (crafted_verify.py)')
ap.add_argument('--twice', action='store_true', help='pack the packed clone a second time and compare again')
args = ap.parse_args()


def parse(spec):
    return [tuple(item.split('=', 1)) for item in spec.split(',') if item]


def verify_keys(home, bindir):
    """Every key of the crafted model reads back as the model says (tombstones too)."""
    if not args.expected:
        return True
    done = subprocess.run([sys.executable, str(here / 'crafted_verify.py'), str(home), args.expected],
                          env=dict(os.environ, LDBIN=str(bindir)), capture_output=True, text=True)
    print(json.dumps({'ev': 'crafted-verify', 'exit': done.returncode, 'out': done.stdout.strip()[-200:]}), flush=True)
    return done.returncode == 0


def oracle(home, bindir):
    env = dict(os.environ, LDBIN=str(bindir))
    done = subprocess.run([sys.executable, str(here / 'oracle.py'), str(home)], env=env,
                          capture_output=True, text=True)
    row = json.loads(done.stdout.strip().splitlines()[-1])
    row['ok'] = done.returncode == 0
    return row


source = ldc.refuse_primary(args.home)
readers = parse(args.oracle) + parse(args.reader)
began = time.time()
before = oracle(source, readers[0][1])
print(json.dumps({'ev': 'before', 'reader': readers[0][0], 'records': before['records'],
                  'digest': before['digest'], 'seconds': round(time.time() - began)}), flush=True)
failed = not before['ok'] or not verify_keys(source, readers[0][1])
parts_before = ldc.receipt_summary(source)['parts']
for run, (name, bindir) in enumerate(parse(args.pack), 1):
    clone = Path(f'{source}.oracle{run}')
    ldc.refuse_primary(clone)
    subprocess.run(['cp', '-cRp', str(source), str(clone)], check=True)
    started = time.time()
    done = subprocess.run([str(Path(bindir) / 'lastdb-pack-maintain'), 'compact', '--home',
                           str(ldc.pack_root(clone)), '--grace-ms', '0'], capture_output=True, text=True)
    pack_s = round(time.time() - started, 1)
    if done.returncode != 0:
        print(json.dumps({'ev': 'pack-failed', 'pack': name, 'stderr': done.stderr[-300:]}), flush=True)
        failed = True
        ldc.remove_owned(clone)
        continue
    after_parts = ldc.receipt_summary(clone)['parts']
    for reader, readdir in readers:
        after = oracle(clone, readdir)
        same = (after['ok'] and after['records'] == before['records']
                and after['by_collection'] == before['by_collection']
                and after['digest'] == before['digest'] and after['digest_full'] == before['digest_full'])
        failed = failed or not same
        print(json.dumps({'ev': 'after', 'pack': name, 'reader': reader, 'equal': same, 'records': after['records'],
                          'digest': after['digest'], 'digest_full': after['digest_full'],
                          'pack_s': pack_s, 'parts_before': parts_before, 'parts_after': after_parts}), flush=True)
    failed = failed or not verify_keys(clone, readers[0][1])
    if args.twice:
        # The second pack reads packed sources in key order, the shape that shares blocks.
        done = subprocess.run([str(Path(bindir) / 'lastdb-pack-maintain'), 'compact', '--home',
                               str(ldc.pack_root(clone)), '--grace-ms', '0'], capture_output=True, text=True)
        if done.returncode != 0:
            print(json.dumps({'ev': 'second-pack-failed', 'pack': name, 'stderr': done.stderr[-300:]}), flush=True)
            failed = True
        for reader, readdir in readers:
            after = oracle(clone, readdir)
            same = (done.returncode == 0 and after['ok'] and after['records'] == before['records']
                    and after['by_collection'] == before['by_collection']
                    and after['digest'] == before['digest'] and after['digest_full'] == before['digest_full'])
            failed = failed or not same
            print(json.dumps({'ev': 'after-second-pack', 'pack': name, 'reader': reader, 'equal': same,
                              'records': after['records'], 'digest': after['digest']}), flush=True)
        failed = failed or not verify_keys(clone, readers[0][1])
    if args.keep:
        print(json.dumps({'kept': str(clone)}), flush=True)
    else:
        ldc.remove_owned(clone)
print(json.dumps({'ev': 'result', 'all_equal': not failed}), flush=True)
sys.exit(1 if failed else 0)
