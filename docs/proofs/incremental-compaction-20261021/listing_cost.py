#!/usr/bin/env python3
"""Cost of many part files in one object directory.

  listing_cost.py [--counts 1000,10000,65536,100000]

The one-object change puts every locator part of a home into one directory. A receipt names at most 65,536
parts, and the sweep and the reclaim list directories. This script makes N empty files named like parts
(`part-<8 hex digits>`, random numbers, as `SparsePages` picks them) in one fresh directory under $TMPDIR and
times: the creation of one more file, a full listing with the file type of each entry (what the orphan sweep
does), and the same listing split into 32 directories (the old shape). It prints one JSON line for each N.
"""
import argparse
import json
import os
import random
import tempfile
import time
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument('--counts', default='1000,10000,65536,100000')
args = ap.parse_args()

tmp = os.environ.get('TMPDIR') or '/tmp'


def make(directory, count, rng):
    os.mkdir(directory)
    names = set()
    while len(names) < count:
        names.add('part-%08x' % rng.getrandbits(32))
    for name in names:
        os.close(os.open(os.path.join(directory, name), os.O_CREAT | os.O_EXCL | os.O_WRONLY))
    return names


def list_dir(directory):
    count = 0
    began = time.perf_counter()
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.is_file(follow_symlinks=False) and entry.name.startswith('part-'):
                count += 1
    return count, time.perf_counter() - began


def best_of(fn, runs=5):
    return min(fn() for _ in range(runs))


for count in (int(x) for x in args.counts.split(',')):
    rng = random.Random(count)
    root = tempfile.mkdtemp(prefix='s1s2.listing.', dir=tmp)
    try:
        one = os.path.join(root, 'one')
        make(one, count, rng)
        many_root = os.path.join(root, 'many')
        os.mkdir(many_root)
        per = count // 32
        for i in range(32):
            make(os.path.join(many_root, 'obj%02d' % i), per, rng)
        began = time.perf_counter()
        fd = os.open(os.path.join(one, 'part-ffffffff'), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        create_s = time.perf_counter() - began
        os.close(fd)
        os.unlink(os.path.join(one, 'part-ffffffff'))
        one_s = best_of(lambda: list_dir(one)[1])

        def many():
            total = 0.0
            for i in range(32):
                total += list_dir(os.path.join(many_root, 'obj%02d' % i))[1]
            return total
        many_s = best_of(many)
        print(json.dumps({'files': count, 'list_one_dir_ms': round(one_s * 1000, 1),
                          'list_32_dirs_ms': round(many_s * 1000, 1),
                          'create_one_more_file_ms': round(create_s * 1000, 3)}), flush=True)
    finally:
        # Delete only the directory that this run made under $TMPDIR.
        real = Path(os.path.realpath(root))
        if real.parent == Path(os.path.realpath(tmp)) and real.name.startswith('s1s2.listing.'):
            import shutil
            shutil.rmtree(real)
