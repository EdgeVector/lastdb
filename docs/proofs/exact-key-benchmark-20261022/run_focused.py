#!/usr/bin/env python3
"""Run four balanced pairs of the sustained encrypted-write follow-up."""
import argparse
import fcntl
import json
from pathlib import Path
import subprocess
import sys
import time

from cleanup import cleanup

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binaries', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    args.scratch.mkdir(mode=0o700, parents=True, exist_ok=False)
    index = []
    lock_path = Path.home() / '.cache/fold-db-perf-guard/host-cargo.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for pair in range(4):
            order = ('before', 'after') if pair % 2 == 0 else ('after', 'before')
            for label in order:
                name = f'encrypted-{pair}-{label}'
                revision = {'before': '4118c7baa12539c8c6bcf4391c497159f2a1529a',
                            'after': 'd7a5f441cc084a9624fac97d60d811c9b230702b'}[label]
                command = [sys.executable, str(HERE / 'focused.py'), '--binary',
                           str(args.binaries / label), '--scratch', str(args.scratch / name),
                           '--output', str(args.output / name), '--source-revision', revision]
                started = time.time()
                print(f'Starting focused {name}', flush=True)
                subprocess.run(command, check=True)
                cleanup((args.scratch / name).resolve(), (args.output / name).resolve())
                index.append(dict(name=name, pair=pair, label=label, config='encrypted',
                                  source_revision=revision, seconds=time.time() - started))
                (args.output / 'index.json').write_text(json.dumps(index, indent=2) + '\n')


if __name__ == '__main__':
    main()
