#!/usr/bin/env python3
"""Run balanced pairs serially; never overlap measured daemons or compilation."""
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
    parser.add_argument('--rounds', type=int, default=4)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    args.scratch.mkdir(mode=0o700, parents=True, exist_ok=False)
    index = []
    lock_path = Path.home() / '.cache/fold-db-perf-guard/host-cargo.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for config in ('plain', 'encrypted', 'preload'):
            for pair in range(args.rounds):
                order = ('before', 'after') if pair % 2 == 0 else ('after', 'before')
                for label in order:
                    name = f'{config}-{pair}-{label}'
                    output = args.output / name
                    scratch = args.scratch / name
                    revision = {'before': 'a02d25ed0337f9fe6ce01d7b6d68a71d547f2e72',
                                'after': 'e5e3a413dbd39035cb615de2f0c6e2b5394660d5'}[label]
                    command = [sys.executable, str(HERE / 'paging.py'), '--binary',
                               str(args.binaries / label), '--scratch', str(scratch),
                               '--output', str(output), '--source-revision', revision,
                               '--config', config]
                    started = time.time()
                    print(f'Starting {name}', flush=True)
                    subprocess.run(command, check=True)
                    cleanup(scratch.resolve(), output.resolve())
                    index.append(dict(name=name, pair=pair, label=label, config=config,
                                      source_revision=revision, seconds=time.time() - started))
                    (args.output / 'index.json').write_text(json.dumps(index, indent=2) + '\n')
    print('All paired measurements complete', flush=True)


if __name__ == '__main__':
    main()
