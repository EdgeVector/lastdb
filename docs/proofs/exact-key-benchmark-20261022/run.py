#!/usr/bin/env python3
"""Run balanced pairs serially; never overlap measured daemons or compilation."""
import argparse
import fcntl
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output
import subprocess
import sys
import time

from cleanup import cleanup

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binaries', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=default_output('exact-key-benchmark-20261022'))
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=4)
    args = parser.parse_args()
    args.output = resolve_output(args.output)
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
                    revision = {'before': '4118c7baa12539c8c6bcf4391c497159f2a1529a',
                                'after': 'd7a5f441cc084a9624fac97d60d811c9b230702b'}[label]
                    command = [sys.executable, str(HERE / 'benchmark.py'), '--binary',
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
