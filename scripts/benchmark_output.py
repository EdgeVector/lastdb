#!/usr/bin/env python3
"""Select benchmark output outside Git and reject an in-tree override."""

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import uuid


REPO = Path(__file__).resolve().parents[1]


def resolve_output(value):
    path = Path(value).expanduser().resolve()
    if path == REPO or REPO in path.parents:
        raise SystemExit('Benchmark output must stay outside the source tree')
    ancestor = path
    while not ancestor.is_dir() and ancestor != ancestor.parent:
        ancestor = ancestor.parent
    result = subprocess.run(['git', '-C', str(ancestor), 'rev-parse', '--show-toplevel'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode == 0:
        raise SystemExit('Benchmark output must stay outside every Git worktree')
    return path


def default_output(label):
    root = os.environ.get('LAST_STACK_EVIDENCE_ROOT')
    if not root:
        if os.environ.get('RUNNER_TEMP'):
            root = str(Path(os.environ['RUNNER_TEMP']) / 'last-stack-evidence')
        else:
            state = Path(os.environ.get('XDG_STATE_HOME', str(Path.home() / '.local/state')))
            root = str(state / 'last-stack/artifacts')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    return resolve_output(Path(root) / 'lastdb' / (label + '-' + stamp + '-' + uuid.uuid4().hex[:10]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', default='benchmark')
    parser.add_argument('--check', type=Path, help='Validate an explicit output path without creating it')
    args = parser.parse_args()
    print(resolve_output(args.check) if args.check is not None else default_output(args.label))


if __name__ == '__main__':
    main()
