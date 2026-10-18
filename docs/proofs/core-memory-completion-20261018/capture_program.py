#!/usr/bin/env python3
"""Preserve the previous executable in a new owned synthetic container."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
EXPECTED = 'bdb83b38545a2161217392f19f9e6f6c2e0fd2d35f8f449e2d6221cb29f2cbed'


def file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    source = ROOT / 'target/release/lastdbd'
    if file_hash(source) != EXPECTED:
        raise RuntimeError('The previous executable no longer has the expected identity')
    destination = Path(tempfile.mkdtemp(prefix='sp.')).resolve()
    info = destination.stat()
    descriptor = os.open(destination / '.synthetic-seed-owner.json',
                         os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    marker = {'version': 1, 'kind': 'synthetic-trial', 'owner_uid': os.getuid(),
              'device': info.st_dev, 'inode': info.st_ino, 'creator_pid': os.getpid()}
    with os.fdopen(descriptor, 'w') as output:
        json.dump(marker, output, sort_keys=True)
        output.write('\n')
    program = destination / 'program-before-lastdbd'
    shutil.copy2(source, program)
    actual = file_hash(program)
    if actual != EXPECTED or file_hash(source) != EXPECTED:
        raise RuntimeError('The executable changes during its private copy')
    old_source = HERE.parent / 'core-memory-state-20261018/current-source-v4.json'
    saved = {'root': str(destination), 'binary': str(program), 'binary_sha256': actual,
        'node_sessions': 0, 'source_binary': str(source), 'marker': marker,
        'source_identity': str(old_source), 'source_identity_sha256': file_hash(old_source),
        'scope': 'Previous reviewed executable only. No home copy and no node start.'}
    with (HERE / 'baseline-root.json').open('x') as output:
        json.dump(saved, output, sort_keys=True, indent=2)
        output.write('\n')
    print(json.dumps(saved), flush=True)


if __name__ == '__main__':
    main()
