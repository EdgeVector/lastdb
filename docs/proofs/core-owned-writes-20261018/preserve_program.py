#!/usr/bin/env python3
"""Preserve one verified candidate executable in a fresh owned private container."""
import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

from capture_before import digest, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    identity = json.loads(args.source.read_text())
    if (identity.get('state') != 'release_verified' or identity['binary_sha256'] != args.sha256
            or digest(args.binary) != args.sha256 or args.output.exists()):
        raise RuntimeError('The candidate identity differs or the output already exists')
    root = Path(tempfile.mkdtemp(prefix='sp.')).resolve()
    info = root.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o777 != 0o700:
        raise RuntimeError('The private program root lacks exclusive ownership')
    marker = {'version': 1, 'kind': 'synthetic-trial', 'owner_uid': os.getuid(),
        'device': info.st_dev, 'inode': info.st_ino, 'creator_pid': os.getpid()}
    descriptor = os.open(root / '.synthetic-seed-owner.json',
                         os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(marker, stream, sort_keys=True)
        stream.write('\n')
    private = root / 'program-candidate-lastdbd'
    shutil.copy2(args.binary, private)
    if digest(private) != args.sha256 or digest(args.binary) != args.sha256:
        raise RuntimeError('The candidate executable changes during its private copy')
    value = {'root': str(root), 'binary': str(private), 'binary_sha256': args.sha256,
        'node_sessions': 0, 'source_binary': str(args.binary.resolve()), 'marker': marker,
        'source_identity': str(args.source.resolve()), 'source_identity_sha256': digest(args.source),
        'scope': 'Candidate executable only. No existing database, primary, archive, or node.'}
    save(args.output, value)
    print(json.dumps(value), flush=True)


if __name__ == '__main__':
    main()
