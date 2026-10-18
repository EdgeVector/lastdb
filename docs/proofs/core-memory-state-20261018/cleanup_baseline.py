#!/usr/bin/env python3
"""Preview or reclaim only this task's released executable container."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent


def capture(command):
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return {'command': command, 'exit_code': result.returncode,
            'stdout': result.stdout, 'stderr': result.stderr}


def save(path, value):
    with path.open('x') as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write('\n')


def file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute-reviewed-preview', action='store_true')
    args = parser.parse_args()
    saved = json.loads((HERE / 'baseline-root.json').read_text())
    root = Path(saved['root'])
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise RuntimeError('The exact root differs from its real path')
    if root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith('sp.'):
        raise RuntimeError('The exact root is outside the owned measurement namespace')
    info = root.stat()
    marker_path = root / '.synthetic-seed-owner.json'
    marker_info = marker_path.lstat()
    marker = json.loads(marker_path.read_text())
    if (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or not stat.S_ISREG(marker_info.st_mode) or marker_info.st_nlink != 1
            or stat.S_IMODE(marker_info.st_mode) != 0o600
            or marker_info.st_uid != os.getuid() or marker.get('kind') != 'synthetic-trial'
            or marker.get('owner_uid') != os.getuid() or marker.get('device') != info.st_dev
            or marker.get('inode') != info.st_ino):
        raise RuntimeError('The exact container ownership identity differs')
    program = Path(saved['binary'])
    if (program.parent != root or program.is_symlink() or file_hash(program) != saved['binary_sha256']
            or set(root.iterdir()) != {marker_path, program} or saved['node_sessions'] != 0):
        raise RuntimeError('The container contents differ from the preserved executable')
    files = capture(['lsof', '-Fpn', '+D', str(root)])
    preflight = {'checked_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'root': str(root), 'realpath': str(root.resolve()), 'owner_uid': info.st_uid,
        'device': info.st_dev, 'inode': info.st_ino, 'marker': marker,
        'binary': str(program), 'binary_sha256': file_hash(program), 'open_file_check': files}
    save(HERE / ('baseline-cleanup-execute-preflight.json' if args.execute_reviewed_preview
                 else 'baseline-cleanup-preflight.json'), preflight)
    if files['exit_code'] != 1 or files['stdout'].strip() or files['stderr'].strip():
        raise RuntimeError('The container still has an open file or the check failed')
    command = [str(Path.home() / '.local/bin/lastdb-dev'), 'reclaim', '--home',
               str(root), '--exact-home']
    if args.execute_reviewed_preview:
        prior = json.loads((HERE / 'baseline-cleanup-preview.json').read_text())
        reviewed = json.loads((HERE / 'baseline-cleanup-preflight.json').read_text())
        if (prior.get('exit_code') != 0 or prior.get('command') != command + ['--dry-run']
                or prior.get('root_exists_after_preview') is not True
                or any(reviewed[name] != preflight[name] for name in ('root', 'device', 'inode', 'marker'))):
            raise RuntimeError('The current root does not match its reviewed supported preview')
        result = capture(command)
        result.update(root=str(root), root_exists_after=root.exists())
        save(HERE / 'baseline-cleanup.json', result)
    else:
        result = capture(command + ['--dry-run'])
        result.update(root_exists_after_preview=root.exists())
        save(HERE / 'baseline-cleanup-preview.json', result)
    print(json.dumps({'root': str(root), 'execute': args.execute_reviewed_preview,
                      'exit_code': result['exit_code'], 'retained': root.exists()}))
    return result['exit_code']


if __name__ == '__main__':
    raise SystemExit(main())
