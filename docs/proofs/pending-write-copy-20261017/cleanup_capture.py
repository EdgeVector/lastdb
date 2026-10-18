#!/usr/bin/env python3
"""Preview or execute reviewed exact cleanup for a node-free program capture."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile


def capture(command):
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return {'command': [str(part) for part in command], 'exit_code': result.returncode,
            'stdout': result.stdout, 'stderr': result.stderr}


def save(path, value):
    with path.open('x') as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write('\n')


def file_hash(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--execute-reviewed-preview', action='store_true')
    args = parser.parse_args()
    directory = args.receipt_dir.resolve(strict=True)
    retained = json.loads((directory / 'private-binary.json').read_text())
    root = Path(retained['root'])
    binary = Path(retained['private_binary'])
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise RuntimeError('The exact capture root differs from its real path')
    if root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith('sp.'):
        raise RuntimeError('The exact capture root is outside the owned namespace')
    info = root.stat()
    marker_path = root / '.synthetic-seed-owner.json'
    marker_info = marker_path.lstat()
    marker = json.loads(marker_path.read_text())
    if (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or not stat.S_ISREG(marker_info.st_mode) or marker_info.st_nlink != 1
            or stat.S_IMODE(marker_info.st_mode) != 0o600
            or marker_info.st_uid != os.getuid() or marker.get('kind') != 'synthetic-trial'
            or marker.get('owner_uid') != os.getuid() or marker.get('device') != info.st_dev
            or marker.get('inode') != info.st_ino or retained.get('node_started') is not False
            or retained.get('owner_uid') != os.getuid() or binary != root / 'program-lastdbd'
            or binary.is_symlink() or file_hash(binary) != retained['sha256']):
        raise RuntimeError('The exact capture ownership or executable differs')
    names = sorted(path.name for path in root.iterdir())
    allowed = {'.synthetic-seed-owner.json', 'inputs.json', 'summary.json', 'program-lastdbd'}
    if not set(names) <= allowed or any(path.is_dir() for path in root.iterdir()):
        raise RuntimeError('The node-free capture contains unexpected entries')
    files = capture(['lsof', '-Fpn', '+D', str(root)])
    preflight = {'checked_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'root': str(root), 'realpath': str(root.resolve()), 'device': info.st_dev,
        'inode': info.st_ino, 'owner_uid': info.st_uid, 'mode': oct(stat.S_IMODE(info.st_mode)),
        'marker': marker, 'marker_mode': oct(stat.S_IMODE(marker_info.st_mode)),
        'node_started': False, 'immediate_entries': names, 'open_file_check': files,
        'binary_sha256': retained['sha256']}
    command = [str(Path.home() / '.local/bin/lastdb-dev'), 'reclaim', '--home', str(root), '--exact-home']
    if args.execute_reviewed_preview:
        reviewed = json.loads((directory / 'cleanup-preview.json').read_text())
        prior = json.loads((directory / 'cleanup-preflight.json').read_text())
        if (reviewed.get('exit_code') != 0 or reviewed.get('root_exists_after_preview') is not True
                or reviewed.get('command') != command + ['--dry-run']
                or prior.get('root') != str(root) or prior.get('device') != info.st_dev
                or prior.get('inode') != info.st_ino or prior.get('marker') != marker):
            raise RuntimeError('The exact capture differs from the reviewed preview')
    save(directory / ('cleanup-execute-preflight.json' if args.execute_reviewed_preview
                      else 'cleanup-preflight.json'), preflight)
    if files['exit_code'] != 1 or files['stdout'].strip() or files['stderr'].strip():
        raise RuntimeError('The capture still has an open file')
    outcome = capture(command if args.execute_reviewed_preview else command + ['--dry-run'])
    if args.execute_reviewed_preview:
        outcome.update(root=str(root), root_exists_after=root.exists())
        save(directory / 'cleanup.json', outcome)
    else:
        outcome.update(root_exists_after_preview=root.exists())
        save(directory / 'cleanup-preview.json', outcome)
    print(json.dumps({'receipt_dir': str(directory), 'root': str(root),
                      'cleanup_exit': outcome['exit_code'], 'retained': root.exists()}))
    return outcome['exit_code']


if __name__ == '__main__':
    raise SystemExit(main())
