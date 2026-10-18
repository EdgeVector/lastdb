#!/usr/bin/env python3
"""Preserve exact ownership checks and preview supported synthetic-root cleanup."""
import argparse
import datetime
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    parser.add_argument('--execute-reviewed-preview', action='store_true')
    args = parser.parse_args()
    directory = args.receipt_dir.resolve(strict=True)
    retained = json.loads((directory / 'retained-root.json').read_text())
    root = Path(retained['root'])
    if args.execute_reviewed_preview:
        reviewed = json.loads((directory / 'cleanup-preview.json').read_text())
        prior = json.loads((directory / 'cleanup-preflight.json').read_text())
        if (reviewed.get('exit_code') != 0 or prior.get('root') != str(root)
                or reviewed.get('root_exists_after_preview') is not True
                or reviewed.get('command') != [str(Path.home() / '.local/bin/lastdb-dev'),
                    'reclaim', '--home', str(root), '--exact-home', '--dry-run']):
            raise RuntimeError('The reviewed preview does not match this exact root')
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise RuntimeError('The exact synthetic root differs from its real path')
    if root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith('sp.'):
        raise RuntimeError('The exact root is outside this measurement namespace')
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
        raise RuntimeError('The exact root ownership marker differs')
    if args.execute_reviewed_preview and (prior.get('device') != info.st_dev
            or prior.get('inode') != info.st_ino or prior.get('marker') != marker):
        raise RuntimeError('The current root identity differs from the reviewed root')
    rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
    starts = [row for row in rows if row.get('operation') == 'owned-node-start']
    stops = [row for row in rows if row.get('operation') == 'owned-node-stop']
    pids = sorted({row['pid'] for row in starts})
    stopped = {row['pid'] for row in stops if row.get('forced_kill') is False
               and row.get('exit_code') in (0, -15)}
    processes = capture(['ps', '-p', ','.join(map(str, pids)), '-o', 'pid=,command='])
    files = capture(['lsof', '-Fpn', '+D', str(root)])
    directories = {Path(row['home']).parent for row in starts}
    if any(path.parent != root for path in directories):
        raise RuntimeError('A recorded node directory escapes the exact owned root')
    sockets = [str(path / name) for path in sorted(directories)
               for name in ('q.sock', 'f.sock') if (path / name).exists()]
    preflight = {'checked_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'root': str(root), 'realpath': str(root.resolve()), 'owner_uid': info.st_uid,
        'current_uid': os.getuid(), 'mode': oct(stat.S_IMODE(info.st_mode)),
        'device': info.st_dev, 'inode': info.st_ino, 'marker': marker,
        'marker_mode': oct(stat.S_IMODE(marker_info.st_mode)), 'owned_node_pids': pids,
        'normal_stops': stops, 'process_check': processes, 'open_file_check': files,
        'known_socket_paths_remaining': sockets}
    save(directory / ('cleanup-execute-preflight.json' if args.execute_reviewed_preview
                      else 'cleanup-preflight.json'), preflight)
    if (not pids or set(pids) != stopped or processes['exit_code'] != 1
            or processes['stdout'].strip() or processes['stderr'].strip()
            or files['exit_code'] != 1 or files['stdout'].strip() or files['stderr'].strip()
            or sockets or retained.get('all_owned_nodes_stopped') is not True):
        raise RuntimeError('The cleanup preflight does not establish a released root')
    command = [str(Path.home() / '.local/bin/lastdb-dev'), 'reclaim', '--home',
               str(root), '--exact-home']
    if args.execute_reviewed_preview:
        outcome = capture(command)
        outcome.update({'root_exists_after': root.exists(), 'root': str(root),
                        'result': 'supported exact reclaim; no retry'})
        save(directory / 'cleanup.json', outcome)
        print(json.dumps({'receipt_dir': str(directory), 'root': str(root),
                          'cleanup_exit': outcome['exit_code'], 'retained': root.exists()}))
        return outcome['exit_code']
    preview = capture(command + ['--dry-run'])
    preview.update({'root_exists_after_preview': root.exists(),
                    'result': 'preview only; scratch retained'})
    save(directory / 'cleanup-preview.json', preview)
    print(json.dumps({'receipt_dir': str(directory), 'root': str(root),
                      'preview_exit': preview['exit_code'], 'retained': root.exists()}))
    return preview['exit_code']


if __name__ == '__main__':
    raise SystemExit(main())
