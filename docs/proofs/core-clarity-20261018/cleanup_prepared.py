#!/usr/bin/env python3
"""Review only the exact never-started response preparation root."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DIRECTORY = HERE / 'accepted-response'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def capture(command):
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return {'command': command, 'exit_code': result.returncode,
            'stdout': result.stdout, 'stderr': result.stderr}


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute-reviewed-preview', action='store_true')
    args = parser.parse_args()
    retained = json.loads((DIRECTORY / 'retained-root.json').read_text())
    root = Path(retained['root'])
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith('sp.')):
        raise RuntimeError('The preparation root differs from its exact temporary path')
    info = root.stat()
    marker_path = root / '.synthetic-seed-owner.json'
    marker_info, marker = marker_path.lstat(), json.loads(marker_path.read_text())
    if (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or not stat.S_ISREG(marker_info.st_mode) or marker_info.st_nlink != 1
            or stat.S_IMODE(marker_info.st_mode) != 0o600 or marker_info.st_uid != os.getuid()
            or marker.get('kind') != 'synthetic-trial' or marker.get('owner_uid') != os.getuid()
            or marker.get('device') != info.st_dev or marker.get('inode') != info.st_ino):
        raise RuntimeError('The preparation root ownership identity differs')
    expected = {marker_path, *(root / name for name in
                ('program-lastdbd', 'inputs.json', 'receipts.jsonl', 'summary.json'))}
    if set(root.iterdir()) != expected or any(not path.is_file() or path.is_symlink() for path in expected):
        raise RuntimeError('The never-started preparation root contains unexpected data')
    rows = [json.loads(line) for line in (DIRECTORY / 'receipts.jsonl').read_text().splitlines()]
    if len(rows) != 1 or rows[0].get('operation') != 'failure' or 'TypeError: remember()' not in rows[0]['error']:
        raise RuntimeError('The retained receipt is not the known pre-node preparation failure')
    if any(digest(root / name) != digest(DIRECTORY / name) for name in
           ('inputs.json', 'receipts.jsonl', 'summary.json', '.synthetic-seed-owner.json')):
        raise RuntimeError('A preserved preparation receipt differs from the root')
    binary_sha = json.loads((DIRECTORY / 'inputs.json').read_text())['binary_sha256']
    if digest(root / 'program-lastdbd') != binary_sha or not retained['all_owned_nodes_stopped']:
        raise RuntimeError('The retained executable or no-node result differs')
    processes = capture(['ps', '-p', str(marker['creator_pid']), '-o', 'pid=,command='])
    files = capture(['lsof', '-Fpn', '+D', str(root)])
    if any(value['exit_code'] != 1 or value['stdout'].strip() or value['stderr'].strip()
           for value in (processes, files)):
        raise RuntimeError('The preparation owner remains active or an open-file check fails')
    preflight = {'root': str(root), 'realpath': str(root.resolve()), 'owner_uid': info.st_uid,
        'mode': oct(stat.S_IMODE(info.st_mode)), 'device': info.st_dev, 'inode': info.st_ino,
        'marker': marker, 'node_sessions': 0, 'no_node_homes_or_sockets': True,
        'exact_root_files': sorted(path.name for path in expected),
        'binary_sha256': binary_sha, 'process_check': processes, 'open_file_check': files}
    command = [str(Path.home() / '.local/bin/lastdb-dev'), 'reclaim', '--home', str(root), '--exact-home']
    if args.execute_reviewed_preview:
        prior = json.loads((DIRECTORY / 'cleanup-preflight.json').read_text())
        preview = json.loads((DIRECTORY / 'cleanup-preview.json').read_text())
        if prior != preflight or preview['exit_code'] != 0 or preview['command'] != command + ['--dry-run']:
            raise RuntimeError('The preparation root differs from its reviewed preview')
        save(DIRECTORY / 'cleanup-execute-preflight.json', preflight)
        result = capture(command)
        result.update(root=str(root), root_exists_after=root.exists())
        save(DIRECTORY / 'cleanup.json', result)
    else:
        relative = str(DIRECTORY.relative_to(ROOT))
        names = set()
        for flags in (['--cached', '--others', '--exclude-standard'],
                      ['--others', '--ignored', '--exclude-standard']):
            names.update(subprocess.check_output(['git', 'ls-files', '-z', *flags, '--', relative],
                         cwd=ROOT).decode().split('\0'))
        sealed = {name: {'bytes': (ROOT / name).stat().st_size, 'sha256': digest(ROOT / name)}
                  for name in sorted(names - {''}) if (ROOT / name).is_file()}
        save(HERE / 'preparation-receipt-seal.json', {'node_sessions': 0,
            'product_operation_started': False, 'files': sealed})
        save(DIRECTORY / 'cleanup-preflight.json', preflight)
        result = capture(command + ['--dry-run'])
        result.update(root_exists_after_preview=root.exists())
        save(DIRECTORY / 'cleanup-preview.json', result)
    print(json.dumps({'root': str(root), 'node_sessions': 0,
                      'execute': args.execute_reviewed_preview, 'exit_code': result['exit_code']}))
    return result['exit_code']


if __name__ == '__main__':
    raise SystemExit(main())
