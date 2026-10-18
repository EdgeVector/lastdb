#!/usr/bin/env python3
"""Preview or reclaim exact sealed synthetic runs, including the documented startup refusal."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile

from seal_runs import digest

HERE = Path(__file__).resolve().parent
GATE = 'native disk homes require their complete backup and cloud-sync gate before activation'


def capture(command):
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return {'command': list(map(str, command)), 'exit_code': result.returncode,
            'stdout': result.stdout, 'stderr': result.stderr}


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def preflight(run):
    directory = Path(run['receipt_directory']).resolve(strict=True)
    if not directory.is_relative_to(HERE) or len(directory.relative_to(HERE).parts) > 2:
        raise RuntimeError('The sealed receipt directory escapes this proof')
    for name, expected in run['files'].items():
        path = directory / name
        if path.stat().st_size != expected['bytes'] or digest(path) != expected['sha256']:
            raise RuntimeError('A sealed original receipt differs: ' + str(path))
    root = Path(run['retained_root']['root'])
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith('sp.')):
        raise RuntimeError('The exact synthetic root escapes its real owned namespace')
    info = root.stat()
    marker_path = root / '.synthetic-seed-owner.json'
    marker_info, marker = marker_path.lstat(), json.loads(marker_path.read_text())
    if (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or not stat.S_ISREG(marker_info.st_mode) or marker_info.st_nlink != 1
            or marker_info.st_uid != os.getuid() or stat.S_IMODE(marker_info.st_mode) != 0o600
            or marker.get('owner_uid') != os.getuid() or marker.get('kind') != 'synthetic-trial'
            or marker.get('device') != info.st_dev or marker.get('inode') != info.st_ino
            or marker != json.loads((directory / '.synthetic-seed-owner.json').read_text())):
        raise RuntimeError('The exact root ownership differs from the preserved marker')
    rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
    starts = [row for row in rows if row.get('operation') == 'owned-node-start']
    stops = [row for row in rows if row.get('operation') == 'owned-node-stop']
    normal = (len(starts) == len(stops) and starts and
        {row['pid'] for row in starts} == {row['pid'] for row in stops} and
        all(row.get('forced_kill') is False and row.get('exit_code') in (0, -15) for row in stops))
    refused_start = (directory.name == 'before-replay-v3' and len(starts) == len(stops) == 1
        and starts[0]['pid'] == stops[0]['pid'] and stops[0].get('forced_kill') is False
        and stops[0].get('exit_code') == 1 and run['result'].get('workflow') == 'replay'
        and not any(row.get('operation') in ('request', 'generated-owner-provision') for row in rows)
        and GATE in (directory / 'node-r.log').read_text())
    if not (normal or refused_start):
        raise RuntimeError('An owned node lacks its normal stop or exact documented startup refusal')
    pids = sorted({row['pid'] for row in starts})
    processes = capture(['ps', '-p', ','.join(map(str, pids)), '-o', 'pid=,command='])
    files = capture(['lsof', '-Fpn', '+D', str(root)])
    directories = {Path(row['home']).parent for row in starts}
    if any(path.parent != root for path in directories):
        raise RuntimeError('An owned node directory escapes the exact root')
    sockets = [str(path / name) for path in directories for name in ('q.sock', 'f.sock')
               if (path / name).exists()]
    result = {'checked_utc': datetime.now(timezone.utc).isoformat(),
        'root': str(root), 'realpath': str(root.resolve()), 'owner_uid': info.st_uid,
        'device': info.st_dev, 'inode': info.st_ino, 'marker': marker,
        'normal_stops': stops, 'documented_startup_gate_refusal': bool(refused_start),
        'process_check': processes, 'open_file_check': files, 'known_socket_paths_remaining': sockets}
    if (processes['exit_code'] != 1 or processes['stdout'].strip() or processes['stderr'].strip()
            or files['exit_code'] != 1 or files['stdout'].strip() or files['stderr'].strip() or sockets):
        raise RuntimeError('The exact owned root still has a process, open file, socket, or failed check')
    return directory, root, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seal', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--execute-reviewed-previews', action='store_true')
    args = parser.parse_args()
    seal, outcomes = json.loads(args.seal.read_text()), []
    if seal.get('all_run_processes_exit_unforced') is not True:
        raise RuntimeError('The seal does not establish released process records')
    for run in seal['runs']:
        directory, root, checked = preflight(run)
        command = [str(Path.home() / '.local/bin/lastdb-dev'), 'reclaim', '--home', str(root), '--exact-home']
        if args.execute_reviewed_previews:
            prior = json.loads((directory / 'cleanup-preflight.json').read_text())
            preview = json.loads((directory / 'cleanup-preview.json').read_text())
            if (any(prior[key] != checked[key] for key in ('root', 'device', 'inode', 'marker'))
                    or preview.get('exit_code') != 0 or preview.get('command') != command + ['--dry-run']
                    or preview.get('root_exists_after_preview') is not True):
                raise RuntimeError('The current exact root differs from its reviewed supported preview')
            save(directory / 'cleanup-execute-preflight.json', checked)
            result = capture(command)
            result.update(root=str(root), root_exists_after=root.exists())
            save(directory / 'cleanup.json', result)
        else:
            save(directory / 'cleanup-preflight.json', checked)
            result = capture(command + ['--dry-run'])
            result.update(root_exists_after_preview=root.exists())
            save(directory / 'cleanup-preview.json', result)
        outcomes.append({'receipt_directory': str(directory), 'root': str(root),
            'startup_gate_refusal': checked['documented_startup_gate_refusal'], 'result': result})
        if result['exit_code'] != 0:
            save(args.output, {'outcomes': outcomes, 'complete': False})
            return result['exit_code']
    save(args.output, {'outcomes': outcomes, 'complete': True,
        'scope': 'Exact supported reclaim only. The startup refusal is not described as a normal session.'})
    print(json.dumps({'roots': len(outcomes), 'execute': args.execute_reviewed_previews, 'complete': True}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
