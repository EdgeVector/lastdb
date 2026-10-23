#!/usr/bin/env python3
"""Remove only an owned synthetic benchmark home after its daemon has stopped."""
import json
import os
from pathlib import Path
import shutil


def cleanup(scratch, output):
    scratch, output = Path(scratch), Path(output)
    if scratch.is_symlink():
        raise RuntimeError('Refusing a symlink scratch root')
    scratch = scratch.resolve(strict=True)
    output = output.resolve(strict=True)
    info = scratch.stat()
    owner = json.loads((scratch / '.snapshot-capture-owner.json').read_text())
    home = scratch / 'home'
    if (owner['primary_data'] is not False or owner['owner_uid'] != os.getuid()
            or info.st_uid != os.getuid() or info.st_mode & 0o777 != 0o700
            or owner['device'] != info.st_dev or owner['inode'] != info.st_ino
            or Path(owner['scratch']) != scratch.resolve() or Path(owner['home']) != home
            or home.is_symlink()):
        raise RuntimeError('Synthetic ownership proof differs; refusing cleanup')
    summary = json.loads((output / 'summary.json').read_text())
    if summary.get('all_owned_processes_stopped') is not True:
        raise RuntimeError('The benchmark does not prove its process stopped')
    for process in Path('/proc').glob('[0-9]*/cmdline'):
        try:
            arguments = process.read_bytes().split(b'\0')
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if b'--data-dir' in arguments and os.fsencode(home) in arguments:
            raise RuntimeError('An active daemon still names this synthetic home')
    files, allocated = 0, 0
    for parent, directories, names in os.walk(home, followlinks=False):
        for name in names:
            file_info = (Path(parent) / name).lstat()
            files += 1
            allocated += file_info.st_blocks * 512
    free_before = shutil.disk_usage(scratch).free
    shutil.rmtree(home)
    receipt = {'run_id': owner['run_id'], 'home': str(home), 'primary_data': False,
               'ownership_verified': True, 'no_active_daemon': True, 'home_removed': True,
               'files_removed': files, 'allocated_bytes_removed': allocated,
               'free_bytes_before': free_before, 'free_bytes_after': shutil.disk_usage(scratch).free}
    (output / 'cleanup.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt
