#!/usr/bin/env python3
"""Save the exact measurement sources and a receipt file digest list."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt-dir', type=Path, required=True)
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    directory = args.receipt_dir.resolve(strict=True)
    if directory.parent != base:
        raise RuntimeError('Use one direct run directory under this proof directory')
    inputs = json.loads((directory / 'inputs.json').read_text())
    shared = base.parent / 'graph-first-20261017/drivers'
    sources = {'measure.py': base / 'measure.py',
               'range_profile.py': shared / 'range_profile.py',
               'ownership.py': shared / 'ownership.py'}
    for name, source in sources.items():
        wanted = inputs['driver_sha256'] if name == 'measure.py' else inputs['imports'][name]
        destination = directory / name
        if destination.exists():
            if digest(destination) != wanted:
                raise RuntimeError('The preserved source differs: ' + name)
            continue
        if digest(source) != wanted:
            raise RuntimeError('The source differs from the saved run identity: ' + name)
        shutil.copy2(source, destination)
    manifest = {}
    for path in directory.iterdir():
        if path.name == 'receipt-manifest.json':
            continue
        if path.is_symlink():
            raise RuntimeError('A receipt is a symbolic link: ' + path.name)
        if path.is_file():
            manifest[path.name] = {'bytes': path.stat().st_size, 'sha256': digest(path)}
    (directory / 'receipt-manifest.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'receipt_dir': str(directory), 'files': len(manifest),
                      'bytes': sum(value['bytes'] for value in manifest.values())}))


if __name__ == '__main__':
    main()
