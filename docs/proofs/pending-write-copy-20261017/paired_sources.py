#!/usr/bin/env python3
"""Preserve and identify the two product variants for the paired live measurement."""
import argparse
import datetime
import difflib
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
PAIRED = HERE / 'eligible'
RECEIPTS = [
    'fold_db/crates/core/src/storage/disk_pack/transaction.rs',
    'fold_db/crates/core/src/storage/disk_pack/logical_paths/source.rs',
    'fold_db/crates/core/src/storage/disk_pack/kv.rs',
]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def record(data):
    return {'bytes': len(data), 'sha256': sha(data)}


def save(path, value):
    with path.open('x') as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write('\n')


def old_files():
    return json.loads((HERE / 'before-source-inputs/manifest.json').read_text())['files']


def freeze(patch):
    names = sorted(set(old_files()) | set(RECEIPTS))
    manifest = {}
    for name in names:
        data = (ROOT / name).read_bytes()
        target = PAIRED / 'after-source-inputs' / (name + '.txt')
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as output:
            output.write(data)
        manifest[name] = record(data)
    delta = patch.read_bytes()
    with (PAIRED / 'write-source-receipts.patch').open('xb') as output:
        output.write(delta)
    save(PAIRED / 'after-source-inputs/manifest.json', {
        'files': manifest, 'receipt_patch': record(delta),
        'scope': 'Final append-only source plus bounded write-only receipt prerequisite.'})
    print(json.dumps({'preserved_files': len(manifest), 'receipt_patch': sha(delta)}))


def restore_after():
    manifest = json.loads((PAIRED / 'after-source-inputs/manifest.json').read_text())['files']
    contents = {}
    for name, expected in manifest.items():
        data = (PAIRED / 'after-source-inputs' / (name + '.txt')).read_bytes()
        if record(data) != expected:
            raise RuntimeError('Frozen replacement source differs: ' + name)
        contents[name] = data
    for name, data in contents.items():
        (ROOT / name).write_bytes(data)
    print(json.dumps({'restored_after_files': len(contents)}))


def prepare_before():
    contents = {}
    for name, expected in old_files().items():
        data = (HERE / 'before-source-inputs' / (name + '.txt')).read_bytes()
        if record(data) != {'bytes': expected['bytes'], 'sha256': expected['sha256']}:
            raise RuntimeError('Frozen original source differs: ' + name)
        contents[name] = data
    try:
        for name, data in contents.items():
            (ROOT / name).write_bytes(data)
        command = ['git', 'apply', '--include=' + RECEIPTS[0]]
        patch = str(PAIRED / 'write-source-receipts.patch')
        subprocess.run(command + ['--check', patch], cwd=ROOT, check=True)
        subprocess.run(command + [patch], cwd=ROOT, check=True)
        print(json.dumps({'restored_before_files': len(contents),
                          'prerequisite_transaction_patch': 'applied'}))
    except BaseException:
        restore_after()
        raise


def identity(label):
    previous = json.loads((HERE / 'before-source.json').read_text())
    baseline = previous['source_files']
    files = {name: record((ROOT / name).read_bytes()) for name in baseline}
    changes = sorted(name for name in baseline if baseline[name] != files[name])
    expected = set(RECEIPTS)
    if label == 'after':
        expected.update(old_files())
    if set(changes) != expected:
        raise RuntimeError('Unexpected product delta: ' + json.dumps(changes))
    result = {'captured_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'variant': label, 'base_head': previous['base_head'],
        'scope': 'Both variants contain the identical bounded write-only receipt prerequisite.',
        'comparison': 'Original prefix copy' if label == 'before' else 'Append new operations only',
        'changed_from_original_baseline': changes,
        'receipt_patch': record((PAIRED / 'write-source-receipts.patch').read_bytes()),
        'source_files': files}
    save(PAIRED / (label + '-source-freeze.json'), result)
    print(json.dumps({'variant': label, 'files': len(files), 'changes': changes}))


def built(label):
    path = PAIRED / (label + '-source-freeze.json')
    result = json.loads(path.read_text())
    mismatches = [name for name, expected in result['source_files'].items()
                  if record((ROOT / name).read_bytes()) != expected]
    if mismatches:
        raise RuntimeError('Product changed during build: ' + json.dumps(mismatches))
    binary = ROOT / 'target/release/lastdbd'
    result['built_program'] = {'path': str(binary), **record(binary.read_bytes())}
    result['verified_after_build_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    result['mismatched_source_files'] = mismatches
    save(PAIRED / (label + '-source.json'), result)
    print(json.dumps({'variant': label, 'program': result['built_program'], 'mismatches': mismatches}))


def receipt_lint_fix():
    original_patch = HERE / 'eligible/write-source-receipts.patch'
    saved = {name: (ROOT / name).read_bytes() for name in RECEIPTS}
    subprocess.run(['git', 'apply', '--reverse', '--check', str(original_patch)], cwd=ROOT, check=True)
    subprocess.run(['git', 'apply', '--reverse', str(original_patch)], cwd=ROOT, check=True)
    try:
        originals = {name: (ROOT / name).read_text() for name in RECEIPTS}
    finally:
        for name, data in saved.items():
            (ROOT / name).write_bytes(data)
    transaction = ROOT / RECEIPTS[0]
    source = transaction.read_text()
    old = '.get_or_insert_with(|| Arc::new(Mutex::new(Default::default()))),'
    new = '.get_or_insert_with(|| Arc::new(Mutex::new(super::logical_paths::source::Sources::default()))),'
    if source.count(old) != 1:
        raise RuntimeError('Receipt default expression differs')
    transaction.write_text(source.replace(old, new, 1))
    subprocess.run(['rustfmt', '--edition', '2021', str(transaction)], cwd=ROOT, check=True)
    patch = ''.join(''.join(difflib.unified_diff(
        originals[name].splitlines(keepends=True), (ROOT / name).read_text().splitlines(keepends=True),
        fromfile='a/' + name, tofile='b/' + name)) for name in RECEIPTS)
    target = HERE / 'write-source-receipts-final.patch'
    with target.open('x') as output:
        output.write(patch)
    print(json.dumps({'receipt_patch': str(target), 'sha256': sha(patch.encode())}))


def main():
    global PAIRED
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['freeze', 'before', 'restore', 'identity', 'built', 'receipt-lint-fix'])
    parser.add_argument('--patch', type=Path)
    parser.add_argument('--label', choices=['before', 'after'])
    parser.add_argument('--pair-dir', default='eligible')
    args = parser.parse_args()
    if args.pair_dir not in ('eligible', 'eligible-final'):
        raise RuntimeError('Unknown paired proof directory')
    PAIRED = HERE / args.pair_dir
    if args.action == 'freeze':
        freeze(args.patch)
    elif args.action == 'before':
        prepare_before()
    elif args.action == 'restore':
        restore_after()
    elif args.action == 'identity':
        identity(args.label)
    elif args.action == 'built':
        built(args.label)
    else:
        receipt_lint_fix()


if __name__ == '__main__':
    main()
