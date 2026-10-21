#!/usr/bin/env python3
"""Archive exact completed evidence, then reuse the supported sealed-root cleanup."""
import argparse
import gzip
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import zlib

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
RUNS = [HERE / 'paired-r1' / label for label in
        ('before-r1', 'after-r1', 'after-r2', 'before-r2')] + [HERE / 'operations-r1/run']
SHARED = HERE.parent / 'core-owned-writes-20261018'
SPEC = importlib.util.spec_from_file_location('atomic_exact_archive_parts',
    HERE.parent / 'durable-write-path-20261020/pack_receipts.py')
parts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(parts)


def read(path):
    return json.loads(path.read_text())


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def slot(base, name):
    # JSON/JSONL directory suffixes match the repository's raw-file ignore rules.
    return base.joinpath(*(part.lstrip('.').replace('.', '-') for part in Path(name).parts))


def completed():
    if read(HERE / 'paired-r1/summary.json').get('complete') is not True:
        raise RuntimeError('The complete paired comparison must finish before archive or cleanup')
    if read(HERE / 'operations-r1/result.json').get('complete') is not True:
        raise RuntimeError('Ordinary operation checks must finish before archive or cleanup')


def verify_archive(directory, manifest):
    import hashlib
    decoded_hash, archive_hash, decoded_bytes, archive_bytes = hashlib.sha256(), hashlib.sha256(), 0, 0
    decoder = zlib.decompressobj(31)
    for part in manifest['gzip']['parts']:
        path = directory / part['path']
        if parts.identity(path) != {name: part[name] for name in ('sha256', 'bytes')}:
            raise RuntimeError('An exact archive part differs')
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                archive_hash.update(block)
                archive_bytes += len(block)
                while block:
                    decoded = decoder.decompress(block, 1024 * 1024)
                    decoded_hash.update(decoded)
                    decoded_bytes += len(decoded)
                    block = decoder.unconsumed_tail
    decoded = decoder.flush()
    decoded_hash.update(decoded)
    decoded_bytes += len(decoded)
    if (not decoder.eof or decoder.unused_data or
        {'sha256': decoded_hash.hexdigest(), 'bytes': decoded_bytes} != manifest['raw'] or
        {'sha256': archive_hash.hexdigest(), 'bytes': archive_bytes} !=
            {name: manifest['gzip'][name] for name in ('sha256', 'bytes')}):
        raise RuntimeError('Exact gzip reassembly does not reproduce the original bytes')


def archive_exact(source, destination):
    if source.is_symlink() or not source.is_file():
        raise RuntimeError('Archive only an exact regular source file')
    before, raw = parts.stamp(source), parts.identity(source)
    destination.mkdir(parents=True, exist_ok=False)
    sink = parts.Parts(destination)
    try:
        with source.open('rb') as original, gzip.GzipFile(fileobj=sink, mode='wb',
                                                       filename='', mtime=0) as archive:
            shutil.copyfileobj(original, archive, 1024 * 1024)
    finally:
        sink.close()
    renamed = []
    for index, path in enumerate(sink.parts, 1):
        following = destination / f'payload.gz.part-{index:03d}'
        path.rename(following)
        renamed.append(following)
    sink.parts = renamed
    manifest = {'format': 'lastdb-exact-evidence-gzip-v1', 'source': str(source),
        'raw': raw, 'gzip': sink.manifest(),
        'contract': 'The gzip stream reproduces every original byte, ID and time. No JSON transformation.'}
    verify_archive(destination, manifest)
    if parts.stamp(source) != before or parts.identity(source) != raw:
        raise RuntimeError('The original changes during exact preservation')
    save(destination / 'manifest.json', manifest)
    return {'manifest': str(destination / 'manifest.json'), 'source': str(source),
            'raw': raw, 'gzip': manifest['gzip']}


def prepare_operation_result():
    source, target = HERE / 'operations-r1/result.json', HERE / 'operations-r1/run/result.json'
    if target.exists():
        if parts.identity(target) != parts.identity(source):
            raise RuntimeError('The operation result copy differs from its original')
    else:
        # The unchanged shared seal helper expects a result beside each receipt file.
        with source.open('rb') as original, target.open('xb') as destination:
            shutil.copyfileobj(original, destination)


def archive_run(directory):
    root = Path(read(directory / 'retained-root.json')['root'])
    if (root.is_symlink() or root.resolve(strict=True) != root or
        root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith('sp.')):
        raise RuntimeError('The retained root escapes the exact synthetic namespace')
    rows = [json.loads(line) for line in (directory / 'receipts.jsonl').read_text().splitlines()]
    starts = [row for row in rows if row.get('operation') == 'owned-node-start']
    stops = [row for row in rows if row.get('operation') == 'owned-node-stop']
    if (not starts or len(starts) != len(stops) or
        {row['pid'] for row in starts} != {row['pid'] for row in stops} or
        any(row.get('forced_kill') is not False or row.get('exit_code') not in (0, -15) for row in stops)):
        raise RuntimeError('A node session lacks its recorded normal stop')
    names = ('receipts.jsonl', 'inputs.json', 'summary.json', 'result.json',
             'retained-root.json', '.synthetic-seed-owner.json')
    archives = [archive_exact(directory / name, slot(directory / 'portable', name))
                for name in names]
    logs = sorted({Path(row['home']).parent / 'node.log' for row in starts})
    for index, log in enumerate(logs):
        if log.parent.parent != root or log.parent.is_symlink():
            raise RuntimeError('An exact node log escapes its retained root')
        archives.append(archive_exact(log, directory / 'portable' / f'node-log-{index:02d}'))
    save(directory / 'portable/manifest.json', {'archives': archives,
        'scope': 'Exact raw files and compact original receipts, including every ID and original timestamp.'})
    return archives


def archive_all():
    prepare_operation_result()
    archives = []
    for directory in RUNS:
        archives.extend(archive_run(directory))
    names = ['before/source-identity.json', 'after-v2/source-identity.json',
        'paired-r1/command.json', 'paired-r1/summary.json', 'operations-r1/command.json',
        'operations-r1/result.json', 'comparison.json']
    if (HERE / 'after/source-identity.json').exists():
        names.append('after/source-identity.json')
    for name in names:
        source = HERE / name
        archives.append(archive_exact(source, slot(HERE / 'portable', name)))
    save(HERE / 'archives.json', {'archives': archives,
        'part_max_bytes': parts.PART_BYTES, 'all_gzip_streams_exactly_verified': True,
        'operation_result_copy': 'operations-r1/run/result.json is an exact copy of operations-r1/result.json',
        'reused_parts_helper': str(Path(parts.__file__).resolve())})
    print(json.dumps({'archives': len(archives), 'part_max_bytes': parts.PART_BYTES}))


def shared_helper(name):
    sys.path.insert(0, str(SHARED))
    module = __import__(name)
    module.HERE = HERE
    if hasattr(module, 'ROOT'):
        module.ROOT = ROOT
    return module


def seal():
    if read(HERE / 'archives.json').get('all_gzip_streams_exactly_verified') is not True:
        raise RuntimeError('Exact archives must precede the immutable seal')
    module = shared_helper('seal_runs')
    sys.argv = ['seal', '--directories', *map(str, RUNS), '--output', str(HERE / 'sealed-originals.json')]
    module.main()
    archive_exact(HERE / 'sealed-originals.json', HERE / 'portable/sealed-originals-json')


def cleanup(execute):
    seal_path = HERE / 'sealed-originals.json'
    if execute:
        review = read(HERE / 'independent-cleanup-review.json')
        if review.get('ready_to_reclaim') is not True or review.get('seal_sha256') != parts.identity(seal_path)['sha256']:
            raise RuntimeError('A fresh independent review must approve this exact seal before reclaim')
    module = shared_helper('cleanup_sealed_runs')
    output = HERE / ('cleanup-execute.json' if execute else 'cleanup-previews.json')
    sys.argv = ['cleanup', '--seal', str(seal_path), '--output', str(output)]
    if execute:
        sys.argv.append('--execute-reviewed-previews')
    return module.main()


def baseline_plan():
    original = read(HERE / 'baseline-original.json')
    root = Path(original['owned_binary_container'])
    command = [str(Path.home() / '.local/bin/lastdb-dev'), 'reclaim', '--home', str(root), '--exact-home']
    return {'root': str(root), 'preserved_owner_marker': str(HERE / '.baseline-binary-owner.json'),
        'preview': command + ['--dry-run'], 'execute_after_independent_review': command,
        'scope': 'Binary container only, no node sessions. Separate ownership, creator PID, binary SHA, '
                 'regular-file inventory and open-file checks are required. No node receipt is invented.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('archive', 'seal', 'preview', 'execute', 'baseline-plan'))
    action = parser.parse_args().action
    completed()
    if action == 'archive':
        archive_all()
    elif action == 'seal':
        seal()
    elif action in ('preview', 'execute'):
        return cleanup(action == 'execute')
    else:
        print(json.dumps(baseline_plan(), sort_keys=True, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
