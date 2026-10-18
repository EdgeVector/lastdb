#!/usr/bin/env python3
"""Preserve the accepted program and verify its source in an immutable Git tree."""
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
COMMIT = '011b29f3a74eac3eb3a977678630c40345157ed4'
EXPECTED = 'a0ad619076570fc472569712d5a40743a5a32823e909d45d4e33991b7c536285'
PRIOR = HERE.parent / 'core-memory-completion-20261018/attempt4-final-identity.json'


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def save(path, value):
    with path.open('x') as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write('\n')


def verify_git_sources(files):
    paths = sorted(files)
    queries = ''.join(COMMIT + ':' + path + '\n' for path in paths).encode()
    result = subprocess.run(['git', 'cat-file', '--batch'], cwd=ROOT,
        input=queries, capture_output=True, check=True)
    stream, mismatches = io.BytesIO(result.stdout), []
    for path in paths:
        header = stream.readline().rstrip(b'\n').split(b' ')
        if len(header) != 3 or header[1] != b'blob':
            raise RuntimeError('The immutable source is absent or has another type: ' + path)
        size = int(header[2])
        data = stream.read(size)
        if len(data) != size or stream.read(1) != b'\n':
            raise RuntimeError('The immutable source response is truncated')
        actual = {'bytes': size, 'sha256': hashlib.sha256(data).hexdigest()}
        if actual != files[path]:
            mismatches.append(path)
    if stream.read(1):
        raise RuntimeError('The immutable source response has extra bytes')
    if mismatches:
        raise RuntimeError('The accepted Git tree differs: ' + json.dumps(mismatches))
    return {'source_commit': COMMIT, 'source_files_checked': len(paths),
            'source_mismatches': mismatches, 'method': 'git cat-file --batch of explicit frozen paths'}


def main():
    if (HERE / 'before-identity.json').exists() or (HERE / 'baseline-root.json').exists():
        raise RuntimeError('The before identity already exists')
    prior = json.loads(PRIOR.read_text())
    if prior['state'] != 'release_verified' or prior['binary_sha256'] != EXPECTED:
        raise RuntimeError('The accepted release manifest differs')
    verification = verify_git_sources(prior['source_files'])
    program = ROOT / 'target/release/lastdbd'
    if digest(program) != EXPECTED:
        raise RuntimeError('The accepted executable differs')
    identity = {'version': 1, 'captured_utc': datetime.now(timezone.utc).isoformat(),
        'role': 'Immutable before build for the core-owned write comparison',
        'source_commit': COMMIT, 'source_input_count': len(prior['source_files']),
        'source_files': prior['source_files'],
        'source_inventory_sha256': prior['source_inventory_sha256'],
        'source_git_verification': verification,
        'accepted_manifest': str(PRIOR), 'accepted_manifest_sha256': digest(PRIOR),
        'binary_sha256': EXPECTED, 'build': prior['build'],
        'scope': 'Accepted completion build from merged main. No new build, home copy, node start or timing run.'}
    save(HERE / 'before-identity.json', identity)
    destination = Path(tempfile.mkdtemp(prefix='sp.')).resolve()
    info = destination.stat()
    marker = {'version': 1, 'kind': 'synthetic-trial', 'owner_uid': os.getuid(),
              'device': info.st_dev, 'inode': info.st_ino, 'creator_pid': os.getpid()}
    descriptor = os.open(destination / '.synthetic-seed-owner.json',
                         os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as output:
        json.dump(marker, output, sort_keys=True)
        output.write('\n')
    private = destination / 'program-before-lastdbd'
    shutil.copy2(program, private)
    if digest(private) != EXPECTED or digest(program) != EXPECTED:
        raise RuntimeError('The accepted executable changes during its private copy')
    baseline = {'root': str(destination), 'binary': str(private), 'binary_sha256': EXPECTED,
        'node_sessions': 0, 'source_binary': str(program), 'marker': marker,
        'source_identity': str(HERE / 'before-identity.json'),
        'source_identity_sha256': digest(HERE / 'before-identity.json'),
        'scope': 'Accepted executable only. No primary, archive, existing home, or node.'}
    save(HERE / 'baseline-root.json', baseline)
    print(json.dumps({'root': str(destination), 'binary_sha256': EXPECTED,
                      'source_files_checked': verification['source_files_checked'],
                      'source_mismatches': verification['source_mismatches']}), flush=True)


if __name__ == '__main__':
    main()
