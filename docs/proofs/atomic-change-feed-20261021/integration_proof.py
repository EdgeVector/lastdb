#!/usr/bin/env python3
"""Preserve and check the main integration without changing the measured proof."""
import argparse
import ast
import contextlib
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
from build_release import HERE, ROOT, identity_helper
import preserve


def context():
    preserve.HERE = HERE / 'operations-main-r1'
    preserve.RUNS = [preserve.HERE / 'run']
    return preserve


def archive():
    proof = context()
    result = proof.HERE / 'result.json'
    if proof.read(result).get('complete') is not True:
        raise RuntimeError('The final integration operation checks must complete first')
    copied = proof.RUNS[0] / 'result.json'
    with result.open('rb') as original, copied.open('xb') as destination:
        shutil.copyfileobj(original, destination)
    if proof.parts.identity(copied) != proof.parts.identity(result):
        raise RuntimeError('The new operation result copy differs')
    archives = proof.archive_run(proof.RUNS[0])
    names = ['operations-main-r1/command.json', 'operations-main-r1/result.json',
             'after-main/source-identity.json', 'after-main-v2/source-identity.json',
             'after-main-v2/binary.json']
    for name in names:
        archives.append(proof.archive_exact(HERE / name, proof.slot(proof.HERE / 'portable', name)))
    proof.save(proof.HERE / 'archives.json', {'archives': archives,
        'part_max_bytes': proof.parts.PART_BYTES,
        'all_gzip_streams_exactly_verified': True,
        'scope': 'Only the new main integration proof. The original measured proof remains unchanged.'})
    print(json.dumps({'archives': len(archives), 'new_proof': str(proof.HERE)}))


def capture(command):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    return {'command': command, 'exit_code': result.returncode,
            'stdout': result.stdout, 'stderr': result.stderr}


def checks():
    directory = HERE / 'after-main-v2'
    binary = preserve.read(directory / 'binary.json')
    actual = hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest()
    if actual != binary['sha256']:
        raise RuntimeError('The integrated executable changes after the operation run')
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        identity_helper(directory).verify(directory / 'source-identity.json', actual)
    verification = json.loads(output.getvalue())
    commands = [
        ['cargo', 'fmt', '--all', '--', '--check'],
        ['git', 'diff', '--check'], ['git', 'diff', '--cached', '--check'],
        ['python3', 'scripts/lints/lint-atom-counter-writers.py'],
        ['bash', 'scripts/lints/lint-no-write-path-syncing-store.sh'],
        ['python3', 'scripts/lints/lint-diff-fn-size.py', '--base', 'origin/main',
         '--head', 'HEAD', '--max', '100', '--allow', '10'],
        ['python3', 'scripts/lints/lint-diff-file-size.py', '--base', 'origin/main',
         '--head', 'HEAD', '--max', '400', '--allow', '10'],
        ['python3', 'scripts/lints/lint-diff-new-allows.py', '--base', 'origin/main', '--head', 'HEAD'],
        ['bash', 'scripts/lints/lint-no-build-artifacts.sh', '--range', 'origin/main..HEAD'],
    ]
    results = [capture(command) for command in commands]
    parsed = []
    for path in sorted(HERE.glob('*.py')):
        ast.parse(path.read_text(), filename=str(path))
        parsed.append({'path': str(path.relative_to(ROOT)),
                       'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    operation = preserve.read(HERE / 'operations-main-r1/result.json')
    review = preserve.read(HERE / 'operations-main-r1/operation-review.json')
    cleanup = preserve.read(HERE / 'operations-main-r1/cleanup-execute.json')
    root_absent = not Path(operation['root']).exists()
    build = preserve.read(directory / 'build-checks.json')
    source = preserve.read(directory / 'source-identity.json')
    command = preserve.read(HERE / 'operations-main-r1/command.json')
    manifest_hash = preserve.parts.identity(directory / 'source-identity.json')['sha256']
    if (command['binary']['sha256'] != actual or command['source']['binary_sha256'] != actual
            or command['source']['sha256'] != manifest_hash
            or command['source']['source_inventory_sha256'] != source['source_inventory_sha256']):
        raise RuntimeError('The final operation receipts do not bind to this integrated release')
    value = {'passed': verification['passed'] and operation['complete'] and
             review['operation_checks_pass'] and cleanup['complete'] and root_absent and
             len(build) == 3 and all(row['exit_code'] == 0 for row in build) and
             all(row['exit_code'] == 0 for row in results),
        'head_checked': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'integrated_main': subprocess.check_output(['git', 'rev-parse', 'origin/main'], cwd=ROOT, text=True).strip(),
        'integrated_binary': binary, 'source_verification': verification,
        'source_inventory_sha256': source['source_inventory_sha256'],
        'commands': results, 'python_syntax': parsed,
        'operation_result': operation, 'operation_source_and_binary_binding_verified': True,
        'operation_review': review,
        'cleanup': cleanup, 'exact_owned_root_absent': root_absent,
        'build_checks': 'after-main-v2/build-checks.json',
        'build_wrapper': ['scripts/ci/with-fold-host-cargo-lock.sh', '--timeout', '3600', '--',
                          'python3', 'docs/proofs/atomic-change-feed-20261021/build_release.py',
                          '--output', 'docs/proofs/atomic-change-feed-20261021/after-main-v2'],
        'failed_integration_attempt': 'after-main/build-checks.json retains the Option/u64 merge-resolution failure; after-main-v2 corrects it and preserves the unconditional durable wait.',
        'timing_scope': 'The ABBA results measure the pre-integration after-v2 executable, not this integrated executable.',
        'limits': 'Fresh synthetic home only. No tests, fixtures, crash, power-loss, cloud, primary, archive or migration.'}
    preserve.save(HERE / 'integration.json', value)
    print(json.dumps({'passed': value['passed'], 'integrated_binary_sha256': actual}))
    return 0 if value['passed'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('archive', 'seal', 'preview', 'execute', 'checks'))
    action = parser.parse_args().action
    if action == 'archive':
        archive()
    elif action == 'checks':
        return checks()
    else:
        proof = context()
        if action == 'seal':
            proof.seal()
        else:
            return proof.cleanup(action == 'execute')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
