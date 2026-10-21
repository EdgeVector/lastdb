#!/usr/bin/env python3
"""Verify unchanged release inputs and retain small non-test check receipts."""
import ast
import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
from build_release import HERE, ROOT, identity_helper


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def release_identity():
    directory = HERE / 'after-v2'
    binary = json.loads((directory / 'binary.json').read_text())
    actual = hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest()
    if actual != binary['sha256']:
        raise RuntimeError('The measured candidate executable changes after the run')
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        identity_helper(directory).verify(directory / 'source-identity.json', actual)
    return {'binary_sha256': actual, 'verification': json.loads(output.getvalue()),
            'scope': 'Read-only verification; the original finalized inventory is unchanged.'}


def baseline_patch():
    prefix = 'fold_db/crates/core/src/storage/disk_pack/'
    names = [prefix + name for name in ('mod.rs', 'view.rs', 'sync_status.rs')]
    baseline = json.loads((HERE / 'before/source-identity.json').read_text())
    for name in names:
        expected = baseline['source_files'][name]
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != expected['sha256']:
            raise RuntimeError('Baseline instrumentation differs from its frozen input')
    old = subprocess.run(['git', 'diff', 'd1e83a127a39874140500af928186c3e7b80e30e',
        '--', *names[:2]], cwd=ROOT, capture_output=True, check=True).stdout
    added = subprocess.run(['git', 'diff', '--no-index', '--', '/dev/null', names[2]],
        cwd=ROOT, capture_output=True, check=False)
    if added.returncode != 1 or added.stderr:
        raise RuntimeError('The new instrumentation source patch cannot be retained')
    path = HERE / 'before/instrumentation.patch'
    with path.open('xb') as stream:
        stream.write(old + added.stdout)
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'base': 'd1e83a127a39874140500af928186c3e7b80e30e', 'files': names,
            'scope': 'Only identical native barrier telemetry is added to the previous durable source.'}


def checks():
    commands = [['cargo', 'fmt', '--all', '--', '--check'], ['git', 'diff', '--check'],
        ['python3', 'scripts/lints/lint-atom-counter-writers.py'],
        ['bash', 'scripts/lints/lint-no-write-path-syncing-store.sh']]
    results = []
    for command in commands:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
        results.append({'command': command, 'exit_code': result.returncode,
                        'stdout': result.stdout, 'stderr': result.stderr})
    parsed = []
    for path in sorted(HERE.glob('*.py')):
        ast.parse(path.read_text(), filename=str(path))
        parsed.append({'path': str(path.relative_to(ROOT)),
                       'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    return results, parsed


def main():
    identity, patch = release_identity(), baseline_patch()
    commands, syntax = checks()
    passed = all(row['exit_code'] == 0 for row in commands)
    save(HERE / 'post-run-checks.json', {'passed': passed, 'release_identity': identity,
        'baseline_instrumentation': patch, 'commands': commands, 'python_syntax': syntax,
        'tests': 'No tests or fixture runner. The API workloads use fresh synthetic homes.'})
    print(json.dumps({'passed': passed, 'binary_sha256': identity['binary_sha256']}))
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
