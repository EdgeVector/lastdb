#!/usr/bin/env python3
"""Freeze and check this release under the external host Cargo lock."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def identity_helper(output):
    path = HERE.parent / 'core-clarity-20261018/source_identity.py'
    spec = importlib.util.spec_from_file_location('atomic_release_identity', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ROOT, module.HERE = ROOT, output
    module.base.ROOT, module.base.HERE = ROOT, module.HERE
    module.base.PRIOR = HERE / 'before/source-identity.json'
    module.base.BUILD_FILES += ['folddb_profile/environments.json']
    return module


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=HERE / 'after')
    output = parser.parse_args().output.resolve()
    output.mkdir(exist_ok=False)
    helper = identity_helper(output)
    manifest = output / 'source-identity.json'
    helper.capture(manifest, manifest)
    commands = [
        ['cargo', 'clippy', '--workspace', '--lib', '--bins', '--locked',
         '--exclude', 'generate_validated_schema_org_seeds', '--exclude', 'schema_service_worker',
         '--', '-D', 'warnings'],
        ['cargo', 'clippy', '--locked', '-p', 'generate_validated_schema_org_seeds',
         '-p', 'schema_service_worker', '--bins', '--', '-D', 'warnings'],
        ['cargo', 'build', '--release', '--locked', '-p', 'lastdb_node', '--bin', 'lastdbd'],
    ]
    results = []
    for command in commands:
        print(json.dumps({'operation': 'build-step-start', 'command': command}), flush=True)
        start = time.monotonic()
        run = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
        row = {'command': command, 'elapsed_seconds': time.monotonic() - start,
               'exit_code': run.returncode, 'stdout': run.stdout, 'stderr': run.stderr}
        results.append(row)
        save(output / 'build-checks.json', results)
        print(json.dumps({'operation': 'build-step-finish', 'exit_code': run.returncode,
                          'elapsed_seconds': row['elapsed_seconds']}), flush=True)
        if run.returncode:
            print(run.stdout[-8000:] + run.stderr[-8000:], flush=True)
            return run.returncode
        helper.verify(manifest)
    binary = ROOT / 'target/release/lastdbd'
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    helper.verify(manifest, digest, True)
    save(output / 'binary.json', {'path': str(binary), 'sha256': digest,
        'bytes': binary.stat().st_size, 'source_identity': str(manifest),
        'release_verified': True})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
