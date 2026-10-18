#!/usr/bin/env python3
"""Capture and verify the complete frozen release source input inventory."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
PRIOR = HERE.parent / 'core-memory-state-20261018/current-source-v4.json'
MANIFEST = HERE / 'completion-identity.json'
CODE_ROOTS = ['.cargo', 'app_identity_crypto', 'env_flag', 'fold_db', 'folddb_profile',
              'lastdb_host', 'lastdb_identity', 'lastdb_node', 'lastdb_uds',
              'schema_service', 'vendor']
ROOT_FILES = ['Cargo.toml', 'Cargo.lock', 'rust-toolchain', 'rust-toolchain.toml',
              'build.rs', 'rustfmt.toml', '.rustfmt.toml', 'clippy.toml', '.clippy.toml']
BUILD_FILES = ['scripts/ci/with-fold-host-cargo-lock.sh',
               'scripts/ci/lib/host-cargo-lock.py']
ENV_KEYS = ['CARGO_BUILD_TARGET', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL',
            'CARGO_TARGET_DIR', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTFLAGS', 'RUSTC',
            'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'RUSTUP_TOOLCHAIN',
            'MACOSX_DEPLOYMENT_TARGET', 'SDKROOT', 'CC', 'CXX', 'AR', 'CFLAGS',
            'CXXFLAGS', 'LDFLAGS', 'RUST_MIN_STACK', 'SCCACHE_DISABLE']


def utc():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def command(args):
    return subprocess.check_output(args, cwd=str(ROOT), text=True).strip()


def product_path(path):
    name = Path(path).name
    parts = Path(path).parts
    return (path.endswith('.rs') or name in ROOT_FILES
            or name in ('Cargo.toml', 'Cargo.lock') or '.cargo' in parts
            or any(part in ('src', 'assets', 'resources') for part in parts))


def discover():
    prior = json.loads(PRIOR.read_text())['source_files']
    inventory = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--']
        + CODE_ROOTS + ROOT_FILES + BUILD_FILES, cwd=str(ROOT))
    candidates = set(prior) | set(BUILD_FILES)
    candidates.update(path for path in inventory.decode().split('\0')
                      if path and (product_path(path) or path in BUILD_FILES))
    existing = sorted(path for path in candidates if (ROOT / path).is_file())
    removed = sorted(path for path in prior if not (ROOT / path).is_file())
    return existing, removed


def source_snapshot():
    paths, removed = discover()
    files = {path: {'sha256': digest(ROOT / path), 'bytes': (ROOT / path).stat().st_size}
             for path in paths}
    return files, removed


def inventory_digest(files):
    encoded = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(encoded).hexdigest()


def host_settings():
    external = {}
    for path in [Path.home() / '.cargo/config', Path.home() / '.cargo/config.toml',
                 Path.home() / '.rustup/settings.toml']:
        if path.is_file():
            external[str(path)] = {'sha256': digest(path), 'bytes': path.stat().st_size}
    tools = {}
    for name in ('rustc', 'cargo'):
        path = shutil.which(name)
        if path is None:
            raise RuntimeError('The required compiler tool is absent: ' + name)
        tools[name] = {'path': path, 'realpath': str(Path(path).resolve()),
                       'version': command([name, '-Vv'])}
    return {'system': platform.platform(), 'machine': platform.machine(),
            'processor': command(['sysctl', '-n', 'machdep.cpu.brand_string']),
            'memory_bytes': int(command(['sysctl', '-n', 'hw.memsize'])),
            'tools': tools, 'external_config_inputs': external,
            'environment': {key: os.environ.get(key) for key in ENV_KEYS}}


def write_manifest(data, manifest):
    manifest.write_text(json.dumps(data, sort_keys=True, indent=2) + '\n')


def capture(manifest, previous_manifest):
    if manifest.exists():
        raise RuntimeError('The source identity already exists; use verify/finalize')
    files, removed = source_snapshot()
    second, second_removed = source_snapshot()
    if files != second or removed != second_removed:
        raise RuntimeError('A product input changes during source capture')
    prior = json.loads(PRIOR.read_text())
    baseline = json.loads((HERE / 'baseline-root.json').read_text())
    if baseline['source_identity_sha256'] != digest(PRIOR):
        raise RuntimeError('The preserved previous source identity changes')
    data = {
        'version': 1, 'state': 'sources_frozen_binary_pending', 'captured_utc': utc(),
        'source_root': str(ROOT), 'head_at_capture': command(['git', 'rev-parse', 'HEAD']),
        'origin_main_at_capture': command(['git', 'rev-parse', 'origin/main']),
        'branch_at_capture': command(['git', 'branch', '--show-current']),
        'prior_binary_sha256': baseline['binary_sha256'],
        'prior_source_identity': str(PRIOR), 'prior_source_identity_sha256': digest(PRIOR),
        'prior_source_input_count': len(prior['source_files']),
        'source_input_count': len(files), 'source_files': files,
        'source_inventory_sha256': inventory_digest(files), 'removed_prior_inputs': removed,
        'new_inputs_since_prior': sorted(set(files) - set(prior['source_files'])),
        'changed_inputs_since_prior': sorted(path for path in set(files) & set(prior['source_files'])
                                             if files[path] != prior['source_files'][path]),
        'binary': str(ROOT / 'target/release/lastdbd'), 'binary_sha256': None,
        'build': {'command': ['scripts/ci/with-fold-host-cargo-lock.sh', '--timeout', '600',
                             '--', 'cargo', 'build', '--release', '--locked', '-p',
                             'lastdb_node', '--bin', 'lastdbd'],
                  'profile': 'release', 'feature_arguments': [], 'locked': True,
                  'host_settings': host_settings()},
        'discovery': {'explicit_roots': CODE_ROOTS, 'explicit_root_files': ROOT_FILES,
                      'build_helpers': BUILD_FILES, 'git_inventory': 'cached + untracked, ignored excluded',
                      'selection': 'All prior inputs, all Rust/Cargo/build settings, and source/assets/resources files in explicit roots.',
                      'scope_limit': 'This is a conservative repository-input superset. External registry dependencies use Cargo.lock; their installed source bytes are not copied.'},
        'source_mismatches_after_build': None,
    }
    if manifest != previous_manifest and previous_manifest.is_file():
        previous_attempt = json.loads(previous_manifest.read_text())
        data['previous_attempt'] = {
            'manifest': str(previous_manifest), 'manifest_sha256': digest(previous_manifest),
            'binary_sha256': previous_attempt['binary_sha256'],
            'source_inventory_sha256': previous_attempt['source_inventory_sha256'],
            'role': 'Preserved earlier diagnostic candidate; see the local attempt report for runtime results.',
        }
    write_manifest(data, manifest)
    print(json.dumps({'state': data['state'], 'source_input_count': len(files),
                      'source_inventory_sha256': data['source_inventory_sha256'],
                      'new_inputs': len(data['new_inputs_since_prior']),
                      'removed_prior_inputs': removed}), flush=True)


def parents_match(data):
    for _ in range(16):
        parent = data.get('previous_attempt')
        if parent is None:
            return True
        path = Path(parent['manifest'])
        if not path.is_file() or digest(path) != parent['manifest_sha256']:
            return False
        data = json.loads(path.read_text())
    return False


def verify(manifest, expected_binary=None, finalize=False):
    data = json.loads(manifest.read_text())
    files, removed = source_snapshot()
    expected = data['source_files']
    mismatches = sorted(path for path in set(files) | set(expected)
                        if files.get(path) != expected.get(path))
    result = {'checked_utc': utc(), 'source_input_count': len(files),
              'source_mismatches': mismatches, 'removed_prior_inputs_match': removed == data['removed_prior_inputs'],
              'prior_identity_matches': digest(PRIOR) == data['prior_source_identity_sha256'],
              'previous_attempt_identities_match': parents_match(data),
              'host_settings_match': host_settings() == data['build']['host_settings']}
    if expected_binary is not None:
        result['binary_sha256'] = digest(Path(data['binary']))
        result['binary_sha256_matches'] = result['binary_sha256'] == expected_binary
    passed = not mismatches and all(value for key, value in result.items() if key.endswith('_match') or key.endswith('_matches'))
    result['passed'] = passed
    if finalize and passed:
        data.update(state='release_verified', binary_sha256=result['binary_sha256'],
                    finalized_utc=result['checked_utc'], source_mismatches_after_build=[],
                    source_inputs_verified_after_build=len(files), final_verification=result)
        write_manifest(data, manifest)
    print(json.dumps(result, sort_keys=True), flush=True)
    if not passed:
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['capture', 'verify', 'finalize'])
    parser.add_argument('--sha256')
    parser.add_argument('--manifest', type=Path, default=MANIFEST)
    parser.add_argument('--previous-attempt-manifest', type=Path, default=MANIFEST)
    args = parser.parse_args()
    manifest = args.manifest.resolve()
    if args.mode == 'capture':
        capture(manifest, args.previous_attempt_manifest.resolve())
    else:
        if args.mode == 'finalize' and not args.sha256:
            parser.error('finalize requires the completed release SHA256')
        verify(manifest, args.sha256, args.mode == 'finalize')


if __name__ == '__main__':
    main()
