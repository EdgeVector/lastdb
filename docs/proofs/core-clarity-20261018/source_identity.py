#!/usr/bin/env python3
"""Capture a frozen native cutover release without a new comparison baseline."""
import importlib.util
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SHARED = HERE.parent / 'core-memory-completion-20261018/source_identity.py'
SPEC = importlib.util.spec_from_file_location('clarity_source_inventory', SHARED)
base = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(base)
base.HERE = HERE
base.ROOT = ROOT
base.PRIOR = HERE.parent / 'core-owned-writes-20261018/candidate-v6-identity.json'
base.MANIFEST = HERE / 'candidate-identity.json'


def helper_identity():
    return {str(path): {'sha256': base.digest(path), 'bytes': path.stat().st_size}
            for path in (Path(__file__).resolve(), SHARED)}


def capture(manifest, previous_manifest):
    if manifest.exists():
        raise RuntimeError('The source identity exists; use verify/finalize')
    files, removed = base.source_snapshot()
    second, second_removed = base.source_snapshot()
    if files != second or removed != second_removed:
        raise RuntimeError('A product input changes during capture')
    prior = json.loads(base.PRIOR.read_text())
    data = {
        'version': 1, 'state': 'sources_frozen_binary_pending',
        'captured_utc': base.utc(), 'source_root': str(ROOT),
        'head_at_capture': base.command(['git', 'rev-parse', 'HEAD']),
        'tree_at_capture': base.command(['git', 'rev-parse', 'HEAD^{tree}']),
        'origin_main_at_capture': base.command(['git', 'rev-parse', 'origin/main']),
        'branch_at_capture': base.command(['git', 'branch', '--show-current']),
        'prior_binary_sha256': prior['binary_sha256'],
        'prior_source_identity': str(base.PRIOR),
        'prior_source_identity_sha256': base.digest(base.PRIOR),
        'prior_identity_role': 'Historical V6 source inventory reference only. No before/after benchmark, executable copy, or existing database is required.',
        'prior_source_input_count': len(prior['source_files']),
        'source_input_count': len(files), 'source_files': files,
        'source_inventory_sha256': base.inventory_digest(files),
        'removed_prior_inputs': removed,
        'new_inputs_since_prior': sorted(set(files) - set(prior['source_files'])),
        'changed_inputs_since_prior': sorted(path for path in set(files) & set(prior['source_files'])
                                            if files[path] != prior['source_files'][path]),
        'binary': str(ROOT / 'target/release/lastdbd'), 'binary_sha256': None,
        'identity_helpers': helper_identity(),
        'build': {'command': ['scripts/ci/with-fold-host-cargo-lock.sh', '--timeout', '600',
            '--', 'cargo', 'build', '--release', '--locked', '-p', 'lastdb_node', '--bin', 'lastdbd'],
            'profile': 'release', 'feature_arguments': [], 'locked': True,
            'host_settings': base.host_settings()},
        'discovery': {'explicit_roots': base.CODE_ROOTS,
            'explicit_root_files': base.ROOT_FILES, 'build_helpers': base.BUILD_FILES,
            'git_inventory': 'cached + untracked; ignored excluded',
            'selection': 'Prior inputs plus Rust/Cargo/build and source/assets/resources in explicit roots.',
            'scope_limit': 'Conservative repository superset. Cargo.lock identifies registry dependencies; installed registry source bytes are not copied.'},
        'source_mismatches_after_build': None,
    }
    if manifest != previous_manifest and previous_manifest.is_file():
        previous = json.loads(previous_manifest.read_text())
        data['previous_attempt'] = {'manifest': str(previous_manifest),
            'manifest_sha256': base.digest(previous_manifest),
            'binary_sha256': previous.get('binary_sha256'),
            'source_inventory_sha256': previous['source_inventory_sha256'],
            'role': 'Retained earlier native cutover attempt; its operational result remains separate.'}
    base.write_manifest(data, manifest)
    print(json.dumps({'state': data['state'], 'source_input_count': len(files),
        'head': data['head_at_capture'], 'tree': data['tree_at_capture'],
        'source_inventory_sha256': data['source_inventory_sha256']}), flush=True)


verify_original = base.verify


def verify(manifest, expected_binary=None, finalize=False):
    data = json.loads(manifest.read_text())
    if data['identity_helpers'] != helper_identity():
        raise RuntimeError('The source identity helper differs from capture')
    verify_original(manifest, expected_binary, finalize)


base.capture = capture
base.verify = verify

if __name__ == '__main__':
    base.main()
