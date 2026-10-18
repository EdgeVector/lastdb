#!/usr/bin/env python3
"""Preserve the uncovered source facts and prepare the approved paired repeat."""
import hashlib
import json
from pathlib import Path
import shutil

BASE = Path('/Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016')
HERE = Path(__file__).resolve().parent
FILES = [
    'fold_db/crates/core/src/storage/disk_pack/read/snapshots.rs',
    'fold_db/crates/core/src/storage/disk_pack/logical_paths/source.rs',
    'fold_db/crates/core/src/storage/disk_pack/transaction/request.rs',
    'fold_db/crates/core/src/storage/disk_pack/queue.rs',
    'fold_db/crates/core/src/fold_db_core/mutation_manager/write/native_paths.rs',
    'fold_db/crates/core/src/fold_db_core/mutation_manager/write/native.rs',
    'fold_db/crates/core/src/fold_db_core/mutation_flush.rs',
    'fold_db/crates/core/src/db_operations/core/open.rs',
    'fold_db/crates/core/src/resident/config.rs',
    'vendor/laststore/src/disk_pack/home/build.rs',
]


def save(path, value):
    with path.open('x') as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write('\n')


def main():
    prior = json.loads((HERE / 'before-source.json').read_text())['source_files']
    later = json.loads((HERE / 'after-source.json').read_text())['source_files']
    facts = {}
    for name in FILES:
        source = BASE / name
        target = HERE / 'uncovered-source-inputs' / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        facts[name] = {'captured_sha256': digest, 'before': prior[name], 'after': later[name],
                       'captured_matches_before': digest == prior[name]['sha256'],
                       'captured_matches_after': digest == later[name]['sha256']}
    save(HERE / 'uncovered-source-facts.json', {
        'source_inputs': facts,
        'source_only_limit': 'No runtime fallback reason is exposed by the public API. The live cases observed zero accepted batches; this source chain explains the missing path.',
        'chain': [
            'transaction/request.rs:65-69 prepares each native mutation in WRITE.scope(pending, future).',
            'read/snapshots.rs:15-17 refuses an active snapshot context if PendingBatch::current is Some.',
            'logical_paths/source.rs:115-118 returns no source receipts when read_snapshot_active is false.',
            'logical_paths/source.rs:283-292 consequently cannot certify a full atom in that write scope.',
            'native_paths.rs:174-182 returns None for a nonempty changed tip, then line 65 requires durable completion.',
            'transaction/request.rs:72-74 removes the memory reply delay, then lines 83-89 wait for the caller durable target.',
        ],
        'operational_result': 'All eight original cases completed with correct normal-reopen readback, no mutation errors or refusals, and zero visible accepted batches.',
        'optimization_comparison_eligible': False,
        'configuration_limit': 'No ordinary API or environment flag bypasses the unconditional PendingBatch snapshot guard.',
    })
    followup = HERE / 'eligible'
    followup.mkdir()
    benchmark = (HERE / 'benchmark.py').read_text()
    benchmark = benchmark.replace("parser.add_argument('--expected-sha256', required=True)",
        "parser.add_argument('--expected-sha256', required=True)\n    parser.add_argument('--source-identity', type=Path, required=True)")
    benchmark = benchmark.replace("'baseline_source_reference': str(HERE / 'before-source.json')\n            if own.label == 'before' else 'The parent preserves the replacement build source identity.'", 
        "'source_identity_reference': str(own.source_identity.resolve(strict=True)),\n        'source_identity_sha256': measure.profile.file_hash(own.source_identity)")
    (followup / 'benchmark.py').write_text(benchmark)
    shutil.copy2(HERE / 'summarize.py', followup / 'summarize.py')
    shutil.copy2(HERE / 'relevant-nonsecret-environment.json', followup / 'relevant-nonsecret-environment.json')
    cleanup = (BASE / 'docs/proofs/schema-preload-product-20261017/cleanup_owned.py').read_text()
    cleanup = cleanup.replace("sockets = [str(root / 'b' / name) for name in ('q.sock', 'f.sock')\n               if (root / 'b' / name).exists()]", 
        "directories = {Path(row['home']).parent for row in starts}\n    if any(path.parent != root for path in directories):\n        raise RuntimeError('A recorded node directory escapes the exact owned root')\n    sockets = [str(path / name) for path in sorted(directories)\n               for name in ('q.sock', 'f.sock') if (path / name).exists()]")
    (HERE / 'cleanup_owned.py').write_text(cleanup)


if __name__ == '__main__':
    main()
