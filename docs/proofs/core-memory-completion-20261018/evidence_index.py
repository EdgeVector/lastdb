#!/usr/bin/env python3
"""Index exact local proof files and propose a compact Git evidence set."""
import collections
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OUTPUTS = {'evidence-index.json', 'evidence-summary.json'}
IGNORE_RULES = '''# Generated raw evidence stays local; hashes and sizes are in evidence-index.json.
/docs/proofs/**/*.jsonl
/docs/proofs/**/*.log
/docs/proofs/**/*.log.gz
/docs/proofs/**/*.stdout
/docs/proofs/**/*.stderr
/docs/proofs/**/*.time
/docs/proofs/**/sample.txt
/docs/proofs/**/source-inputs/
/docs/proofs/**/*-source-inputs/
/docs/proofs/**/*.py
!/docs/proofs/*/*.py
!/docs/proofs/graph-first-20261017/drivers/*.py
/docs/proofs/**/source-identity.json
/docs/proofs/**/receipt-manifest.json
/docs/proofs/**/current-source-pending*.json
/docs/proofs/schema-range-preload-20261017/keys-*.json
/docs/proofs/**/*-source.json
/docs/proofs/**/*source-freeze.json
/docs/proofs/**/source.json
/docs/proofs/**/source-inputs.json
/docs/proofs/core-memory-state-20261018/current-source*.json
/docs/proofs/core-memory-state-20261018/before-source-prior.json
!/docs/proofs/core-memory-state-20261018/before-source.json
!/docs/proofs/core-memory-state-20261018/current-source-v4.json
/docs/proofs/*/disk-component/**/read*.json
/docs/proofs/*/disk-component/**/range*.json
'''


def listed(*flags):
    result = subprocess.run(['git', 'ls-files', *flags, 'docs/proofs'],
                            cwd=ROOT, capture_output=True, text=True, check=True)
    return result.stdout.splitlines()


def file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def disposition(path):
    parts, name = Path(path).parts, Path(path).name
    if path.suffix == '.jsonl':
        return 'local', 'raw request and generated value receipts'
    if name == 'sample.txt' or name.endswith(('.log', '.log.gz', '.stdout', '.stderr', '.time')):
        return 'local', 'raw node, build, driver or timing output'
    if any(part == 'source-inputs' or part.endswith('-source-inputs') for part in parts):
        return 'local', 'preserved duplicate source input bytes'
    if path.suffix == '.py' and len(parts) > 4:
        if parts[:4] != ('docs', 'proofs', 'graph-first-20261017', 'drivers'):
            return 'local', 'per-case duplicate executed driver; shared driver stays in Git'
    if name in ('source-identity.json', 'receipt-manifest.json'):
        return 'local', 'per-case duplicate source or receipt identity'
    if name.startswith('current-source-pending') and path.suffix == '.json':
        return 'local', 'intermediate source identity before executable build'
    if parts[2] == 'schema-range-preload-20261017' and name.startswith('keys-') and path.suffix == '.json':
        return 'local', 'generated range-key payload; representation summary stays in Git'
    final_sources = {'docs/proofs/core-memory-state-20261018/before-source.json',
                     'docs/proofs/core-memory-state-20261018/current-source-v4.json'}
    source_dump = (name.endswith(('-source.json', 'source-freeze.json')) or
                   name in ('source.json', 'source-inputs.json') or
                   (parts[2] == 'core-memory-state-20261018' and
                    (name.startswith('current-source') or name == 'before-source-prior.json')))
    if source_dump and str(path) not in final_sources:
        return 'local', 'intermediate or duplicate full source manifest; final identity summaries stay in Git'
    if 'disk-component' in parts and name.startswith(('read', 'range')) and path.suffix == '.json':
        return 'local', 'raw generated key/value file component result; component summary stays in Git'
    return 'git', 'report, shared driver, result, identity or cleanup summary'


def main():
    (HERE / 'proposed-ignore.txt').write_text(IGNORE_RULES)
    paths = sorted(set(listed('--cached') + listed('--others', '--exclude-standard')
                       + listed('--others', '--ignored', '--exclude-standard')))
    at_head = subprocess.run(['git', 'ls-tree', '-r', '--name-only', 'HEAD', '--', 'docs/proofs'],
        cwd=ROOT, capture_output=True, text=True, check=True)
    existing = set(at_head.stdout.splitlines())
    checked = subprocess.run(['git', 'check-ignore', '--no-index', '--stdin'],
        input='\n'.join(paths) + '\n', cwd=ROOT, capture_output=True, text=True, check=False)
    if checked.returncode not in (0, 1):
        raise RuntimeError(checked.stderr)
    ignored = set(checked.stdout.splitlines())
    rows, totals = [], collections.defaultdict(lambda: {'files': 0, 'bytes': 0})
    for name in paths:
        path = Path(name)
        if path.parent == HERE.relative_to(ROOT) and path.name in OUTPUTS:
            continue
        source = ROOT / path
        if not source.is_file() or source.is_symlink():
            continue
        keep, reason = disposition(path)
        size = source.stat().st_size
        if keep == 'git' and size > 2 * 1024 * 1024:
            raise RuntimeError('The proposed Git set still exceeds the artifact limit: ' + name)
        rows.append({'path': name, 'bytes': size, 'sha256': file_hash(source),
                     'disposition': keep, 'reason': reason, 'git_ignored': name in ignored,
                     'tracked_at_head': name in existing})
        totals[keep]['files'] += 1
        totals[keep]['bytes'] += size
    result = {'version': 1, 'local_root': str(ROOT),
        'scope': 'Original local files stay in place. This index proposes disposition; root must review ignore rules before stage. This index excludes its own generated outputs.',
        'sha256_scope': 'Exact current bytes for each relative path below local_root.',
        'files': rows}
    (HERE / 'evidence-index.json').write_text(json.dumps(result, separators=(',', ':')) + '\n')
    summary = {'totals': dict(totals), 'indexed_files': len(rows),
        'index_bytes': (HERE / 'evidence-index.json').stat().st_size,
        'proposed_rules': IGNORE_RULES.splitlines(),
        'rule_mismatches': [{'path': row['path'], 'disposition': row['disposition'],
            'git_ignored': row['git_ignored']} for row in rows
            if (row['disposition'] == 'local') != row['git_ignored']],
        'new_git_evidence': {'files': sum(row['disposition'] == 'git' and not row['tracked_at_head'] for row in rows),
            'bytes': sum(row['bytes'] for row in rows if row['disposition'] == 'git' and not row['tracked_at_head'])},
        'remaining_limit': 'Relative links into local raw evidence require the index note. Raw files are not uploaded or deleted.'}
    (HERE / 'evidence-summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    print(json.dumps(summary['totals'], sort_keys=True))


if __name__ == '__main__':
    main()
