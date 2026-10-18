#!/usr/bin/env python3
"""Recover eight exact baseline texts into proof files, with SHA256 checks."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
PROOF = Path(__file__).resolve().parent
DURABLE = ROOT / 'docs/proofs/schema-preload-durable-review-fix-20261017/source-inputs'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError('Inverse reconstruction anchor is not unique: ' + old[:80])
    return text.replace(old, new, 1)


def old_write(text):
    text = replace_once(text, '    batch_bytes: u64,\n', '')
    start = text.index('/// New operations and replacement quota;')
    end = text.index('/// Durable native boundary', start)
    text = text[:start] + text[end:]
    start = text.index('    pub(super) fn prepare_commit(')
    end = text.index('        if self.failed.load(Ordering::Acquire) {', start)
    signature = '''    pub(super) fn prepare_commit(
        &self,
        operations: Vec<HomeMutation>,
    ) -> StorageResult<PreparedCommit> {
'''
    text = text[:start] + signature + text[end:]
    old = '''        let limit = writer.current().batch_operation_limit();
        let batch_bytes = prefix_batch_bytes(previous, &operations, limit)?;
        let reservation = match previous {
            Some(previous) => writer.reserve_mutation_extension(&previous.reservation, &operations),
            None => writer.reserve_mutation(&operations),
        }
        .map_err(map)?;
'''
    new = '''        transaction::validate(&operations, writer.current().batch_operation_limit())?;
        let reservation = writer.reserve_mutation(&operations).map_err(map)?;
'''
    text = replace_once(text, old, new)
    text = replace_once(text, '            batch_bytes,\n', '')
    text = replace_once(text, '            batch_bytes: _,\n', '')
    text = text[:text.index('\nfn prefix_batch_bytes(')]
    return text


def old_mutation_quote(text):
    start = text.index('    /// Extend a prefix from the new request')
    end = text.index('    /// Exact journal dimensions', start)
    text = text[:start] + text[end:]
    start = text.index('fn physical_bytes(journal: JournalQuote, map_pages: u64)')
    end = text.index('fn overflow(bytes: usize)', start)
    text = text[:start] + text[end:]
    old = '''        Ok(MutationQuote {
            journal,
            map_pages,
            dependency_parts,
            physical_bytes: physical_bytes(journal, map_pages)?,
        })
'''
    new = '''        // Publication and stream rotation each save a complete receipt.
        let receipt_pages = 1 + (65_536u64 - 150).div_ceil(201);
        let metadata_bytes = map_pages
            .checked_add(2 * receipt_pages)
            .and_then(|pages| pages.checked_mul(PAGE_BYTES as u64))
            .ok_or_else(|| corrupt("complete mutation metadata reserve overflow"))?;
        let physical_bytes = add(add(journal.physical_reserve(), metadata_bytes)?, 8 * 4096)?;
        Ok(MutationQuote {
            journal,
            map_pages,
            dependency_parts,
            physical_bytes,
        })
'''
    return replace_once(text, old, new)


def main():
    baseline_path = PROOF / 'before-source.json'
    baseline = json.loads(baseline_path.read_text())['source_files']
    records, contents = {}, {}
    names = ['fold_db/crates/core/src/storage/disk_pack/' + name
             for name in ['queue.rs', 'write.rs', 'transaction.rs']]
    names += ['vendor/laststore/src/disk_pack/' + name for name in
              ['home/admission.rs', 'home/mutation_quote.rs', 'journal/admission.rs',
               'journal/quote.rs', 'journal/writer.rs']]
    for name in names:
        if name.endswith('/queue.rs') or name.endswith('/transaction.rs'):
            source = DURABLE / name
            data, method = source.read_bytes(), 'exact preserved source'
        elif name.endswith('/write.rs'):
            source = PROOF / 'after-source-inputs' / (name + '.txt')
            data = old_write(source.read_text()).encode()
            method = 'inverse of append-only PreparedAppend and prefix byte accounting changes'
        elif name.endswith('/mutation_quote.rs'):
            source = PROOF / 'after-source-inputs' / (name + '.txt')
            data = old_mutation_quote(source.read_text()).encode()
            method = 'inverse of extension method and extracted physical-byte helper'
        else:
            source = 'git HEAD:' + name
            data = subprocess.run(['git', 'show', 'HEAD:' + name], cwd=ROOT,
                                  capture_output=True, check=True).stdout
            method = 'exact tracked HEAD source'
        expected = baseline[name]
        actual = {'bytes': len(data), 'sha256': digest(data)}
        if actual != expected:
            raise RuntimeError(name + ': reconstructed identity differs: ' + json.dumps(actual))
        records[name] = {**actual, 'expected_sha256': expected['sha256'], 'matches_baseline': True,
                         'source': str(source), 'recovery_method': method}
        contents[name] = data
    target = PROOF / 'before-source-inputs'
    target.mkdir(exist_ok=False)
    for name, data in contents.items():
        path = target / (name + '.txt')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    manifest = {'captured_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'baseline_inventory': str(baseline_path),
                'baseline_inventory_sha256': digest(baseline_path.read_bytes()),
                'recovery_script_sha256': digest(Path(__file__).read_bytes()),
                'files': records, 'verified_files': len(records), 'missing_files': [],
                'product_files_changed': False}
    (target / 'manifest.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'target': str(target), 'verified_files': len(records), 'missing_files': []}))


if __name__ == '__main__':
    main()
