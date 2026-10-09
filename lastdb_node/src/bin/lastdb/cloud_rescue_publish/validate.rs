//! Manifest and candidate validation and page grouping for the S0 publisher. Moved verbatim from `cloud_rescue_publish.rs`.

use super::*;

pub(super) fn validate_manifest(plan: &RescuePlan) -> Result<(), String> {
    let manifest = &plan.manifest;
    validate_manifest_chain(None, manifest)
        .map_err(|error| format!("invalid S0 rescue root manifest: {error}"))?;
    if plan.version != 2
        || manifest.counter == 0
        || manifest.mutable_chunks.is_empty() && manifest.atom_chunks.is_empty()
        || manifest.previous_manifest_sha256.is_some()
        || !manifest
            .mutable_chunks
            .iter()
            .any(|chunk| chunk.collection == PIN_LOG_NAMESPACE)
        || !manifest.named_holes.is_empty()
        || !manifest.deletion_receipts.is_empty()
        || !manifest.b2_cas_blob_refs.is_empty()
        || manifest
            .mutable_chunks
            .len()
            .saturating_add(manifest.atom_chunks.len())
            > MAX_CHUNKS
        || cloud_db_hash_for_store_uuid(&manifest.store_uuid) != plan.db_hash
        || manifest_sha256_hex(manifest).map_err(|error| error.to_string())? != plan.manifest_sha256
    {
        return Err("S0 rescue plan is incomplete or has a different store identity".into());
    }
    plan.manifest_bytes()?;
    Ok(())
}

/// A root cut has no predecessor atoms to retire. The LastStore cut can still
/// report old, locally absent purged SHAs from its pending retirement file.
pub(super) fn omit_root_cut_retirement_receipts(
    manifest: &mut BackupManifest,
) -> Result<(), String> {
    if manifest.previous_manifest_sha256.is_some() {
        return Err("S0 rescue cut is not a root manifest".into());
    }
    if manifest.deletion_receipts.len() > 1 {
        return Err("S0 rescue root cut has multiple deletion receipts".into());
    }
    let local_atom_shas: BTreeSet<&str> = manifest
        .atom_chunks
        .iter()
        .map(|chunk| chunk.sha256.as_str())
        .collect();
    for receipt in &manifest.deletion_receipts {
        let expected = BackupDeletionReceipt::new_purged_atom_retirement(
            0,
            manifest.counter,
            receipt.retired_atom_chunk_shas.clone(),
            receipt.authorized_at_unix_secs,
        );
        if receipt != &expected
            || receipt.authorized_at_unix_secs == 0
            || receipt.retired_atom_chunk_shas.is_empty()
            || receipt
                .retired_atom_chunk_shas
                .iter()
                .any(|sha| local_atom_shas.contains(sha.as_str()))
        {
            return Err("S0 rescue root cut has an unsupported deletion receipt".into());
        }
    }
    manifest.deletion_receipts.clear();
    Ok(())
}

pub(super) fn checked_file(
    data_root: &Path,
    candidate: &BackupChunkUploadCandidate,
) -> Result<File, String> {
    if candidate.chunk.bytes == 0 || candidate.chunk.bytes > MAX_CHUNK_BYTES {
        return Err(format!(
            "S0 rescue chunk {} exceeds the 128 MiB service limit",
            candidate.chunk.sha256
        ));
    }
    let meta = fs::symlink_metadata(&candidate.path)
        .map_err(|error| format!("S0 rescue chunk metadata unavailable: {error}"))?;
    if !meta.file_type().is_file() || meta.len() < candidate.chunk.bytes {
        return Err("S0 rescue chunk is not a complete regular file".into());
    }
    let canonical_root = data_root
        .canonicalize()
        .map_err(|error| format!("resolve stopped-copy data path: {error}"))?;
    let canonical_path = candidate
        .path
        .canonicalize()
        .map_err(|error| format!("resolve S0 rescue chunk path: {error}"))?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err("S0 rescue chunk escapes the stopped copy".into());
    }
    let mut file =
        File::open(&candidate.path).map_err(|error| format!("open S0 rescue chunk: {error}"))?;
    let mut limited = (&mut file).take(candidate.chunk.bytes);
    let mut hasher = Sha256::new();
    let mut copied = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = limited
            .read(&mut buffer)
            .map_err(|error| format!("hash S0 rescue chunk: {error}"))?;
        if read == 0 {
            break;
        }
        copied += read as u64;
        hasher.update(&buffer[..read]);
    }
    if copied != candidate.chunk.bytes
        || format!("{:x}", hasher.finalize()) != candidate.chunk.sha256
    {
        return Err(format!(
            "S0 rescue chunk {} byte SHA-256 mismatch",
            candidate.chunk.sha256
        ));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("rewind S0 rescue chunk: {error}"))?;
    Ok(file)
}

pub(super) fn validate_candidates(
    home: &Path,
    store: &fold_db::storage::LastStoreNamespacedStore,
    manifest: &BackupManifest,
) -> Result<BTreeMap<String, BackupChunkUploadCandidate>, String> {
    let scan = store
        .scan_backup_chunks(None)
        .map_err(|error| format!("scan stopped-copy chunks: {error}"))?;
    if !scan.unresolvable.is_empty() {
        return Err(format!(
            "stopped copy has {} unresolvable chunk(s)",
            scan.unresolvable.len()
        ));
    }
    let expected = ordered_refs(
        manifest
            .mutable_chunks
            .iter()
            .chain(&manifest.atom_chunks)
            .cloned(),
    )?;
    let actual = ordered_refs(scan.candidates.iter().map(|item| item.chunk.clone()))?;
    if actual != expected {
        return Err("saved S0 rescue manifest does not match stopped-copy chunks".into());
    }
    let mut by_sha = BTreeMap::new();
    for candidate in scan.candidates {
        checked_file(&home.join("data"), &candidate)?;
        if let Some(previous) = by_sha.insert(candidate.chunk.sha256.clone(), candidate.clone()) {
            if previous.chunk.bytes != candidate.chunk.bytes {
                return Err("S0 rescue digest names different chunk sizes".into());
            }
        }
    }
    Ok(by_sha)
}

pub(super) fn page_groups(
    candidates: &BTreeMap<String, BackupChunkUploadCandidate>,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let mut pages: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for sha in candidates.keys() {
        let prefix = sha
            .get(..2)
            .ok_or("S0 rescue chunk has an invalid SHA-256")?;
        pages
            .entry(prefix.to_string())
            .or_default()
            .push(sha.clone());
    }
    if pages.values().any(|chunks| chunks.len() > 512) {
        return Err("S0 rescue page has more than 512 chunks".into());
    }
    Ok(pages)
}

pub(super) fn page_marker(plan: &RescuePlan, prefix: &str, chunks: &[String]) -> RescuePageMarker {
    RescuePageMarker {
        version: 1,
        manifest_sha256: plan.manifest_sha256.clone(),
        prefix: prefix.to_string(),
        chunks_sha256: sha256_hex(chunks.join("\n").as_bytes()),
    }
}

pub(super) fn split_page_chunks<'a>(
    chunks: &'a [String],
    candidates: &BTreeMap<String, BackupChunkUploadCandidate>,
) -> Result<(Vec<&'a String>, Vec<&'a String>), String> {
    let mut small = Vec::new();
    let mut large = Vec::new();
    for sha in chunks {
        let candidate = candidates
            .get(sha)
            .ok_or("S0 rescue page names a missing local chunk")?;
        if candidate.chunk.bytes >= ISOLATED_CONFIRM_MIN_BYTES {
            large.push(sha);
        } else {
            small.push(sha);
        }
    }
    Ok((small, large))
}
