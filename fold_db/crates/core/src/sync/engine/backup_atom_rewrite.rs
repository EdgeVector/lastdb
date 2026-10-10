//! Content-preserving replacement of plain backup chunks after an owner erase.
//!
//! This module does not authorize an erase. The owner must
//! first prove that each requested atom is no longer live, then upload every
//! replacement before the ordinary manifest CAS. Unrelated encoded records are
//! copied byte for byte, including their version order and delete markers.

use crate::atom::atom_key_codec;
use crate::hex::sha256_hex;
use crate::storage::laststore::{
    manifest_sha256_hex, BackupChunkRef, BackupDeletionReceipt, BackupManifest, BackupManifestRole,
};
use crate::sync::error::{SyncError, SyncResult};
use std::collections::BTreeSet;

const MAX_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_ATOMS: usize = 64;

#[derive(Debug)]
pub struct BackupAtomChunkRewrite {
    original: BackupChunkRef,
    replacement: BackupChunkRef,
    bytes: Vec<u8>,
    removed_records: usize,
}

impl BackupAtomChunkRewrite {
    #[must_use]
    pub fn original(&self) -> &BackupChunkRef {
        &self.original
    }
    /// An empty replacement removes the whole chunk through its receipt.
    #[must_use]
    pub fn replacement(&self) -> Option<&BackupChunkRef> {
        (!self.bytes.is_empty()).then_some(&self.replacement)
    }
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    #[must_use]
    pub fn removed_records(&self) -> usize {
        self.removed_records
    }
}

fn invalid(reason: &str) -> SyncError {
    SyncError::Storage(format!("backup atom rewrite: {reason}"))
}

pub(super) fn replacement_uuid(original: &str) -> SyncResult<String> {
    let old = uuid::Uuid::parse_str(original).map_err(|_| invalid("invalid source UUID"))?;
    let next = old
        .as_u128()
        .checked_add(1)
        .ok_or_else(|| invalid("source UUID exhausted"))?;
    Ok(uuid::Uuid::from_u128(next).to_string())
}

fn take<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> SyncResult<&'a [u8]> {
    let end = pos
        .checked_add(len)
        .ok_or_else(|| invalid("length overflow"))?;
    let out = bytes
        .get(*pos..end)
        .ok_or_else(|| invalid("truncated record"))?;
    *pos = end;
    Ok(out)
}

/// Remove only exact atom-body keys from a digest-verified plain segment.
/// A framed segment or a damaged record refuses the entire operation.
pub fn rewrite_plain_backup_atom_chunk(
    chunk: &BackupChunkRef,
    bytes: &[u8],
    atom_ids: &BTreeSet<String>,
) -> SyncResult<Option<BackupAtomChunkRewrite>> {
    if chunk.role != BackupManifestRole::Atom || chunk.collection != "atoms" {
        return Err(invalid("not an atom chunk"));
    }
    if chunk.pack.is_some() {
        return Err(invalid("packed atom rewrite is unsupported"));
    }
    if atom_ids.is_empty()
        || atom_ids.len() > MAX_ATOMS
        || atom_ids.iter().any(|id| {
            id.len() != 64
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
    {
        return Err(invalid("invalid atom selection"));
    }
    if bytes.len() > MAX_CHUNK_BYTES
        || bytes.len() as u64 != chunk.bytes
        || sha256_hex(bytes) != chunk.sha256
    {
        return Err(invalid("input bytes do not match the manifest"));
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut pos = 0;
    let mut removed = 0;
    while pos < bytes.len() {
        let start = pos;
        let op = take(bytes, &mut pos, 1)?[0];
        let len = u16::from_le_bytes(take(bytes, &mut pos, 2)?.try_into().unwrap()) as usize;
        let key = std::str::from_utf8(take(bytes, &mut pos, len)?)
            .map_err(|_| invalid("invalid key encoding"))?;
        match op {
            1 => {
                let len =
                    u32::from_le_bytes(take(bytes, &mut pos, 4)?.try_into().unwrap()) as usize;
                take(bytes, &mut pos, len)?;
            }
            2 => {}
            _ => return Err(invalid("unsupported segment format")),
        }
        // Owner /api/atom absence proves only the main namespace. A matching
        // digest in another namespace can still serve an unrelated live row.
        // Preserve all namespaced bodies; never broaden a main-namespace erase.
        // uuid_of peels a short org prefix (`tenant:atom:{id}`), so also
        // require the raw key to start with atom: or atom\0.
        let selected = crate::kind_partition::starts_with_kind(key, "atom")
            && atom_key_codec::uuid_of(key).is_some_and(|id| atom_ids.contains(id));
        if selected {
            removed += 1;
        } else {
            out.extend_from_slice(&bytes[start..pos]);
        }
    }
    if removed == 0 {
        return Ok(None);
    }
    let mut replacement = chunk.clone();
    replacement.sha256 = sha256_hex(&out);
    replacement.bytes = out.len() as u64;
    replacement.pack = None;
    // A replacement is a new chunk identity. Reusing the UUID would bypass
    // the chain's removed-chunk receipt check (which keys on chunk identity).
    // Adjacent identity preserves restore order unless another chunk occupies
    // that identity. Manifest preparation checks both collision and ordering.
    replacement.chunk_uuid = replacement_uuid(&chunk.chunk_uuid)?;
    Ok(Some(BackupAtomChunkRewrite {
        original: chunk.clone(),
        replacement,
        bytes: out,
        removed_records: removed,
    }))
}

/// Prepare an exact successor after the caller verifies and uploads replacements.
/// Keep the S0 frontier and every unrelated reference unchanged. The chained
/// owner receipt covers only the exact old digests, never a whole collection.
pub fn manifest_with_atom_rewrites(
    previous: &BackupManifest,
    rewrites: &[BackupAtomChunkRewrite],
    counter: u64,
    authorized_at: u64,
) -> SyncResult<BackupManifest> {
    if rewrites.is_empty() || counter <= previous.counter || authorized_at == 0 {
        return Err(invalid("invalid successor boundary"));
    }
    let mut next = previous.clone();
    let mut retired = BTreeSet::new();
    for rewrite in rewrites {
        if !retired.insert(rewrite.original.sha256.clone())
            || rewrite.removed_records == 0
            || rewrite.replacement.sha256 != sha256_hex(&rewrite.bytes)
            || rewrite.replacement.bytes != rewrite.bytes.len() as u64
        {
            return Err(invalid("invalid or repeated replacement"));
        }
        let mut expected = rewrite.original.clone();
        expected.sha256.clone_from(&rewrite.replacement.sha256);
        expected.bytes = rewrite.replacement.bytes;
        expected.pack = None;
        expected.chunk_uuid = replacement_uuid(&rewrite.original.chunk_uuid)?;
        if expected != rewrite.replacement || expected.bytes >= rewrite.original.bytes {
            return Err(invalid("replacement changed its chunk address"));
        }
        let slots: Vec<_> = next
            .atom_chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| c.sha256 == rewrite.original.sha256)
            .map(|(i, _)| i)
            .collect();
        if slots.len() != 1 || next.atom_chunks[slots[0]] != rewrite.original {
            return Err(invalid("source chunk is absent or changed"));
        }
        if !rewrite.bytes.is_empty()
            && previous
                .atom_chunks
                .iter()
                .any(|c| c.chunk_uuid == expected.chunk_uuid)
        {
            return Err(invalid("replacement UUID collides with a prior chunk"));
        }
        if rewrite.bytes.is_empty() {
            next.atom_chunks.remove(slots[0]);
        } else {
            next.atom_chunks[slots[0]] = rewrite.replacement.clone();
        }
    }
    // Restore sorts chunks by CSN and UUID. Byte preservation alone does not
    // preserve the winning version when a replacement changes that order.
    let order = super::backup_restore::backup_restore_chunk_order;
    let mut before: Vec<_> = previous
        .atom_chunks
        .iter()
        .filter(|c| {
            !rewrites
                .iter()
                .any(|r| r.original.sha256 == c.sha256 && r.bytes.is_empty())
        })
        .collect();
    let mut after: Vec<_> = next.atom_chunks.iter().collect();
    before.sort_by_key(|c| order(c));
    after.sort_by_key(|c| order(c));
    let before_ids: Vec<_> = before.iter().map(|c| c.sha256.as_str()).collect();
    let after_ids: Vec<_> = after
        .iter()
        .map(|c| {
            rewrites
                .iter()
                .find(|r| r.replacement.sha256 == c.sha256)
                .map_or(c.sha256.as_str(), |r| r.original.sha256.as_str())
        })
        .collect();
    if before_ids != after_ids {
        return Err(invalid("replacement changes unrelated restore order"));
    }
    next.counter = counter;
    next.created_at_unix_secs = authorized_at;
    next.previous_manifest_sha256 =
        Some(manifest_sha256_hex(previous).map_err(|e| invalid(&e.to_string()))?);
    next.deletion_receipts = vec![BackupDeletionReceipt::new_purged_atom_retirement(
        previous.counter,
        counter,
        retired.into_iter().collect(),
        authorized_at,
    )];
    Ok(next)
}

/// A prepared replacement has no remote effect until the owner publishes it.
/// Its fields are private so callers cannot substitute different record bytes.
#[derive(Debug)]
pub struct PreparedBackupAtomRewrite {
    previous: BackupManifest,
    next: BackupManifest,
    rewrites: Vec<BackupAtomChunkRewrite>,
}

impl PreparedBackupAtomRewrite {
    #[must_use]
    pub fn manifest(&self) -> &BackupManifest {
        &self.next
    }
    #[must_use]
    pub fn retired_chunks(&self) -> usize {
        self.rewrites.len()
    }
    #[must_use]
    pub fn removed_records(&self) -> usize {
        self.rewrites.iter().map(|r| r.removed_records).sum()
    }
}

/// Read a pinned manifest chain and only the explicitly selected chunk digests.
/// The caller supplies owner erase intent; local absence alone is not intent.
/// No credentials or atom identities enter the returned manifest or receipt.
pub async fn prepare_cloud_backup_atom_rewrite(
    auth: &crate::sync::auth::AuthClient,
    s3: &crate::sync::s3::S3Client,
    expected_manifest: &str,
    chunk_shas: &BTreeSet<String>,
    atom_ids: &BTreeSet<String>,
    authorized_at: u64,
) -> SyncResult<PreparedBackupAtomRewrite> {
    let latest = auth.backup_latest_get().await?;
    latest.latest.require_supported_format()?;
    if latest.latest.manifest_sha256 != expected_manifest
        || chunk_shas.is_empty()
        || chunk_shas.len() > 64
    {
        return Err(invalid("stale manifest or invalid chunk selection"));
    }
    let chain = super::backup_restore::download_manifest_chain(auth, s3, expected_manifest)
        .await
        .map_err(|e| invalid(&e.to_string()))?;
    let previous = chain
        .into_iter()
        .next()
        .ok_or_else(|| invalid("empty manifest chain"))?;
    if latest.latest.format_version() != previous.version {
        return Err(invalid("latest format differs from manifest"));
    }
    let scope = crate::storage::laststore::cloud_db_hash_for_store_uuid(&previous.store_uuid);
    if auth.db_hash_scope() != Some(scope.as_str())
        || latest.latest.store_uuid != previous.store_uuid
        || latest.latest.epoch != previous.epoch
        || latest.latest.counter != previous.counter
    {
        return Err(invalid("source scope or pointer mismatch"));
    }
    let selected: Vec<_> = previous
        .atom_chunks
        .iter()
        .filter(|c| chunk_shas.contains(&c.sha256))
        .collect();
    if selected.len() != chunk_shas.len()
        || selected.iter().any(|c| c.bytes > MAX_CHUNK_BYTES as u64)
        || selected
            .iter()
            .try_fold(0u64, |sum, c| sum.checked_add(c.bytes))
            .is_none_or(|sum| sum > 128 * 1024 * 1024)
    {
        return Err(invalid("selected chunks absent or over budget"));
    }
    let mut rewrites = Vec::new();
    for chunk in selected {
        let bytes = super::backup_restore::download_backup_chunk(auth, s3, chunk).await?;
        let rewrite = rewrite_plain_backup_atom_chunk(chunk, &bytes, atom_ids)?
            .ok_or_else(|| invalid("selected chunk contains no selected main-namespace body"))?;
        rewrites.push(rewrite);
    }
    let counter = previous
        .counter
        .checked_add(1)
        .ok_or_else(|| invalid("counter overflow"))?;
    let next = manifest_with_atom_rewrites(&previous, &rewrites, counter, authorized_at)?;
    crate::storage::laststore::validate_manifest_chain(Some(&previous), &next)
        .map_err(|e| invalid(&e.to_string()))?;
    Ok(PreparedBackupAtomRewrite {
        previous,
        next,
        rewrites,
    })
}

/// Publish replacements before the exact receipt-backed manifest successor.
/// The owner must keep the primary cloud publisher durably paused throughout,
/// then atomically mirror the returned manifest before it permits new cuts.
/// Re-check the pointer before any writes and immediately before CAS. A later
/// competing counter makes this counter stale, so it cannot overwrite that cut.
pub async fn publish_cloud_backup_atom_rewrite(
    auth: &crate::sync::auth::AuthClient,
    s3: &crate::sync::s3::S3Client,
    prepared: &PreparedBackupAtomRewrite,
) -> SyncResult<BackupManifest> {
    let expected = manifest_sha256_hex(&prepared.previous).map_err(|e| invalid(&e.to_string()))?;
    let scope =
        crate::storage::laststore::cloud_db_hash_for_store_uuid(&prepared.previous.store_uuid);
    if auth.db_hash_scope() != Some(scope.as_str()) {
        return Err(invalid("publication source scope mismatch"));
    }
    let next_sha = manifest_sha256_hex(&prepared.next).map_err(|e| invalid(&e.to_string()))?;
    let latest = auth.backup_latest_get().await?;
    if latest.latest.manifest_sha256 == next_sha && latest.latest.counter == prepared.next.counter {
        return Ok(prepared.next.clone());
    }
    if latest.latest.manifest_sha256 != expected {
        return Err(invalid("cloud tip changed before upload"));
    }
    for rewrite in &prepared.rewrites {
        let Some(chunk) = rewrite.replacement() else {
            continue;
        };
        let presign = auth
            .presign_backup_chunk_upload(&chunk.sha256, chunk.bytes)
            .await?;
        if !presign.already_present {
            let url = presign
                .url
                .ok_or_else(|| invalid("chunk upload URL absent"))?;
            s3.upload_snapshot(&url, rewrite.bytes.clone()).await?;
            auth.confirm_backup_chunk_upload(&chunk.sha256).await?;
        }
        if !auth.require_backup_chunk_present(&chunk.sha256).await? {
            return Err(invalid("replacement upload is not confirmed"));
        }
    }
    let manifest_bytes = serde_json::to_vec(&prepared.next).map_err(|e| invalid(&e.to_string()))?;
    let presign = auth
        .presign_backup_manifest_upload(&next_sha, manifest_bytes.len() as u64)
        .await?;
    if !presign.already_present {
        let url = presign
            .url
            .ok_or_else(|| invalid("manifest upload URL absent"))?;
        s3.upload_snapshot(&url, manifest_bytes).await?;
        auth.confirm_backup_manifest_upload(&next_sha).await?;
    }
    if !auth.require_backup_manifest_present(&next_sha).await? {
        return Err(invalid("manifest upload is not confirmed"));
    }
    if auth.backup_latest_get().await?.latest.manifest_sha256 != expected {
        return Err(invalid("cloud tip changed before CAS"));
    }
    let committed = auth
        .backup_latest_cas_for_format(
            &prepared.next.store_uuid,
            prepared.next.epoch,
            prepared.next.counter,
            &next_sha,
            prepared.next.version,
        )
        .await?;
    if committed.latest.store_uuid != prepared.next.store_uuid
        || committed.latest.epoch != prepared.next.epoch
        || committed.latest.counter != prepared.next.counter
        || committed.latest.manifest_sha256 != next_sha
    {
        return Err(invalid("CAS receipt mismatch"));
    }
    Ok(prepared.next.clone())
}
