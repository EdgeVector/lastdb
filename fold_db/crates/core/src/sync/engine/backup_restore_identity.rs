//! Recover plain segment addresses for receipt-backed atom replacements.
//!
//! A rewrite increments the manifest UUID to retire the old chunk identity.
//! That UUID is an object-generation identity, not the original numbered
//! segment address. Follow only verified predecessor metadata and exact purge
//! receipts. Downloads always use the current digest; retired bodies are never
//! needed to recover the address.

use super::super::backup_atom_rewrite::replacement_uuid;
use crate::storage::laststore::{BackupChunkRef, BackupManifest, BackupManifestRole};
use crate::sync::error::{SyncError, SyncResult};
use std::collections::{BTreeMap, BTreeSet};

pub(super) type ChunkAddress = (String, u16, Option<u32>, String);

pub(super) fn address(chunk: &BackupChunkRef) -> ChunkAddress {
    (
        chunk.collection.clone(),
        chunk.shard,
        chunk.group_id,
        chunk.chunk_uuid.clone(),
    )
}

fn invalid() -> SyncError {
    SyncError::Storage("backup restore has an ambiguous rewritten segment identity".to_string())
}

fn chunk_index(manifest: &BackupManifest) -> SyncResult<BTreeMap<ChunkAddress, &BackupChunkRef>> {
    let mut index = BTreeMap::new();
    for chunk in manifest.atom_chunks.iter().chain(&manifest.mutable_chunks) {
        if index.insert(address(chunk), chunk).is_some() {
            return Err(invalid());
        }
    }
    Ok(index)
}

/// Input order is latest to root, after the full chain passed hash, scope and
/// receipt validation. The result contains only aliases still present at the
/// selected tip. Unchanged chunks retain their original install path.
pub(super) fn rewritten_segment_origins(
    manifests: &[BackupManifest],
) -> SyncResult<BTreeMap<ChunkAddress, String>> {
    let mut aliases = BTreeMap::<ChunkAddress, String>::new();
    for pair in manifests.windows(2).rev() {
        let current = &pair[0];
        let previous = &pair[1];
        let before = chunk_index(previous)?;
        let after = chunk_index(current)?;
        let mut next = BTreeMap::new();
        for (key, chunk) in &after {
            if before.contains_key(key) {
                if let Some(origin) = aliases.get(key) {
                    next.insert(key.clone(), origin.clone());
                }
                continue;
            }
            if chunk.role != BackupManifestRole::Atom
                || chunk.collection != "atoms"
                || chunk.end_csn != 0
            {
                continue;
            }
            let uuid = uuid::Uuid::parse_str(&chunk.chunk_uuid).map_err(|_| invalid())?;
            let Some(predecessor) = uuid.as_u128().checked_sub(1) else {
                continue;
            };
            let predecessor = uuid::Uuid::from_u128(predecessor).to_string();
            let old_key = (
                chunk.collection.clone(),
                chunk.shard,
                chunk.group_id,
                predecessor,
            );
            let Some(old) = before.get(&old_key) else {
                continue;
            };
            // An adjacent UUID alone is not authority to alias a destination.
            // Require the exact erase-rewrite transition and its original SHA.
            if after.contains_key(&old_key)
                || old.role != BackupManifestRole::Atom
                || old.end_csn != chunk.end_csn
                || old.sha256 == chunk.sha256
                || chunk.bytes == 0
                || chunk.bytes >= old.bytes
                || replacement_uuid(&old.chunk_uuid)? != chunk.chunk_uuid
                || !current.deletion_receipts.iter().any(|receipt| {
                    receipt.covers_purged_atom(previous.counter, current.counter, &old.sha256)
                })
            {
                return Err(invalid());
            }
            let origin = aliases.get(&old_key).unwrap_or(&old.chunk_uuid).clone();
            next.insert(key.clone(), origin);
        }
        aliases = next;
    }
    // A later ordinary cut must not reintroduce an original UUID beside its
    // replacement: both would otherwise overwrite the same numbered file.
    let tip = manifests.first().ok_or_else(invalid)?;
    let mut destinations = BTreeSet::new();
    for (mut key, _) in chunk_index(tip)? {
        if let Some(origin) = aliases.get(&key) {
            key.3.clone_from(origin);
        }
        if !destinations.insert(key) {
            return Err(invalid());
        }
    }
    Ok(aliases)
}
