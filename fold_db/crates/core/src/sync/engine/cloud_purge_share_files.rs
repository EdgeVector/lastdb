//! Logical photographs admit only bounded inline files, before loading bytes.

use super::*;
use crate::db_operations::DbOperations;
use crate::sync::snapshot::{
    cloud_purge::share_limit_error, NamespaceData, Snapshot, SnapshotEntry,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use std::collections::BTreeSet;

const MAX_INLINE_CIPHER_BYTES: u64 = 16 * 1024 * 1024 * 4 / 3 + 4096;

pub(super) async fn add_share_file_closure(
    ops: &DbOperations,
    snapshot: &mut Snapshot,
) -> SyncResult<()> {
    let keys = atom_keys(snapshot)?;
    let mut refs = BTreeSet::new();
    let mut known = BTreeSet::new();
    for batch in keys.chunks(128) {
        for atom in ops
            .atoms()
            .get_atoms_by_storage_keys(batch)
            .await
            .map_err(|_| error("share atom source decode failed"))?
        {
            let atom = atom.ok_or_else(|| error("share source atom absent"))?;
            let declared =
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata());
            if let Some(bytes) = crate::atom::file_pointer::blob_logical_bytes_of_atom(
                atom.content(),
                atom.metadata(),
            ) {
                if !declared.is_empty() && (bytes == 0 || bytes > MAX_INLINE_CIPHER_BYTES) {
                    return Err(inline_limit_error());
                }
                known.extend(declared.iter().cloned());
            }
            refs.extend(declared);
        }
    }
    let missing: Vec<_> = refs.difference(&known).cloned().collect();
    validate_missing_sizes(ops, &missing).await?;
    let cas = ops.open_namespace("cas_blobs").await?;
    let mut entries = Vec::new();
    let mut bytes = snapshot.cloud_purge_share_bytes()?;
    let refs: Vec<_> = refs.into_iter().collect();
    for batch in refs.chunks(4) {
        let values = cas
            .get_many(batch.iter().map(|id| id.as_bytes().to_vec()).collect())
            .await?;
        for (id, value) in batch.iter().zip(values) {
            let value = value.ok_or_else(|| error("owned share source lacks exact file bytes"))?;
            if value.len() as u64 > MAX_INLINE_CIPHER_BYTES + 4096 {
                return Err(inline_limit_error());
            }
            let entry = SnapshotEntry {
                key: BASE64.encode(id),
                value: BASE64.encode(value),
            };
            bytes = bytes
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.len())
                .saturating_add(64);
            if bytes > crate::sync::snapshot::cloud_purge::MAX_SHARE_SNAPSHOT_BYTES {
                return Err(share_limit_error());
            }
            entries.push(entry);
        }
    }
    snapshot
        .namespaces
        .retain(|namespace| namespace.name != "cas_blobs");
    snapshot.namespaces.push(NamespaceData {
        name: "cas_blobs".into(),
        entries,
    });
    Ok(())
}

async fn validate_missing_sizes(ops: &DbOperations, refs: &[String]) -> SyncResult<()> {
    let memo = ops.open_namespace("sync_file_blob_known").await?;
    for batch in refs.chunks(128) {
        let values = memo
            .get_many(
                batch
                    .iter()
                    .map(|id| id.strip_prefix("sha256:").unwrap_or(id).as_bytes().to_vec())
                    .collect(),
            )
            .await?;
        for (id, value) in batch.iter().zip(values) {
            let reference: FileBlobRef =
                serde_json::from_slice(&value.ok_or_else(inline_limit_error)?)
                    .map_err(|_| inline_limit_error())?;
            if reference.blob_ref != *id
                || reference.encrypted_size_bytes == 0
                || reference.encrypted_size_bytes > MAX_INLINE_CIPHER_BYTES
            {
                return Err(inline_limit_error());
            }
        }
    }
    Ok(())
}

fn atom_keys(snapshot: &Snapshot) -> SyncResult<Vec<String>> {
    let mut keys = Vec::new();
    for namespace in &snapshot.namespaces {
        if namespace.name != "main" && namespace.name != "atoms" {
            continue;
        }
        for entry in &namespace.entries {
            let key = String::from_utf8(
                BASE64
                    .decode(&entry.key)
                    .map_err(|_| error("invalid share snapshot key"))?,
            )
            .map_err(|_| error("invalid share snapshot key"))?;
            if crate::atom::atom_key_codec::uuid_of(&key).is_some() {
                keys.push(key);
            }
        }
    }
    Ok(keys)
}

fn inline_limit_error() -> SyncError {
    error("owned share file size is absent or exceeds the 16 MiB inline product limit before cloud hold; use file slabs")
}

fn error(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
