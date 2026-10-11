//! Preserve the stopped peer's inbound subscription data outside personal replacement.

use crate::db_operations::DbOperations;
use crate::sync::{SyncError, SyncResult};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};

mod metadata;
mod source;
use metadata::preserve_metadata;
use source::{physical_keys, InboundData};

const COLLECTIONS: &[&str] = &[
    "main",
    "legacy_blob_refs",
    "mutation_history",
    "field_update_order_log",
    "field_update_order_count",
    "field_hashrange_page_index",
    "field_hashrange_hash_index",
    "field_hashrange_complete",
    "indexes",
    "tips",
    "proteins",
    "atom_locators",
    "atoms",
];

/// Both stores are stopped. The caller commits only after the complete data,
/// metadata, file, cursor, and device-consent closure passes its switch proof.
pub async fn preserve_inbound_shares(
    previous: &DbOperations,
    candidate: &DbOperations,
) -> SyncResult<(String, Vec<String>)> {
    let subscriptions = crate::sharing::store::list_share_subscriptions_in_ops(previous)
        .await
        .map_err(error)?;
    let mut scopes = BTreeSet::new();
    for subscription in &subscriptions {
        let parts: Vec<_> = subscription.share_prefix.split(':').collect();
        if parts.len() != 3
            || parts[0] != "share"
            || parts[1].len() != 32
            || !parts[1].bytes().all(|byte| byte.is_ascii_hexdigit())
            || parts[2].is_empty()
        {
            return Err(error("the prior inbound share scope is invalid"));
        }
        scopes.insert(format!("from:{}:", parts[1]));
    }
    let InboundData {
        rows,
        molecules,
        atom_keys,
    } = source::read(previous, &scopes).await?;
    let destination = candidate.open_namespace("main").await.map_err(error)?;
    preserve_metadata(previous, candidate, &molecules).await?;
    let mut blobs = BTreeSet::new();
    for keys in atom_keys.chunks(128) {
        let atoms = previous
            .atoms()
            .get_atoms_by_storage_keys(keys)
            .await
            .map_err(error)?;
        if atoms.len() != keys.len() {
            return Err(error("the inbound atom batch is incomplete"));
        }
        for atom in atoms {
            let atom = atom.ok_or_else(|| error("an inbound atom is absent"))?;
            blobs.extend(crate::atom::file_pointer::blob_refs_of_atom(
                atom.content(),
                atom.metadata(),
            ));
        }
    }
    let mut digest = sha2::Sha256::new();
    copy_blobs(previous, candidate, &blobs, &mut digest).await?;
    // Remove another device's receiver namespace before this peer's exact copy.
    let candidate_raw = candidate
        .namespaced_store()
        .raw_last_store()
        .ok_or_else(|| error("inbound preservation requires a LastStore candidate"))?;
    let inherited = physical_keys(&candidate_raw, "from:")?
        .into_iter()
        .map(String::into_bytes)
        .collect::<Vec<_>>();
    for keys in inherited.chunks(128) {
        destination
            .batch_delete(keys.to_vec())
            .await
            .map_err(error)?;
    }
    for rows in rows.chunks(128) {
        hash(&mut digest, "main", rows);
        destination
            .restore_batch_put(rows.to_vec())
            .await
            .map_err(error)?;
    }
    candidate
        .namespaced_store()
        .restore_durability_barrier()
        .await
        .map_err(error)?;
    Ok((
        crate::hex::hex_lower(digest.finalize()),
        subscriptions
            .into_iter()
            .map(|subscription| subscription.share_prefix)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    ))
}

async fn copy_blobs(
    previous: &DbOperations,
    candidate: &DbOperations,
    refs: &BTreeSet<String>,
    digest: &mut sha2::Sha256,
) -> SyncResult<()> {
    let source = previous.open_namespace("cas_blobs").await.map_err(error)?;
    let destination = candidate.open_namespace("cas_blobs").await.map_err(error)?;
    for keys in refs
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect::<Vec<_>>()
        .chunks(4)
    {
        let (old, current) = futures::join!(
            source.get_many(keys.to_vec()),
            destination.get_many(keys.to_vec())
        );
        let (old, current) = (old.map_err(error)?, current.map_err(error)?);
        if old.len() != keys.len() || current.len() != keys.len() {
            return Err(error("inbound file batch is incomplete"));
        }
        let mut rows = Vec::new();
        for ((key, old), current) in keys.iter().zip(old).zip(current) {
            // An absent local cache remains fetchable under its unchanged subscription.
            let Some(old) = old else { continue };
            if current.as_ref().is_some_and(|current| current != &old) {
                return Err(error("a prior inbound file conflicts with adopted data"));
            }
            rows.push((key.clone(), old));
        }
        hash(digest, "cas_blobs", &rows);
        destination.restore_batch_put(rows).await.map_err(error)?;
    }
    Ok(())
}

fn hash(digest: &mut sha2::Sha256, namespace: &str, rows: &[(Vec<u8>, Vec<u8>)]) {
    digest.update((namespace.len() as u64).to_be_bytes());
    digest.update(namespace.as_bytes());
    for (key, value) in rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
}

fn error(value: impl std::fmt::Display) -> SyncError {
    SyncError::CloudPurgeHeld(value.to_string())
}
