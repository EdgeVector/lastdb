//! Preserve the stopped peer's inbound subscription data outside personal replacement.

use crate::db_operations::DbOperations;
use crate::sync::{SyncError, SyncResult};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};

mod data;
mod metadata;
mod receivers;
mod source;
use metadata::preserve_metadata;
use source::InboundData;

/// Both stores are stopped. The caller commits only after the complete data,
/// metadata, file, cursor, and device-consent closure passes its switch proof.
pub async fn preserve_inbound_shares(
    previous: &DbOperations,
    candidate: &DbOperations,
) -> SyncResult<(String, Vec<String>)> {
    let subscriptions = crate::sharing::store::list_share_subscriptions_in_ops(previous)
        .await
        .map_err(error)?;
    let (scopes, bindings) = receivers::source(previous, &subscriptions).await?;
    let admission = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
    let InboundData {
        rows,
        molecules,
        atom_keys,
    } = source::read(previous, &scopes, &admission).await?;
    preserve_metadata(previous, candidate, &molecules, &admission).await?;
    let blobs = data::blob_refs(previous, &atom_keys, &admission).await?;
    let mut digest = sha2::Sha256::new();
    let qualified_cursors =
        receivers::preserve_frontiers(previous, candidate, &bindings, &mut digest).await?;
    receivers::preserve(candidate, bindings, &mut digest).await?;
    copy_blobs(previous, candidate, &blobs, &admission, &mut digest).await?;
    data::replace_rows(candidate, &rows, &admission, &mut digest).await?;
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
            .chain(qualified_cursors)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    ))
}

async fn copy_blobs(
    previous: &DbOperations,
    candidate: &DbOperations,
    refs: &BTreeSet<String>,
    admission: &crate::sync::cloud_purge::memory::Admission,
    digest: &mut sha2::Sha256,
) -> SyncResult<()> {
    let ids: Vec<_> = refs.iter().cloned().collect();
    crate::sync::cloud_purge::blob_copy::copy_admitted(
        previous.namespaced_store().as_ref(),
        candidate.namespaced_store().as_ref(),
        &ids,
        admission,
        |key, bytes| {
            digest.update(("cas_blobs".len() as u64).to_be_bytes());
            digest.update(b"cas_blobs");
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        },
    )
    .await?;
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
