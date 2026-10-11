//! Read source access metadata only after exact per-body memory admission.

use super::cloud_purge_file_closure::{collect_required, insert_required, RequiredBlob};
use super::*;
use crate::db_operations::DbOperations;
use crate::sync::cloud_purge::memory::{self, Admission};
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn collect(
    engine: &SyncEngine,
    ops: &DbOperations,
    keys: &[String],
) -> SyncResult<BTreeMap<String, RequiredBlob>> {
    let admission = Admission::acquire(0).await?;
    let sizes = memory::raw_lengths(&admission, 0, ops.namespaced_store().as_ref(), "main", keys)?;
    let (mut required, mut declared) = (BTreeMap::new(), BTreeSet::new());
    for (key, size) in keys.iter().zip(sizes) {
        admission.check(memory::read_peak(
            size.ok_or_else(|| fail("source atom disappeared"))?,
        )?)?;
        let atoms = ops
            .atoms()
            .get_atoms_by_storage_keys(std::slice::from_ref(key))
            .await
            .map_err(|_| fail("source atom decode failed"))?;
        collect_required(atoms, &mut required, &mut declared)?;
    }
    let missing: Vec<_> = declared
        .into_iter()
        .filter(|id| !required.contains_key(id))
        .collect();
    let memo_keys: Vec<_> = missing
        .iter()
        .map(|id| id.strip_prefix("sha256:").unwrap_or(id).to_string())
        .collect();
    let sizes = memory::raw_lengths(
        &admission,
        0,
        engine.store.as_ref(),
        "sync_file_blob_known",
        &memo_keys,
    )?;
    let memo = engine.store.open_namespace("sync_file_blob_known").await?;
    for ((id, key), size) in missing.into_iter().zip(memo_keys).zip(sizes) {
        admission.check(memory::read_peak(
            size.ok_or_else(|| fail("source file access metadata is absent"))?,
        )?)?;
        let bytes = memo
            .get(key.as_bytes())
            .await?
            .ok_or_else(|| fail("source file access metadata disappeared"))?;
        let reference: FileBlobRef = serde_json::from_slice(&bytes)
            .map_err(|_| fail("source file access metadata is invalid"))?;
        if reference.blob_ref != id {
            return Err(fail("source file memo identity differs"));
        }
        insert_required(&mut required, reference, None)?;
    }
    Ok(required)
}

fn fail(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
