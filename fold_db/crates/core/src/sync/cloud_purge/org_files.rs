//! Reachable Org files copy directly to the isolated store, one admitted row.

use super::{memory, org_rows::error};
use crate::db_operations::DbOperations;
use crate::storage::{KvStore, LastStoreNamespacedStore};
use crate::sync::SyncResult;
use std::collections::BTreeSet;

pub(super) async fn copy_blobs(
    ops: &DbOperations,
    refs: &BTreeSet<String>,
    key: &[u8; 32],
    destination: &LastStoreNamespacedStore,
    admission: &memory::Admission,
) -> SyncResult<()> {
    let ids: Vec<_> = refs.iter().cloned().collect();
    let sizes = memory::raw_lengths(
        admission,
        0,
        ops.namespaced_store().as_ref(),
        "cas_blobs",
        &ids,
    )?;
    let source = ops.open_namespace("cas_blobs").await.map_err(error)?;
    let target = super::org_element_protection::store(destination, "cas_blobs", key).await?;
    for (id, size) in ids.into_iter().zip(sizes) {
        let size = size.ok_or_else(|| error("org source lacks an exact referenced file blob"))?;
        admission.check(memory::read_peak(size)?)?;
        let bytes = source
            .get(id.as_bytes())
            .await
            .map_err(error)?
            .ok_or_else(|| error("org source file disappeared after size preflight"))?;
        target.put(id.as_bytes(), bytes).await.map_err(error)?;
    }
    Ok(())
}
