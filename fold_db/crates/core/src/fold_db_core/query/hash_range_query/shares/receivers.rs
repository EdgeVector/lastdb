//! Queries select only the locally committed receiver namespace.

use crate::db_operations::DbOperations;

mod memory;

pub(super) async fn namespaces(ops: &DbOperations) -> crate::sync::SyncResult<Vec<String>> {
    let controls = memory::controls(ops).await?;
    Ok(controls
        .senders
        .iter()
        .zip(&controls.bindings)
        .map(|(sender, binding)| {
            binding.as_ref().map_or_else(
                || format!("from:{sender}"),
                |binding| binding.storage_prefix.clone(),
            )
        })
        .collect())
}
