//! Queries select only the locally committed receiver namespace.

use crate::db_operations::DbOperations;
use crate::storage::error::StorageResult;

mod memory;

pub(super) async fn namespaces(ops: &DbOperations) -> StorageResult<Vec<String>> {
    let controls = memory::controls(ops).await?;
    Ok(controls
        .senders
        .iter()
        .map(|sender| format!("from:{sender}"))
        .collect())
}
