use std::sync::Arc;

use serde_json::Value;

use crate::db_operations::DbOperations;
use crate::durable_flush::{self, BatchPlacementLog};
use crate::schema::SchemaError;

/// Keep the exact tip locks while the Delete barrier reaches disk. A scoped
/// flush keeps unrelated dirty groups out of this per-key order point.
pub(super) async fn flush_delete_barriers(
    db_ops: &Arc<DbOperations>,
    barrier_items: Vec<(String, Value)>,
) -> Result<(), SchemaError> {
    if barrier_items.is_empty() {
        return Ok(());
    }

    let raw = db_ops.atoms().raw();
    if raw.inner().batch_put_has_durable_barrier() {
        raw.batch_put_items(barrier_items)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("write Delete barriers: {error}")))?;
        return Ok(());
    }

    let placements = BatchPlacementLog::shared();
    durable_flush::scope(Arc::clone(&placements), async {
        db_ops
            .atoms()
            .raw()
            .batch_put_items(barrier_items)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("write Delete barriers: {error}")))?;

        let written = placements.written_keys();
        let flush = if written.is_empty() {
            // Other storage backends have no LastStore placement log.
            db_ops.flush().await
        } else {
            db_ops.flush_dirty_scope(&written).await
        };
        flush.map_err(|error| SchemaError::InvalidData(format!("flush Delete barriers: {error}")))
    })
    .await
}
