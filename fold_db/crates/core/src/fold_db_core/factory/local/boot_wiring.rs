//! Native logical-store construction and decrypt-integrity check.

use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use crate::storage::disk_pack::PackNamespacedStore;

pub(super) async fn open_db_operations(
    store: PackNamespacedStore,
    e2e_keys: &E2eKeys,
) -> FoldDbResult<DbOperations> {
    DbOperations::from_namespaced_store_with_content_and_field_keys(
        store,
        e2e_keys.encryption_key(),
    )
    .await
    .map_err(|e| FoldDbError::Config(e.to_string()))
}
