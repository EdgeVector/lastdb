//! Native logical-store construction and decrypt-integrity check.

use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use std::sync::Arc;

pub(super) async fn open_db_operations(
    store: Arc<dyn crate::storage::traits::NamespacedStore>,
    e2e_keys: &E2eKeys,
) -> FoldDbResult<DbOperations> {
    // Each field key's key bundle blinds its hashes; its contract orders ranges.
    DbOperations::from_namespaced_store_with_content_and_field_keys(
        store,
        Some(e2e_keys.encryption_key()),
        Some(e2e_keys.encryption_key()),
    )
    .await
    .map_err(|e| FoldDbError::Config(e.to_string()))
}
