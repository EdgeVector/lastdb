//! Native local FoldDB construction and at-rest encryption.

use super::{map_boot_decrypt_error, FoldDbOptions};
use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::fold_db::FoldDbInit;
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::disk_pack::PackNamespacedStore;
use std::path::Path;
use std::sync::Arc;

pub(super) async fn create_local_fold_db(
    path: &Path,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    let key = e2e_keys.encryption_key();
    let store = PackNamespacedStore::open_keyed(path, Some(&key))?;
    let db_ops = DbOperations::from_namespaced_store_with_content_and_field_keys(store, key)
        .await
        .map_err(|e| FoldDbError::Config(e.to_string()))?;
    let fold_db = FoldDB::initialize_from_init(FoldDbInit {
        db_ops: Arc::new(db_ops),
        offline_maintenance: options.offline_maintenance,
        signer,
        search_outbox_inbox: options.search_outbox_inbox,
    })
    .await
    .map_err(|e| map_boot_decrypt_error(e.to_string()))?;
    Ok(Arc::new(fold_db))
}
