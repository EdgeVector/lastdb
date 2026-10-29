//! Native local FoldDB construction and at-rest encryption.

mod boot_wiring;
mod open_store;

use crate::crypto::{E2eKeys, LocalCryptoProvider};
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::fold_db::FoldDbInit;
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::EncryptingNamespacedStore;
use std::sync::Arc;

use super::boot::map_boot_decrypt_error;
use super::FoldDbOptions;
use boot_wiring::open_db_operations;

pub(super) async fn create_local_fold_db(
    path: &std::path::Path,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    let store = Arc::new(EncryptingNamespacedStore::new(
        open_store::open_primary_store(path, &e2e_keys.encryption_key())?,
        Arc::new(LocalCryptoProvider::from_key(e2e_keys.encryption_key())),
    ));
    crate::db_operations::atom_store::counter::restore_validation::validate_pending(store.as_ref())
        .await
        .map_err(|error| FoldDbError::Config(error.to_string()))?;
    let db_ops = open_db_operations(store, e2e_keys).await?;
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
