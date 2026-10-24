//! Native local FoldDB construction and at-rest encryption.

mod boot_wiring;
mod open_store;
mod store_stack;

use crate::crypto::keyring::{KeyPurpose, Keyring};
use crate::crypto::keyring_provider::KeyringCryptoProvider;
use crate::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::fold_db::FoldDbInit;
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::node_config_store::NodeConfigStore;
use std::sync::Arc;

use super::boot::map_boot_decrypt_error;
use super::FoldDbOptions;
use boot_wiring::open_db_operations;

/// Identify the key used for the local CAS storage envelope.
/// Mini always uses the account E2E content key.
pub fn at_rest_provider_label(at_rest_keyring: Option<&Arc<Keyring>>) -> &'static str {
    match at_rest_keyring {
        Some(_) => "keyring_store_dek",
        None => "e2e_content_key",
    }
}

/// Mini uses its portable account key for the CAS wrapper. An embedded caller
/// can supply a separate keyring for that wrapper.
pub(super) fn store_seam_crypto(
    e2e_keys: &E2eKeys,
    at_rest_keyring: Option<&Arc<Keyring>>,
) -> Arc<dyn CryptoProvider> {
    match at_rest_keyring {
        Some(keyring) => Arc::new(KeyringCryptoProvider::new(
            Arc::clone(keyring),
            KeyPurpose::Store,
        )),
        None => Arc::new(LocalCryptoProvider::from_key(e2e_keys.encryption_key())),
    }
}

pub(super) async fn create_local_fold_db(
    path: &std::path::Path,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    let base_store = open_store::open_primary_store(path)?;
    let store_stack = store_stack::build_store_stack(
        Arc::clone(&base_store),
        e2e_keys,
        options.at_rest_keyring.as_ref(),
    );
    crate::db_operations::atom_store::counter::restore_validation::validate_pending(
        store_stack.store.as_ref(),
    )
    .await
    .map_err(|error| FoldDbError::Config(error.to_string()))?;
    let config_store = NodeConfigStore::with_namespaced_store(
        Arc::clone(&base_store),
        Some(e2e_keys.encryption_key()),
    )
    .await
    .map_err(|e| FoldDbError::Config(format!("Failed to open config store: {e}")))?;
    let db_ops = open_db_operations(store_stack.store, e2e_keys).await?;
    let fold_db = FoldDB::initialize_from_init(FoldDbInit {
        db_ops: Arc::new(db_ops),
        offline_maintenance: options.offline_maintenance,
        signer,
        search_outbox_inbox: options.search_outbox_inbox,
        mutation_log_capture: Some(store_stack.mutation_log_capture),
    })
    .await
    .map_err(|e| map_boot_decrypt_error(e.to_string()))?;
    fold_db.set_config_store(config_store);
    Ok(Arc::new(fold_db))
}
