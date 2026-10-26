use super::store_seam_crypto;
use crate::crypto::keyring::Keyring;
use crate::crypto::E2eKeys;
use crate::storage::traits::NamespacedStore;
use crate::storage::EncryptingNamespacedStore;
use std::sync::Arc;

pub(super) struct StoreStack {
    pub(super) store: Arc<dyn NamespacedStore>,
    pub(super) mutation_log_capture: Arc<crate::sync::capture::MutationLogCaptureRouter>,
}

pub(super) fn build_store_stack(
    base_store: Arc<dyn NamespacedStore>,
    e2e_keys: &E2eKeys,
    at_rest_keyring: Option<&Arc<Keyring>>,
) -> StoreStack {
    let store = Arc::new(EncryptingNamespacedStore::new(
        base_store,
        store_seam_crypto(e2e_keys, at_rest_keyring),
    ));
    // The router holds only the mutation admission gate. No cloud capture
    // wrapper exists, so the encrypting store is the outermost store.
    let mutation_log_capture = Arc::new(crate::sync::capture::MutationLogCaptureRouter::default());
    StoreStack {
        store,
        mutation_log_capture,
    }
}
