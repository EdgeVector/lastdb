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
    let enc_store = Arc::new(EncryptingNamespacedStore::new(
        base_store,
        store_seam_crypto(e2e_keys, at_rest_keyring),
    ));
    // Local capture has no cloud engine. A future cloud implementation must
    // pass its own native-format activation gate before attaching one.
    let mutation_log_capture = Arc::new(crate::sync::capture::MutationLogCaptureRouter::default());
    let store = Arc::new(
        crate::sync::capture::MutationLogCaptureNamespacedStore::new(
            enc_store as Arc<dyn NamespacedStore>,
            Arc::clone(&mutation_log_capture),
        ),
    );
    StoreStack {
        store,
        mutation_log_capture,
    }
}
