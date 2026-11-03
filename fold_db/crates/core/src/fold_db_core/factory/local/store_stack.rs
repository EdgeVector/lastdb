use crate::crypto::{E2eKeys, LocalCryptoProvider};
use crate::storage::traits::NamespacedStore;
use crate::storage::EncryptingNamespacedStore;
use std::sync::Arc;

pub(super) struct StoreStack {
    pub(super) store: Arc<dyn NamespacedStore>,
}

pub(super) fn build_store_stack(
    base_store: Arc<dyn NamespacedStore>,
    e2e_keys: &E2eKeys,
) -> StoreStack {
    let store = Arc::new(EncryptingNamespacedStore::new(
        base_store,
        Arc::new(LocalCryptoProvider::from_key(e2e_keys.encryption_key())),
    ));
    StoreStack { store }
}
