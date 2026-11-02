use super::encrypting_store::EncryptingKvStore;
use super::error::StorageResult;
use super::traits::{KvStore, NamespacedStore};
use crate::crypto::CryptoProvider;
use std::sync::Arc;

/// Only local CAS blob records receive a storage-level encryption envelope.
/// Record rows are plain JSON on a v2 home; the run-file block encryption
/// protects them. Tips, counters, catalogs, and other control records pass
/// directly to storage. No packed file is encrypted here.
pub struct EncryptingNamespacedStore {
    inner: Arc<dyn NamespacedStore>,
    crypto: Arc<dyn CryptoProvider>,
}

impl EncryptingNamespacedStore {
    /// Construct the fixed CAS-only policy. Callers cannot broaden it.
    pub fn new(inner: Arc<dyn NamespacedStore>, crypto: Arc<dyn CryptoProvider>) -> Self {
        Self { inner, crypto }
    }

    pub(crate) fn should_encrypt(&self, namespace: &str) -> bool {
        namespace == "cas_blobs"
    }
}

mod store_impl;
