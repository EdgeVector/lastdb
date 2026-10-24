mod checkpoints;
mod reap_chunks;
mod reseal;

use super::encrypting_store::EncryptingKvStore;
use super::error::{StorageError, StorageResult};
use super::reap_unsealed::{
    reap_checkpoint_key, ReapUnsealedCheckpoint, ReapUnsealedOptions, ReapUnsealedReport,
    REAP_CHECKPOINT_NAMESPACE,
};
use super::reseal_at_rest::{
    checkpoint_key, collection_is_allowed, ResealAtRestCheckpoint, ResealAtRestOptions,
    ResealAtRestReport, RESEAL_AT_REST_ALLOWLIST, RESEAL_CHECKPOINT_NAMESPACE,
    RESEAL_CHECKPOINT_VERSION, RESEAL_TARGET_FORMAT_VERSION,
};
use super::traits::{KvStore, NamespacedStore};
use crate::crypto::CryptoProvider;
use std::sync::Arc;

/// Published or derived catalog collections. This list classifies legacy
/// backup/replay metadata; it does not select storage encryption.
pub const CATALOG_NAMESPACES: &[&str] = &[
    "schemas",
    // Database/schema -> universal instance references. Catalog metadata only;
    // never molecule/atom content.
    "db_catalog",
    "molecule_keys",
    "schema_states",
    "schema_superseded_by",
    "schema_index",
    "public_keys",
    "node_id_schema_permissions",
    "idempotency",
    "lineage_forward",
    "lineage_reverse",
];

/// Only local CAS blob records receive a storage-level encryption envelope.
/// Atoms retain their content encryption. Tips, counters, catalogs, and other
/// control records pass directly to storage. No packed file is encrypted here.
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

    /// Whether new values in this namespace receive the storage envelope.
    pub fn encrypts_namespace(&self, namespace: &str) -> bool {
        self.should_encrypt(namespace)
    }
}

mod store_impl;
