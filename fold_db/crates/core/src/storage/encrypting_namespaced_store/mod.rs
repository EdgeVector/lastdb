use super::encrypting_store::EncryptingKvStore;
use super::error::StorageResult;
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
/// Atom rows are plain JSON on a v2 home; the run-file block encryption
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
