use super::error::{StorageError, StorageResult};
use super::traits::{KvStore, NamespacedStore};
use std::future::Future;
use std::sync::Arc;
use zeroize::Zeroize;

const TREE_NAME: &str = "node_config";

mod crypto;
mod identity;
mod kv;

pub use identity::NodeIdentity;

/// Thin wrapper for storing node configuration on a NamespacedStore collection.
///
/// Sensitive fields (currently: the node's Ed25519 private key) are
/// transparently encrypted at rest with AES-256-GCM when a 32-byte
/// encryption key is supplied via [`NodeConfigStore::with_namespaced_store`].
/// Reads transparently handle both legacy plaintext values (pre-migration)
/// and encrypted values, and writes always produce encrypted output when
/// a key is configured.
///
/// All runtime config (identity keys) lives here. Cloud *intent* (whether
/// sync is on, and its api_url) is NOT stored here — it lives in
/// node_config.json (`database.cloud_sync`); see design-canonical-cloud-auth.
#[derive(Clone)]
pub struct NodeConfigStore {
    store: Arc<dyn KvStore>,
    /// Optional 32-byte key used to encrypt sensitive fields at rest.
    /// When `None`, sensitive fields are stored in plaintext (legacy mode)
    /// and reads of previously-encrypted values will fail loudly rather
    /// than silently returning ciphertext.
    identity_key: Option<[u8; 32]>,
}

impl Drop for NodeConfigStore {
    fn drop(&mut self) {
        self.zeroize_key();
    }
}

impl NodeConfigStore {
    /// Open node config on a NamespacedStore collection (Last Store product path).
    pub async fn with_namespaced_store(
        store: Arc<dyn NamespacedStore>,
        identity_key: Option<[u8; 32]>,
    ) -> Result<Self, StorageError> {
        let kv = store.open_namespace(TREE_NAME).await?;
        Ok(Self {
            store: kv,
            identity_key,
        })
    }

    /// Scrub the cached identity key (and any clone of this handle) from
    /// memory on drop (at-rest threat model Gap G3).
    fn zeroize_key(&mut self) {
        if let Some(key) = self.identity_key.as_mut() {
            key.zeroize();
        }
    }

    // NOTE: cloud config (the old `cloud:api_url` / `cloud:user_hash` /
    // `cloud:enabled` keys with `get_cloud_config` / `set_cloud_config` /
    // `is_cloud_enabled`) used to live here. It was a write-only duplicate
    // store with zero production readers — node_config.json
    // (`database.cloud_sync`) is the single source of truth for cloud intent
    // (design-canonical-cloud-auth L2). Removed to collapse the duplicate.
}

pub(super) fn block_on_storage<T, Fut>(future: Fut) -> Result<T, StorageError>
where
    T: Send + 'static,
    Fut: Future<Output = StorageResult<T>> + Send + 'static,
{
    fn run<T, Fut>(future: Fut) -> Result<T, StorageError>
    where
        T: Send + 'static,
        Fut: Future<Output = StorageResult<T>> + Send + 'static,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(StorageError::IoError)?;
        runtime.block_on(future)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run(future))
            .join()
            .map_err(|_| StorageError::BackendError("node config storage thread panicked".into()))?
    } else {
        run(future)
    }
}
