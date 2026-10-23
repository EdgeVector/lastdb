//! Org exports use the same element/record protection policy as ordinary writes.
//! The org-encrypted format marker is durable before any direct atom/CAS value
//! enters the isolated source. Older readers therefore refuse this format.
//! File envelopes, file keys and ciphertext addresses remain unchanged.

use super::org_rows::error;
use crate::db_operations::atom_store::key_format::KEY_FORMAT;
use crate::db_operations::AtomStore;
use crate::storage::encrypting_store::EncryptingKvStore;
use crate::storage::{KvStore, LastStoreNamespacedStore, NamespacedStore};
use crate::sync::SyncResult;
use std::sync::Arc;

pub(super) async fn store(
    destination: &LastStoreNamespacedStore,
    namespace: &str,
    key: &[u8; 32],
) -> SyncResult<EncryptingKvStore> {
    let inner = destination.open_namespace(namespace).await.map_err(error)?;
    Ok(EncryptingKvStore::new(
        namespace,
        inner,
        Arc::new(crate::crypto::LocalCryptoProvider::from_key(*key)),
    ))
}

pub(super) async fn initialize(
    destination: &LastStoreNamespacedStore,
    key: &[u8; 32],
) -> SyncResult<()> {
    store(destination, "main", key)
        .await?
        .put(
            AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes(),
            KEY_FORMAT.to_vec(),
        )
        .await
        .map_err(error)?;
    destination
        .restore_durability_barrier()
        .await
        .map_err(error)
}

pub(super) async fn row(
    protection: &EncryptingKvStore,
    key: Vec<u8>,
    value: Vec<u8>,
) -> SyncResult<(Vec<u8>, Vec<u8>)> {
    let value = protection.protect_value(&key, value).await.map_err(error)?;
    Ok((key, value))
}
