use super::error::{StorageError, StorageResult};
use super::traits::{KvMutation, KvStore, PhysicalScanCursor, PhysicalScanPage};
use crate::crypto::at_rest::{decode_binary_ciphertext, encode_binary_ciphertext};
use crate::crypto::CryptoProvider;
use async_trait::async_trait;
use std::sync::Arc;

/// The storage envelope for record file rows (`cas_blobs`).
///
/// This wraps any `KvStore` implementation, inserting an encryption layer
/// between the `TypedKvStore` serialization boundary and the actual backend.
///
/// ```text
/// TypedKvStore (JSON bytes)
///       ↓
/// EncryptingKvStore (encrypt → binary `ENB:` envelope)
///       ↓
/// byte-oriented KvStore backend
/// ```
///
/// Keys are NOT encrypted — only values. This preserves indexing and scan_prefix.
///
/// Every CAS record has the binary `ENB:` envelope. A stored value without it
/// is an error. Other namespaces do not use this decorator.
pub struct EncryptingKvStore {
    inner: Arc<dyn KvStore>,
    crypto: Arc<dyn CryptoProvider>,
}

impl EncryptingKvStore {
    /// Create a new encrypting store wrapping the given inner store.
    ///
    /// Every write receives the `ENB:` envelope.
    pub fn new(inner: Arc<dyn KvStore>, crypto: Arc<dyn CryptoProvider>) -> Self {
        Self { inner, crypto }
    }

    /// Select the crypto provider for a key.
    fn select_crypto(&self, key: &[u8]) -> Arc<dyn CryptoProvider> {
        let _ = key;
        Arc::clone(&self.crypto)
    }

    /// Seal one value as `ENB:`. A file row is already ciphertext, so it is
    /// not compressed first.
    async fn seal_value(&self, key: &[u8], value: &[u8]) -> StorageResult<Vec<u8>> {
        let crypto = self.select_crypto(key);
        let ciphertext = crypto
            .encrypt(value)
            .await
            .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
        Ok(encode_binary_ciphertext(&ciphertext))
    }

    /// Open one stored `ENB:` value. A value without the envelope is an error.
    async fn open_sealed_value(
        &self,
        data: &[u8],
        crypto: &dyn CryptoProvider,
    ) -> StorageResult<Vec<u8>> {
        let Some(ciphertext) = decode_binary_ciphertext(data)
            .map_err(|e| StorageError::EncryptionError(e.to_string()))?
        else {
            return Err(StorageError::EncryptionError(
                "CAS record has no ENB: envelope".into(),
            ));
        };
        crypto
            .decrypt(ciphertext)
            .await
            .map_err(|e| StorageError::EncryptionError(e.to_string()))
    }

    /// Open one stored value and record the decrypt receipt.
    async fn open_value(&self, key: &[u8], stored: Vec<u8>) -> StorageResult<Vec<u8>> {
        let crypto = self.select_crypto(key);
        let started = std::time::Instant::now();
        let result = self.open_sealed_value(&stored, crypto.as_ref()).await;
        let bytes = result.as_ref().map_or(0, Vec::len);
        crate::read_receipts::decrypted(bytes, started.elapsed());
        result
    }

    async fn decrypt_rows(
        &self,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut decrypted_rows = Vec::with_capacity(rows.len());
        for (key, stored) in rows {
            let plaintext = self.open_value(&key, stored).await?;
            decrypted_rows.push((key, plaintext));
        }
        Ok(decrypted_rows)
    }
}

mod kv_store_impl;
