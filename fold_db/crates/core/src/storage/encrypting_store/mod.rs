use super::error::{StorageError, StorageResult};
use super::traits::{KvMutation, KvStore, PartitionedScan, PhysicalScanCursor, PhysicalScanPage};
use crate::crypto::at_rest::{
    at_rest_max_inflated_bytes, compress_for_seal, decode_binary_ciphertext,
    encode_binary_ciphertext, inflate_at_rest, kv_at_rest_compress_min_bytes,
    kv_at_rest_compression_enabled,
};
use crate::crypto::CryptoProvider;
use async_trait::async_trait;
use std::sync::Arc;

/// Write-side compression policy for one store instance.
///
/// Resolved **once, at construction** rather than per write. Both knobs are
/// environment variables, and this seam is on the write path of every value in
/// the product — re-reading `std::env::var` per `put` would take the process
/// environment lock and allocate a `String` on every single write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KvCompressionPolicy {
    /// Whether new writes may set the deflate flag of `ENB:`.
    enabled: bool,
    /// Values under this many plaintext bytes are sealed verbatim.
    min_bytes: usize,
}

impl KvCompressionPolicy {
    /// The policy this process was started with.
    fn from_env() -> Self {
        Self {
            enabled: kv_at_rest_compression_enabled(),
            min_bytes: kv_at_rest_compress_min_bytes(),
        }
    }
}

/// The storage envelope for local CAS blob records.
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
    /// Write-side compression policy. Reads never consult it.
    compression: KvCompressionPolicy,
}

impl EncryptingKvStore {
    /// Create a new encrypting store wrapping the given inner store.
    ///
    /// Every write receives the `ENB:` envelope.
    pub fn new(inner: Arc<dyn KvStore>, crypto: Arc<dyn CryptoProvider>) -> Self {
        Self {
            inner,
            crypto,
            compression: KvCompressionPolicy::from_env(),
        }
    }

    /// Select the crypto provider for a key.
    fn select_crypto(&self, key: &[u8]) -> Arc<dyn CryptoProvider> {
        let _ = key;
        Arc::clone(&self.crypto)
    }

    /// Seal one value as `ENB:`: deflate first when that is smaller, then
    /// encrypt.
    ///
    /// Compression happens **before** encryption because ciphertext is
    /// incompressible — a value sealed without being compressed can never be
    /// shrunk again by compaction, the backup chunk plane, or the cloud bill.
    /// That order is the standing rule in
    /// `preference-lastdb-storage-compress-records-and-blobs`.
    ///
    /// The deflate flag stays clear whenever the write-side switch is off,
    /// the value is under the floor, or deflate did not actually shrink it, so
    /// this can never store more bytes than the uncompressed path would.
    async fn seal_value(&self, key: &[u8], value: &[u8]) -> StorageResult<Vec<u8>> {
        let crypto = self.select_crypto(key);
        let deflated = compress_for_seal(
            value,
            self.compression.min_bytes,
            at_rest_max_inflated_bytes(),
            self.compression.enabled,
        );
        let plaintext: &[u8] = deflated.as_deref().unwrap_or(value);
        let ciphertext = crypto
            .encrypt(plaintext)
            .await
            .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
        Ok(encode_binary_ciphertext(&ciphertext, deflated.is_some()))
    }

    /// Open one stored `ENB:` value. A value without the envelope is an error.
    async fn open_sealed_value(
        &self,
        data: &[u8],
        crypto: &dyn CryptoProvider,
    ) -> StorageResult<Vec<u8>> {
        let Some((deflated, ciphertext)) = decode_binary_ciphertext(data)
            .map_err(|e| StorageError::EncryptionError(e.to_string()))?
        else {
            return Err(StorageError::EncryptionError(
                "CAS record has no ENB: envelope".into(),
            ));
        };
        let plaintext = crypto
            .decrypt(ciphertext)
            .await
            .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
        if deflated {
            return inflate_at_rest(&plaintext)
                .map_err(|e| StorageError::EncryptionError(e.to_string()));
        }
        Ok(plaintext)
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

    fn lossy_derived_key(key: &[u8]) -> bool {
        key.starts_with(b"emb:")
    }
}

mod kv_store_impl;
