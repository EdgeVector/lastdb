use super::error::{StorageError, StorageResult};
use super::traits::{
    ExecutionModel, FlushBehavior, KvMutation, KvStore, PartitionedScan, PhysicalScanCursor,
    PhysicalScanPage,
};
use crate::crypto::at_rest::{
    at_rest_max_inflated_bytes, compress_for_seal, decode_binary_ciphertext,
    encode_binary_ciphertext, inflate_at_rest, kv_at_rest_compress_min_bytes,
    kv_at_rest_compression_enabled, AT_REST_COMPRESS_MIN_BYTES_DEFAULT,
};
use crate::crypto::CryptoProvider;
use crate::crypto::{
    is_sealed_at_rest, AT_REST_ENC_BINARY_PREFIX, AT_REST_ENC_DEFLATE_PREFIX, AT_REST_ENC_PREFIX,
};
use crate::storage::reap_unsealed::{
    is_reap_checkpoint_key, ReapUnsealedOptions, ReapUnsealedReport, REAP_CHECKPOINT_NAMESPACE,
    REAP_DEFAULT_MAX_ROWS, REAP_DEFAULT_MAX_SECS,
};
use crate::storage::reseal_at_rest::{
    is_checkpoint_key, ResealAtRestOptions, ResealAtRestReport, ResealAtRestTarget,
    RESEAL_COLLECTION_END, RESEAL_DEFAULT_MAX_ROWS, RESEAL_DEFAULT_MAX_SECS, RESEAL_PAGE_HANDLES,
    RESEAL_PAGE_ROWS,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use std::sync::{Arc, Mutex};

use crate::resident::LogicalResidentSet;
use crate::storage::laststore::logical_path;

/// Prefix marker for encrypted values.
/// On write: `ENC:` + base64(ciphertext) → valid UTF-8 string.
/// On read: detect prefix → strip → base64-decode → decrypt.
pub(crate) const ENCRYPTED_PREFIX: &str = "ENC:";

/// Prefix marker for values that were **deflated before encryption**:
/// `ENZ:` + base64(ciphertext(deflate(plaintext))).
///
/// Same width as [`ENCRYPTED_PREFIX`] and it likewise ends in `:`, which is
/// outside the base64 alphabet, so the two can never be confused with each
/// other or with a base64 body. Re-exported from the `at_rest` codec rather
/// than redeclared, because a second copy of this constant is exactly the
/// per-store drift that codec exists to prevent.
pub(crate) const DEFLATED_PREFIX: &str = AT_REST_ENC_DEFLATE_PREFIX;

/// A decorator that transparently encrypts values on write and decrypts on read.
///
/// This wraps any `KvStore` implementation, inserting an encryption layer
/// between the `TypedKvStore` serialization boundary and the actual backend.
///
/// ```text
/// TypedKvStore (JSON bytes)
///       ↓
/// EncryptingKvStore (encrypt → binary or legacy base64 envelope)
///       ↓
/// byte-oriented KvStore backend
/// ```
///
/// Keys are NOT encrypted — only values. This preserves indexing and scan_prefix.
///
/// New binary writes use `ENB:` + flags + ciphertext. Legacy `ENC:` and `ENZ:`
/// values remain readable.
///
/// During migration (dual-read mode), values without the `ENC:` prefix are
/// treated as pre-migration plaintext and returned as-is.
/// Write-side compression policy for one store instance.
///
/// Resolved **once, at construction** rather than per write. Both knobs are
/// environment variables, and this seam is on the write path of every value in
/// the product — re-reading `std::env::var` per `put` would take the process
/// environment lock and allocate a `String` on every single write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct KvCompressionPolicy {
    /// Whether new writes may emit `ENZ:`.
    enabled: bool,
    /// Values under this many plaintext bytes are sealed verbatim.
    min_bytes: usize,
    /// Whether new writes may omit base64 and emit `ENB:`.
    raw: bool,
}

impl KvCompressionPolicy {
    /// The policy this process was started with.
    fn from_env() -> Self {
        Self {
            enabled: kv_at_rest_compression_enabled(),
            min_bytes: kv_at_rest_compress_min_bytes(),
            raw: matches!(env_flag::var_parse("LASTDB_KV_AT_REST_RAW"), Some(true)),
        }
    }
}

pub struct EncryptingKvStore {
    /// The namespace this handle serves. Only used to name a discard in the
    /// log and the counter; never used to decide policy.
    namespace: Arc<str>,
    inner: Arc<dyn KvStore>,
    crypto: Arc<dyn CryptoProvider>,
    /// Write-side compression policy. Reads never consult it.
    compression: KvCompressionPolicy,
    logical: Option<Arc<Mutex<LogicalResidentSet>>>,
}

impl EncryptingKvStore {
    /// Create a new encrypting store wrapping the given inner store.
    ///
    /// There is no migration mode. A value without an at-rest envelope reads as
    /// absent: `decision-2026-09-14-drop-dual-read-unsealed-is-gone`.
    pub fn new(namespace: &str, inner: Arc<dyn KvStore>, crypto: Arc<dyn CryptoProvider>) -> Self {
        Self {
            namespace: Arc::from(namespace),
            inner,
            crypto,
            compression: KvCompressionPolicy::from_env(),
            logical: None,
        }
    }

    pub(crate) fn with_logical(mut self, logical: Arc<Mutex<LogicalResidentSet>>) -> Self {
        self.logical = Some(logical);
        self
    }

    fn admit_plain(&self, key: &[u8], plaintext: &[u8]) {
        if let Some(set) = &self.logical {
            logical_path::admit_plaintext(set, key, plaintext);
        }
    }

    /// Admit the plaintext of a put. `sealed` is the bytes the inner store
    /// wrote, so an older concurrent put does not replace a newer body.
    fn admit_written(&self, key: &[u8], sealed_digest: u64, plaintext: &[u8]) {
        if let Some(set) = &self.logical {
            logical_path::admit_written_plaintext(set, key, sealed_digest, plaintext);
        }
    }

    fn lookup_plain(&self, key: &[u8]) -> Option<logical_path::ResidentLookup> {
        self.logical
            .as_ref()
            .map(|set| logical_path::lookup_plaintext(set, key))
    }

    /// Select the crypto provider for a key.
    fn select_crypto(&self, key: &[u8]) -> Arc<dyn CryptoProvider> {
        let _ = key;
        Arc::clone(&self.crypto)
    }

    /// True when a stored value carries an at-rest envelope marker (`ENC:` /
    /// `ENZ:` / `ENB:`) — i.e. it was written through an encrypting layer.
    /// Used by the reseal sweep to tell rows it may rewrite from the rest, and
    /// by the `reap-unsealed` pass as the *only* test that decides whether a
    /// row stays: sealed stays, un-enveloped goes.
    pub(crate) fn is_encrypted_value(value: &[u8]) -> bool {
        is_sealed_at_rest(value)
    }

    /// Encode ciphertext bytes into a UTF-8-safe string with the `ENC:` prefix.
    pub(crate) fn encode_ciphertext(ciphertext: &[u8]) -> Vec<u8> {
        let encoded = format!("{}{}", ENCRYPTED_PREFIX, B64.encode(ciphertext));
        encoded.into_bytes()
    }

    /// Encode ciphertext that was produced from a **deflated** plaintext.
    fn encode_deflated_ciphertext(ciphertext: &[u8]) -> Vec<u8> {
        let encoded = format!("{}{}", DEFLATED_PREFIX, B64.encode(ciphertext));
        encoded.into_bytes()
    }

    fn encode_binary_ciphertext(ciphertext: &[u8], deflated: bool) -> Vec<u8> {
        encode_binary_ciphertext(ciphertext, deflated)
    }

    /// Seal one value: deflate first when that is smaller, then encrypt.
    ///
    /// Compression happens **before** encryption because ciphertext is
    /// incompressible — a value sealed without being compressed can never be
    /// shrunk again by compaction, the backup chunk plane, or the cloud bill.
    /// That order is the standing rule in
    /// `preference-lastdb-storage-compress-atoms-and-blobs`.
    ///
    /// Falls back to a plain `ENC:` seal whenever the write-side switch is off,
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
        Ok(if self.compression.raw {
            Self::encode_binary_ciphertext(&ciphertext, deflated.is_some())
        } else if deflated.is_some() {
            Self::encode_deflated_ciphertext(&ciphertext)
        } else {
            Self::encode_ciphertext(&ciphertext)
        })
    }

    /// Open one stored value.
    ///
    /// `Ok(Some(plaintext))` when the value carried an at-rest envelope.
    /// `Ok(None)` when it did not: this store only ever wraps namespaces LastDB
    /// encrypts, so an un-enveloped value there is not data this store wrote.
    /// It is counted, warned about once per namespace, and treated as absent.
    ///
    /// Before `decision-2026-09-14-drop-dual-read-unsealed-is-gone` such a row
    /// was returned as cleartext and then re-sealed in place on the read path,
    /// which turned a stray plaintext write into a durable, trusted row.
    pub(crate) async fn open_sealed_value(
        &self,
        data: Vec<u8>,
        crypto: &dyn CryptoProvider,
    ) -> StorageResult<Option<Vec<u8>>> {
        if let Some((deflated, ciphertext)) = decode_binary_ciphertext(&data)
            .map_err(|e| StorageError::EncryptionError(e.to_string()))?
        {
            let plaintext = crypto
                .decrypt(ciphertext)
                .await
                .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
            if deflated {
                let inflated = inflate_at_rest(&plaintext)
                    .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
                return Ok(Some(inflated));
            }
            return Ok(Some(plaintext));
        }

        // Check for either base64 sealed prefix. Both are four bytes wide, so one
        // slice serves both. `ENZ:` is decoded unconditionally and is never
        // gated on the write-side switch: turning compression off must stop new
        // compressed writes without stranding a single row already on disk.
        let deflated = data.starts_with(DEFLATED_PREFIX.as_bytes());
        if deflated || data.starts_with(ENCRYPTED_PREFIX.as_bytes()) {
            let b64_part = &data[ENCRYPTED_PREFIX.len()..];
            let ciphertext = B64
                .decode(b64_part)
                .map_err(|e| StorageError::EncryptionError(format!("Base64 decode failed: {e}")))?;
            let plaintext = crypto
                .decrypt(&ciphertext)
                .await
                .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
            if deflated {
                let inflated = inflate_at_rest(&plaintext)
                    .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
                return Ok(Some(inflated));
            }
            return Ok(Some(plaintext));
        }

        // No at-rest envelope in a namespace this store encrypts. Not ours.
        crate::crypto::record_unsealed_discard(&self.namespace);
        Ok(None)
    }

    /// Open a stored value, or report it absent when it carries no envelope.
    ///
    /// `context` names the read for the discard log.
    async fn open_or_discard(&self, key: &[u8], stored: Vec<u8>) -> StorageResult<Option<Vec<u8>>> {
        let crypto = self.select_crypto(key);
        self.open_sealed_value(stored, crypto.as_ref()).await
    }

    async fn decrypt_rows(
        &self,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut decrypted_rows = Vec::with_capacity(rows.len());
        for (key, stored) in rows {
            // A row with no envelope is absent, so it is simply not in the scan.
            if let Some(plaintext) = self.open_or_discard(&key, stored).await? {
                self.admit_plain(&key, &plaintext);
                decrypted_rows.push((key, plaintext));
            }
        }
        Ok(decrypted_rows)
    }

    fn lossy_derived_key(key: &[u8]) -> bool {
        key.starts_with(b"emb:")
    }

    /// Seal plaintext for one explicit ENB target, ignoring write switches.
    async fn seal_explicit_enb(
        &self,
        key: &[u8],
        plaintext: &[u8],
        target: ResealAtRestTarget,
    ) -> StorageResult<(Vec<u8>, bool)> {
        let crypto = self.select_crypto(key);
        let deflated = compress_for_seal(
            plaintext,
            AT_REST_COMPRESS_MIN_BYTES_DEFAULT,
            at_rest_max_inflated_bytes(),
            target == ResealAtRestTarget::BinaryCompress,
        );
        let to_encrypt: &[u8] = deflated.as_deref().unwrap_or(plaintext);
        let ciphertext = crypto
            .encrypt(to_encrypt)
            .await
            .map_err(|e| StorageError::EncryptionError(e.to_string()))?;
        let used_deflate = deflated.is_some();
        Ok((
            encode_binary_ciphertext(&ciphertext, used_deflate),
            used_deflate,
        ))
    }
}

mod kv_store_impl;
mod maintenance;
