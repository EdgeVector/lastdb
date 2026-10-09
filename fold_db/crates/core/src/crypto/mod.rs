//! Cryptographic primitives for E2E encryption.
//!
//! This module provides:
//! - **`CryptoProvider`** trait — abstract encrypt/decrypt interface
//! - **`LocalCryptoProvider`** — file-based AES-256-GCM for standalone dev nodes
//! - **`NoOpCryptoProvider`** — passthrough for tests and migration
//! - **`envelope`** — self-describing binary ciphertext format
//! - **`keyring`** — KEK-wrapped per-purpose DEKs (`keyring.enc`)
//! - **`keyring_store`** — on-disk lifecycle for `keyring.enc`
//! - **`keyring_provider`** — `CryptoProvider` backed by the keyring (the seam adapter)
//! - **`at_rest`** — shared synchronous `ENC:` / `ENZ:` / binary `ENB:` value
//!   codec for stores that bypass the namespaced encryption seam

pub mod at_rest;
pub mod e2e;
pub mod envelope;
pub mod error;
pub mod keyring;
pub mod keyring_provider;
pub mod keyring_store;
pub mod provider;

pub use at_rest::{
    at_rest_compression_enabled, at_rest_compression_stats, at_rest_raw_enabled, is_sealed_at_rest,
    open_at_rest, record_unsealed_discard, record_unsealed_reap, seal_at_rest,
    seal_at_rest_deflate, seal_at_rest_deflate_raw, seal_at_rest_deflate_utf8, seal_at_rest_raw,
    seal_at_rest_utf8, unsealed_discarded_count, unsealed_reaped_totals, AtRestCompressionStats,
    AT_REST_ENC_BINARY_PREFIX, AT_REST_ENC_DEFLATE_PREFIX, AT_REST_ENC_PREFIX,
};
pub use e2e::{at_rest_kek_from_seed, E2eKeys};
pub use envelope::{
    decrypt_envelope, decrypt_envelope_v2, decrypt_envelope_with_context, encrypt_envelope,
    encrypt_envelope_v2, peek_envelope, EnvelopeHeader, KeyId,
};
pub use error::{CryptoError, CryptoResult};
pub use keyring::{Dek, KeyPurpose, Keyring};
pub use keyring_provider::KeyringCryptoProvider;
pub use keyring_store::{keyring_path, load_or_init, KEYRING_FILENAME};
pub use provider::{CryptoProvider, LocalCryptoProvider, NoOpCryptoProvider};
pub mod inbox;
