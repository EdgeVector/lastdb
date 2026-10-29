//! Cryptographic primitives for E2E encryption.
//!
//! This module provides:
//! - **`CryptoProvider`** trait — abstract encrypt/decrypt interface
//! - **`LocalCryptoProvider`** — AES-256-GCM under an explicit account content key
//! - **`envelope`** — self-describing binary ciphertext format
//! - **`at_rest`** — shared synchronous `ENC:` / binary `ENB:` value codec for
//!   stores that bypass the namespaced encryption seam

pub mod at_rest;
pub(crate) mod content_key_proof;
pub mod e2e;
pub mod envelope;
pub mod error;
pub mod provider;
pub mod stream;

pub use at_rest::{
    at_rest_compression_stats, is_sealed_at_rest, kv_at_rest_compression_enabled, open_at_rest,
    seal_at_rest_raw, seal_at_rest_utf8, AtRestCompressionStats, AT_REST_ENC_BINARY_PREFIX,
    AT_REST_ENC_PREFIX,
};
pub use e2e::E2eKeys;
pub use envelope::{
    decrypt_envelope, decrypt_envelope_v2, decrypt_envelope_with_context, encrypt_envelope,
    encrypt_envelope_v2, peek_envelope, EnvelopeHeader, KeyId,
};
pub use error::{CryptoError, CryptoResult};
pub use provider::{CryptoProvider, LocalCryptoProvider};
pub mod inbox;
