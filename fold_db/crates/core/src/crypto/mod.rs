//! Cryptographic primitives for E2E encryption.
//!
//! This module provides:
//! - **`CryptoProvider`** trait — abstract encrypt/decrypt interface
//! - **`LocalCryptoProvider`** — AES-256-GCM under an explicit account content key
//! - **`envelope`** — self-describing binary ciphertext format
//! - **`at_rest`** — shared synchronous `ENC:` / binary `ENB:` value codec for
//!   stores that bypass the namespaced encryption seam

pub mod at_rest;
pub mod e2e;
pub mod envelope;
pub mod error;
pub mod provider;
pub mod stream;

pub use at_rest::{
    is_sealed_at_rest, seal_at_rest_raw, seal_at_rest_utf8, AT_REST_ENC_BINARY_PREFIX,
    AT_REST_ENC_PREFIX,
};
pub use e2e::E2eKeys;
pub use envelope::{decrypt_envelope, encrypt_envelope};
pub use error::{CryptoError, CryptoResult};
pub use provider::{CryptoProvider, LocalCryptoProvider};
