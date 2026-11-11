//! Cryptographic primitives for E2E encryption.

pub mod at_rest;
pub mod e2e;
pub mod envelope;
pub mod error;

pub use at_rest::{is_sealed_at_rest, seal_at_rest_raw, AT_REST_ENC_BINARY_PREFIX};
pub use e2e::E2eKeys;
pub use envelope::{decrypt_envelope, encrypt_envelope};
pub use error::{CryptoError, CryptoResult};
