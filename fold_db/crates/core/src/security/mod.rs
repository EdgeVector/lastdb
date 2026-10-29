//! Security module for client key management, message signing, and encryption
//!
//! ## Encryption Architecture
//!
//! FoldDB uses **end-to-end (E2E) encryption** as its sole encryption mechanism.
//! The E2E key is derived from the node identity seed via HKDF-SHA256: one
//! AES-256-GCM content key. The disk home and the cloud backup use it. HashKey
//! tokens use each molecule bundle's own blind key, not a key derived here.
//!
//! This module provides:
//! - Ed25519 key pair generation and management
//! - Message signing and verification
//! - Integration with network and permissions layers

pub mod keys;
pub mod signing;
pub mod types;

pub use keys::*;
pub use signing::*;
pub use types::PublicKeyInfo;

use thiserror::Error;

/// Security-related errors
#[derive(Error, Debug)]
pub enum SecurityError {
    #[error("Key generation failed: {0}")]
    KeyGenerationFailed(String),

    #[error("Signature verification failed: {0}")]
    SignatureVerificationFailed(String),

    #[error("Invalid public key: {0}")]
    InvalidPublicKey(String),

    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),
}

pub type SecurityResult<T> = Result<T, SecurityError>;
