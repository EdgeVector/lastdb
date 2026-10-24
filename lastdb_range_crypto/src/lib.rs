//! Portable candidate range codec; the serving disk format does not select it.
//!
//! The construction is binary CLWW OPE (FSE 2016, section 3.2), with a
//! non-wrapping PRF instantiation. Independent 128-bit output blocks retain
//! the published first-differing-bit leakage; no suffix carry crosses a block.
//! The PRF excludes its maximum output through bounded rejection sampling.
//! Its statistical distance from uniform is at most Q / 2^128 over Q prefix
//! evaluations, in addition to the HMAC PRF assumption. Review this exact
//! instantiation before activation; no restricted vendor source is used.
//!
//! Terms compare by ordinary unsigned byte order within one key scope. They
//! deliberately expose order, equality, length, and additional information.
//! The variable-length framing and bounded PRF still require independent
//! review. Terms are not authenticated content or a completeness proof.
//!
//! The crate accepts whole byte strings, not a narrowed integer or a digest
//! of the range. Prefix bounds may contain bytes that are not valid UTF-8.
//! Every allocation has an explicit caller limit or a checked context size.

#![forbid(unsafe_code)]

mod key;
mod molecule;
mod prf;
mod protected;
mod root;
mod term;
mod wrapping;

pub use key::{EncryptedRangeBounds, RangeOpeKey};
pub use molecule::RangeMoleculeKey;
pub use protected::ProtectedRange;
pub use root::{RangeOpeRoot, RangeScope};
pub use term::RangeTerm;

/// LastDB-specific candidate identity, independent of any provider version.
pub const FORMAT_ID: &str = "lastdb:clww-binary128-nonwrapping:hmac-sha256:v1";

/// One independent 128-bit output block for each input bit.
pub const BIT_BLOCK_BYTES: usize = 16;

/// Non-secret errors. An error never contains a private key or range value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A requested output or context cannot fit its size boundary.
    #[error("range crypto byte limit exceeded")]
    ByteLimit,
    /// The allocator refused the bounded output or context.
    #[error("range crypto allocation refused")]
    Allocation,
    /// Molecule identity is absent or cannot be framed.
    #[error("range crypto scope is invalid")]
    InvalidScope,
    /// The supplied representation is not a term of this format.
    #[error("range crypto term is invalid")]
    InvalidTerm,
    /// A cryptographic primitive refused the configured key length.
    #[error("range crypto key derivation failed")]
    KeyDerivation,
    /// The authenticated range or scoped grant does not match its context.
    #[error("range crypto authentication failed")]
    Authentication,
    /// The operating system could not supply a nonce.
    #[error("range crypto nonce source failed")]
    Entropy,
    /// All bounded PRF draws were excluded; no ciphertext was produced.
    #[error("range crypto prefix PRF refused")]
    PrfRefused,
}

/// Fallible result for the portable range codec.
pub type Result<T> = std::result::Result<T, Error>;

/// Raw ciphertext bytes for a range of `input_bytes`, including the marker.
pub fn term_bytes(input_bytes: usize) -> Result<usize> {
    input_bytes
        .checked_add(1)
        .and_then(|length| length.checked_mul(8))
        .and_then(|length| length.checked_mul(BIT_BLOCK_BYTES))
        .ok_or(Error::ByteLimit)
}
