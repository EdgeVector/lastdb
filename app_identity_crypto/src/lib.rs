//! Shared Ed25519, JCS (RFC 8785), and signed registry-list helpers.
//!
//! Used by `lastdb_node` for Ed25519 key parsing/signing/verification, RFC 8785
//! JSON canonicalization, and verification of signed app registry interface lists.
#![warn(missing_docs)]

mod ed25519;
mod error;
mod hex;
mod jcs;
mod registry_list;

pub use ed25519::{
    key_id, sign, verify, verifying_key_from_base64, SigningKey, VerifyingKey, PUBLIC_KEY_LEN,
    SIGNATURE_LEN,
};
pub use error::{CanonicalizeError, KeyParseError};
pub use hex::hex_lower;
pub use jcs::canonicalize;
pub use registry_list::{
    github_repo, interface_list_path, valid_app_id, valid_source, validate_channel,
    verify_index_bytes, IndexSignature, InterfaceList, ListEntry, LIST_CHANNELS, MAX_LIST_APPS,
    MAX_LIST_BYTES, REGISTRY_LIST_PUBKEY_B64,
};
