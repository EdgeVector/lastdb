//! Writer provenance of a mutation.
//!
//! A user write carries a signature. A record names no writer (decision D7),
//! so a record write refuses a mutation that carries one.

use serde::{Deserialize, Serialize};

/// Writer identity and verifiability information for a mutation.
///
/// `User` — signed by an end-user's keypair; authority is by signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Provenance {
    /// User-originated write. Signed by the user's Ed25519 keypair.
    User {
        /// Base64-encoded Ed25519 public key of the signer.
        pubkey: String,
        /// Base64-encoded Ed25519 signature over canonical bytes.
        signature: String,
        /// Signature scheme version (1 = hand-rolled canonical concat).
        signature_version: u8,
    },
}
