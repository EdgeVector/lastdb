//! Provenance types for field keys.
//!
//! User writes carry a signature. `Derived` is retained as historical wire
//! compatibility for field keys that may already have been serialized with
//! computed-write provenance; live mutation writes reject it before storage.

use serde::{Deserialize, Serialize};

/// Writer identity and verifiability information for a field key.
///
/// `User` — signed by an end-user's keypair; authority is by signature.
/// `Derived` -- historical computed-write provenance; unsigned. The field
/// names remain stable for compatibility with already-serialized data, but live
/// writes no longer mint or accept this variant.
/// `encoding_version = 2` (current) length-prefixes
/// `FieldKeyRef::canonical_bytes` so that `sources_merkle_root` is
/// injective. v1 used `0x00` separators and was vulnerable to a NUL-shift
/// collision across the value_id/key boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Provenance {
    /// User-originated write. Signed by the user's Ed25519 keypair.
    User {
        /// Base64-encoded Ed25519 public key of the signer.
        pubkey: String,
        /// Base64-encoded Ed25519 signature over canonical bytes.
        signature: String,
        /// Signature scheme version (1 = hand-rolled canonical concat,
        /// matching `RecordEntry canonical bytes`).
        signature_version: u8,
    },
    /// Historical computed-output write. Unsigned; retained for wire
    /// compatibility and rejected on live writes.
    Derived {
        /// Historical SHA-256 hex of the compute module bytes that produced
        /// this field key.
        wasm_hash: String,
        /// SHA-256 hex of the canonical input snapshot. This is the content
        /// address of the historical computed inputs.
        input_snapshot_hash: String,
        /// SHA-256 hex of the Merkle root over the source `FieldKeyRef`s.
        /// The full source set lives in local rebuildable indexes (PR 6),
        /// not on the field key.
        sources_merkle_root: String,
        /// Canonicalization version for `input_snapshot_hash` and the Merkle
        /// leaves. `2` is current — `1` joined `FieldKeyRef::canonical_bytes`
        /// fields with `0x00` separators (NUL-shift collision across the
        /// value_id/key boundary). Bump if and only if the canonical byte
        /// layout changes — a change here changes the content address, so
        /// treat as forever.
        encoding_version: u8,
    },
}
