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

/// Canonical reference to a single record version on a single field key.
///
/// Used as a Merkle leaf for `Provenance::Derived::sources_merkle_root` and
/// as the payload for the forward/reverse lineage indexes (PR 6). The
/// `written_at` pins recomputation to the exact source version even if the
/// field key has moved on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldKeyRef {
    #[serde(alias = "molecule_uuid")]
    pub field_key_id: String,
    #[serde(alias = "atom_uuid")]
    pub value_id: String,
    /// `None` for single-keyed field keys, `Some(k)` for range-keyed field keys.
    pub key: Option<String>,
    /// Nanoseconds since the Unix epoch at which this record was written.
    pub written_at: u64,
}

impl FieldKeyRef {
    /// Canonical byte encoding used as a Merkle leaf.
    ///
    /// Layout (governed by `Provenance::Derived::encoding_version` — bump
    /// the variant's version if this ever changes again):
    ///
    /// ```text
    /// len(field_key_id) | field_key_id
    ///   | len(value_id) | value_id
    ///   | len(key_or_empty) | key_or_empty
    ///   | written_at(u64 BE)
    /// ```
    ///
    /// Each variable-length field is preceded by a 4-byte big-endian `u32`
    /// length prefix. `key_or_empty` is the empty byte string when `key`
    /// is `None` and the UTF-8 bytes of the key otherwise — so `None` and
    /// `Some("")` continue to canonicalize identically (deliberate; see
    /// `field_key_ref_canonical_bytes_distinguishes_none_from_empty_string_key`).
    ///
    /// Length-prefixing (rather than a `0x00` separator) is required
    /// because both `value_id` (peer-supplied on the import path —
    /// `FieldKeyRange::set_value_id_imported`) and `key` (user-supplied
    /// range key on every Range-keyed schema) can legitimately contain a
    /// `0x00` byte. With a single-byte separator, an attacker could shift
    /// a `0x00` byte across either the field_key_id/value_id or the
    /// value_id/key boundary so two semantically-different `FieldKeyRef`s
    /// produced byte-identical canonical bytes — which becomes a
    /// Merkle-leaf collision in `Provenance::Derived::sources_merkle_root`
    /// (via `view::derived_metadata::compute_derived_metadata`),
    /// defeating the "verifiable by recomputation" authority the variant
    /// is documented to provide.
    ///
    /// Same bug class and fix as PR #408 (share-rule canonical bytes),
    /// PR #409 (`FieldKeyHashRange`), PR #438 (`FieldKeyRange`), and
    /// PR #422 (`hash_input_snapshot`). Pinned by
    /// `nul_shift_across_field_boundary_does_not_collide`.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let key_bytes = self.key.as_deref().unwrap_or("").as_bytes();
        crate::canonical::CanonicalWriter::with_capacity(
            self.field_key_id.len() + self.value_id.len() + key_bytes.len() + 8 + 3 * 4,
        )
        .field(self.field_key_id.as_bytes())
        .field(self.value_id.as_bytes())
        .field(key_bytes)
        .u64(self.written_at)
        .finish()
    }
}
