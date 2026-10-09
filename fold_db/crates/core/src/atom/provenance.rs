//! Provenance types for molecules.
//!
//! User writes carry a signature. `Derived` is retained as historical wire
//! compatibility for molecules that may already have been serialized with
//! computed-write provenance; live mutation writes reject it before storage.

use serde::{Deserialize, Serialize};

/// Writer identity and verifiability information for a molecule.
///
/// `User` — signed by an end-user's keypair; authority is by signature.
/// `Derived` -- historical computed-write provenance; unsigned. The field
/// names remain stable for compatibility with already-serialized data, but live
/// writes no longer mint or accept this variant.
/// `encoding_version = 2` (current) length-prefixes
/// `MoleculeRef::canonical_bytes` so that `sources_merkle_root` is
/// injective. v1 used `0x00` separators and was vulnerable to a NUL-shift
/// collision across the atom_uuid/key boundary.
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
        /// matching `AtomEntry canonical bytes`).
        signature_version: u8,
    },
    /// Historical computed-output write. Unsigned; retained for wire
    /// compatibility and rejected on live writes.
    Derived {
        /// Historical SHA-256 hex of the compute module bytes that produced
        /// this molecule.
        wasm_hash: String,
        /// SHA-256 hex of the canonical input snapshot. This is the content
        /// address of the historical computed inputs.
        input_snapshot_hash: String,
        /// SHA-256 hex of the Merkle root over the source `MoleculeRef`s.
        /// The full source set lives in local rebuildable indexes (PR 6),
        /// not on the molecule.
        sources_merkle_root: String,
        /// Canonicalization version for `input_snapshot_hash` and the Merkle
        /// leaves. `2` is current — `1` joined `MoleculeRef::canonical_bytes`
        /// fields with `0x00` separators (NUL-shift collision across the
        /// atom_uuid/key boundary). Bump if and only if the canonical byte
        /// layout changes — a change here changes the content address, so
        /// treat as forever.
        encoding_version: u8,
    },
}

impl Provenance {
    /// Constructor for `User` variant with `signature_version = 1` (the only
    /// version currently defined).
    #[must_use]
    pub fn user(pubkey: String, signature: String) -> Self {
        Self::User {
            pubkey,
            signature,
            signature_version: 1,
        }
    }

    /// Constructor for `Derived` variant with `encoding_version = 2`.
    ///
    /// Version 2 fixes the `MoleculeRef::canonical_bytes` Merkle-leaf
    /// encoding to length-prefix `molecule_uuid` / `atom_uuid` / `key`
    /// instead of joining them with a single-byte `0x00` separator (the
    /// pre-fix v1 layout was vulnerable to a `0x00`-shift collision across
    /// the atom_uuid/key boundary — same bug class as PR #408 / #409 /
    /// #438 / #422). `input_snapshot_hash` is unchanged.
    #[must_use]
    pub fn derived(
        wasm_hash: String,
        input_snapshot_hash: String,
        sources_merkle_root: String,
    ) -> Self {
        Self::Derived {
            wasm_hash,
            input_snapshot_hash,
            sources_merkle_root,
            encoding_version: 2,
        }
    }
}

/// A field's molecule signature in transplantable form: everything a remote
/// node needs to (a) verify the original author signed this exact value
/// (`verify_imported_parts` on the matching molecule type, with the
/// content-derived `atom_uuid` and deterministic `molecule_uuid` recomputed
/// locally) and (b) store the entry so the same signature stays verifiable
/// at rest (the `*_imported` setters with the signed `written_at`/`version`
/// preserved). Produced by `FieldVariant::signed_entry_provenance`; carried
/// on the `data_share` wire by fold_db_node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportedFieldProvenance {
    /// Content-derived atom UUID the author signed. The importer MUST
    /// re-derive this from the received value and reject on mismatch --
    /// it is the link between the signature and the actual data.
    pub atom_uuid: String,
    /// The `written_at` (nanos) inside the signed canonical bytes.
    pub written_at: u64,
    /// The signed `version` -- `Some` only for single (non-keyed) molecules;
    /// per-key entries do not sign a version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    /// Base64 Ed25519 public key of the original author.
    pub writer_pubkey: String,
    /// Base64 Ed25519 signature over the molecule canonical bytes.
    pub signature: String,
    /// Signature scheme version (1 = current canonical layout).
    pub signature_version: u8,
}

/// Canonical reference to a single atom version on a single molecule.
///
/// Used as a Merkle leaf for `Provenance::Derived::sources_merkle_root` and
/// as the payload for the forward/reverse lineage indexes (PR 6). The
/// `written_at` pins recomputation to the exact source version even if the
/// molecule has moved on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeRef {
    pub molecule_uuid: String,
    pub atom_uuid: String,
    /// `None` for single-keyed molecules, `Some(k)` for range-keyed molecules.
    pub key: Option<String>,
    /// Nanoseconds since the Unix epoch at which this atom was written.
    pub written_at: u64,
}

impl MoleculeRef {
    /// Canonical byte encoding used as a Merkle leaf.
    ///
    /// Layout (governed by `Provenance::Derived::encoding_version` — bump
    /// the variant's version if this ever changes again):
    ///
    /// ```text
    /// len(molecule_uuid) | molecule_uuid
    ///   | len(atom_uuid) | atom_uuid
    ///   | len(key_or_empty) | key_or_empty
    ///   | written_at(u64 BE)
    /// ```
    ///
    /// Each variable-length field is preceded by a 4-byte big-endian `u32`
    /// length prefix. `key_or_empty` is the empty byte string when `key`
    /// is `None` and the UTF-8 bytes of the key otherwise — so `None` and
    /// `Some("")` continue to canonicalize identically (deliberate; see
    /// `molecule_ref_canonical_bytes_distinguishes_none_from_empty_string_key`).
    ///
    /// Length-prefixing (rather than a `0x00` separator) is required
    /// because both `atom_uuid` (peer-supplied on the import path —
    /// `MoleculeRange::set_atom_uuid_imported`) and `key` (user-supplied
    /// range key on every Range-keyed schema) can legitimately contain a
    /// `0x00` byte. With a single-byte separator, an attacker could shift
    /// a `0x00` byte across either the molecule_uuid/atom_uuid or the
    /// atom_uuid/key boundary so two semantically-different `MoleculeRef`s
    /// produced byte-identical canonical bytes — which becomes a
    /// Merkle-leaf collision in `Provenance::Derived::sources_merkle_root`
    /// (via `view::derived_metadata::compute_derived_metadata`),
    /// defeating the "verifiable by recomputation" authority the variant
    /// is documented to provide.
    ///
    /// Same bug class and fix as PR #408 (share-rule canonical bytes),
    /// PR #409 (`MoleculeHashRange`), PR #438 (`MoleculeRange`), and
    /// PR #422 (`hash_input_snapshot`). Pinned by
    /// `nul_shift_across_field_boundary_does_not_collide`.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let key_bytes = self.key.as_deref().unwrap_or("").as_bytes();
        crate::canonical::CanonicalWriter::with_capacity(
            self.molecule_uuid.len() + self.atom_uuid.len() + key_bytes.len() + 8 + 3 * 4,
        )
        .field(self.molecule_uuid.as_bytes())
        .field(self.atom_uuid.as_bytes())
        .field(key_bytes)
        .u64(self.written_at)
        .finish()
    }
}
