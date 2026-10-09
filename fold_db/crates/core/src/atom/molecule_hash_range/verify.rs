//! Canonical bytes and signature verification.

use super::MoleculeHashRange;

impl MoleculeHashRange {
    /// Builds canonical bytes for per-key signing/verification.
    ///
    /// Layout: each variable-length field is preceded by a 4-byte big-endian
    /// length prefix and followed by its raw bytes, in this fixed order:
    /// `len(molecule_uuid) || molecule_uuid || len(hash_value) || hash_value
    ///  || len(range_value) || range_value || len(atom_uuid) || atom_uuid
    ///  || written_at.to_be_bytes()`.
    ///
    /// Length-prefixing (rather than a `0x00` separator) is required because
    /// `hash_value` and `range_value` are user-supplied strings on every
    /// HashRange-keyed schema and may legitimately contain `0x00` bytes. With
    /// a separator, an attacker could shift bytes across the
    /// hash_value/range_value boundary so two semantically-different
    /// `(hash, range)` tuples produce byte-identical canonical bytes — letting
    /// one Ed25519 signature verify both entries. Mirrors the share-rule fix
    /// in #408. See `nul_in_hash_or_range_does_not_collide_with_shifted_neighbour`.
    pub(super) fn build_canonical_bytes(
        molecule_uuid: &str,
        hash_value: &str,
        range_value: &str,
        atom_uuid: &str,
        written_at: u64,
    ) -> Vec<u8> {
        crate::canonical::CanonicalWriter::with_capacity(
            molecule_uuid.len()
                + hash_value.len()
                + range_value.len()
                + atom_uuid.len()
                + 8
                + 4 * 4,
        )
        .field(molecule_uuid.as_bytes())
        .field(hash_value.as_bytes())
        .field(range_value.as_bytes())
        .field(atom_uuid.as_bytes())
        .u64(written_at)
        .finish()
    }

    /// Verifies the signature for a specific hash+range key entry.
    /// Verifies a signature over the canonical bytes of a prospective entry
    /// WITHOUT requiring the entry to exist in any molecule yet. Used by the
    /// inbound `data_share` import gate — see
    /// `MoleculeHash::verify_imported_parts` for full rationale. Same
    /// canonical layout as `verify_key`.
    #[must_use]
    pub fn verify_imported_parts(
        molecule_uuid: &str,
        hash_value: &str,
        range_value: &str,
        atom_uuid: &str,
        written_at: u64,
        signature: &str,
        writer_pubkey: &str,
    ) -> bool {
        let canonical = Self::build_canonical_bytes(
            molecule_uuid,
            hash_value,
            range_value,
            atom_uuid,
            written_at,
        );
        crate::security::verify_molecule_signature(&canonical, signature, writer_pubkey)
    }

    #[must_use]
    pub fn verify_key(&self, hash_value: &str, range_value: &str) -> bool {
        let Some(entry) = self
            .atom_uuids
            .get(hash_value)
            .and_then(|rm| rm.get(range_value))
        else {
            return false;
        };
        if entry.signature_version == 0 {
            return false;
        }
        let canonical = Self::build_canonical_bytes(
            &self.uuid,
            hash_value,
            range_value,
            &entry.atom_uuid,
            entry.written_at,
        );
        crate::security::verify_molecule_signature(
            &canonical,
            &entry.signature,
            &entry.writer_pubkey,
        )
    }
}
