//! Storage key layout for protein records and member back-refs.

/// Durable protein record: `protein:{uuid}` → [`super::Protein`].
pub const PROTEIN_RECORD_PREFIX: &str = "protein:";

#[must_use]
pub fn protein_record_key(protein_uuid: &str) -> String {
    format!("{PROTEIN_RECORD_PREFIX}{protein_uuid}")
}

/// Bi-directional member back-ref: `molprot:{molecule_uuid}` → protein uuid string.
pub const MEMBER_BACKREF_PREFIX: &str = "molprot:";

#[must_use]
pub fn member_backref_key(molecule_uuid: &str) -> String {
    format!("{MEMBER_BACKREF_PREFIX}{molecule_uuid}")
}
