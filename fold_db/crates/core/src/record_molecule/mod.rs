//! One molecule per keyed record: envelope, compact, dual-read, dual-write.
//!
//! Dual-read is per key (document envelope `v==1`), not a catalog-wide flip.
//! Compact discovers keys by range under one hash-field molecule — no scan.

mod compact;
mod dual_read;
mod dual_write;
mod envelope;

pub use compact::{compact_record_molecule, compact_record_molecule_key, CompactReport};
pub use dual_read::{hash_range_key_from_envelope, overlay_document_envelope};
pub use dual_write::fold_mutations_to_record_envelope;
pub use envelope::{
    envelope_is_complete, is_document_envelope, merge_sent_fields, parse_document_fields,
    stamp_document_envelope, stamp_document_envelope_marked, ENVELOPE_VERSION,
};

use crate::schema::Schema;

pub(crate) use crate::schema::types::RECORD_SENTINEL;

/// Deterministic record-molecule UUID for a schema identity.
#[must_use]
pub fn record_molecule_uuid(schema_name: &str) -> String {
    crate::atom::deterministic_molecule_uuid(schema_name, RECORD_SENTINEL)
}

/// True when `name` is the runtime-only record-molecule field.
#[must_use]
pub fn is_record_molecule_field(name: &str) -> bool {
    name == RECORD_SENTINEL
}

/// Declared runtime field names (excludes the record-molecule sentinel).
#[must_use]
pub fn declared_runtime_field_names(schema: &Schema) -> Vec<String> {
    schema
        .runtime_fields
        .keys()
        .filter(|name| !is_record_molecule_field(name))
        .cloned()
        .collect()
}
