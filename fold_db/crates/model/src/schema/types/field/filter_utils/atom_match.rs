//! One selected tip and its projection into a field value.

use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;

/// One filtered match, carried from tip scan to body hydrate:
/// `(key, atom_uuid, key_metadata, writer_pubkey)`.
pub type KeyedAtomMatch = (
    KeyValue,
    String,
    Option<crate::atom::KeyMetadata>,
    Option<String>,
);

/// Build the `FieldValue` for a resolved atom.
pub fn field_value_from_atom(
    atom: &crate::atom::Atom,
    atom_uuid: String,
    key_meta: Option<crate::atom::KeyMetadata>,
    writer_pubkey: Option<String>,
) -> FieldValue {
    // A selected tip owns its complete optional metadata. An explicit None
    // clears that field; it must not inherit an older atom body's metadata.
    // Without a key-metadata record, the atom's own metadata is the only
    // available source (including direct molecule and physical atom reads).
    let (source_file_name, metadata) = match key_meta {
        Some(km) => (km.source_file_name, km.metadata),
        None => (atom.source_file_name().cloned(), atom.metadata().cloned()),
    };
    // Populate `written_at` from the atom's creation timestamp
    // so downstream view compute-as-mutations can build canonical
    // MoleculeRef leaves. `timestamp_nanos_opt` returns `None` for
    // timestamps outside the representable range (±292y from
    // epoch) — we skip `written_at` in that case rather than
    // falling back to a lossy u64 cast. Nanos are cast to `u64`
    // for dates after 1970; pre-1970 would be negative and also
    // skipped via the `try_into` guard.
    let written_at = atom
        .created_at()
        .timestamp_nanos_opt()
        .and_then(|ns| u64::try_from(ns).ok());
    FieldValue {
        value: atom.content().clone(),
        atom_uuid,
        source_file_name,
        metadata,
        molecule_uuid: None,
        // Per-key writer_pubkey looked up from the molecule's
        // AtomEntry. For Hash/Range/HashRange variants this is
        // the only place writer_pubkey gets surfaced (the
        // molecule has no molecule-level signing key); for
        // Single this path will be `None` here and overwritten
        // by `FieldVariant::stamp_molecule_meta` with the
        // molecule-level pubkey.
        writer_pubkey: writer_pubkey.filter(|s| !s.is_empty()),
        written_at,
    }
}
