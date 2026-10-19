//! Atom UUID resolution for filtered field values.

use crate::db_operations::DbOperations;
use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;
use std::collections::HashMap;
use std::sync::Arc;

mod batch;
pub(crate) use batch::{fetch_field_atom_batches, FieldAtomBatch};

/// One filtered match, carried from tip scan to body hydrate:
/// `(key, atom_uuid, key_metadata, writer_pubkey)`.
pub type KeyedAtomMatch = (
    KeyValue,
    String,
    Option<crate::atom::KeyMetadata>,
    Option<String>,
);

/// Resolve atom UUID matches into concrete FieldValue map under an optional
/// storage prefix (`storage_prefix` / share `from:{sender}`). Exact-prefix only —
/// no bare dual-read. Every selected tip requires its body in that same scope.
///
/// Each match is a [`KeyedAtomMatch`].
pub async fn fetch_atoms_with_key_metadata_async_with_prefix(
    db_ops: &Arc<DbOperations>,
    matches: impl IntoIterator<Item = KeyedAtomMatch>,
    storage_prefix: Option<&str>,
    molecule_uuid: Option<&str>,
) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
    let mut resolved_values: HashMap<KeyValue, FieldValue> = HashMap::new();

    let runtime = db_ops.namespaced_store().disk_pack_runtime();
    let mut misses = Vec::new();
    for entry in matches {
        if let Some(value) = crate::storage::disk_pack::logical_paths::field::lookup(
            runtime.clone(),
            storage_prefix,
            molecule_uuid,
            &crate::storage::disk_pack::logical_paths::field::AtomSelection {
                entry: &entry,
                primary: db_ops.atoms().body_storage_key(&entry.1, storage_prefix),
            },
        )? {
            resolved_values.insert(entry.0, value);
        } else {
            misses.push(entry);
        }
    }
    let uuids: Vec<&str> = misses
        .iter()
        .map(|(_, atom_uuid, _, _)| atom_uuid.as_str())
        .collect();
    let atoms =
        crate::db_operations::tip_atoms::get_atoms_for_tips(db_ops, &uuids, storage_prefix).await?;

    for ((key, atom_uuid, key_meta, writer_pubkey), atom) in misses.into_iter().zip(atoms) {
        let value = field_value_from_atom(&atom, atom_uuid, key_meta, writer_pubkey);
        crate::storage::disk_pack::logical_paths::field::admit(
            runtime.clone(),
            storage_prefix,
            molecule_uuid,
            &key,
            &value,
        )?;
        resolved_values.insert(key, value);
    }

    Ok(resolved_values)
}

/// Build the `FieldValue` for a resolved atom.
pub(crate) fn field_value_from_atom(
    atom: &crate::atom::Atom,
    atom_uuid: String,
    key_meta: Option<crate::atom::KeyMetadata>,
    writer_pubkey: Option<String>,
) -> FieldValue {
    // Prefer molecule per-key metadata, fall back to atom metadata
    let (source_file_name, metadata) = match key_meta {
        Some(km) => (
            km.source_file_name
                .or_else(|| atom.source_file_name().cloned()),
            km.metadata.or_else(|| atom.metadata().cloned()),
        ),
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
        molecule_version: None,
        // Per-key writer_pubkey looked up from the molecule's
        // AtomEntry. For Hash/Range/HashRange variants this is
        // the only place writer_pubkey gets surfaced (the
        // molecule has no molecule-level signing key); for
        // Single this path will be `None` here and overwritten
        // by `FieldVariant::resolve_value` with the
        // molecule-level pubkey.
        writer_pubkey: writer_pubkey.filter(|s| !s.is_empty()),
        written_at,
    }
}
