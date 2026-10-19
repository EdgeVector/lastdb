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
/// no bare dual-read. Missing atoms under a scoped read are skipped with a
/// warning (shared/replay can leave orphan molecule refs); personal reads
/// still surface integrity errors below when both miss and storage_prefix is None.
///
/// Each match is a [`KeyedAtomMatch`].
pub async fn fetch_atoms_with_key_metadata_async_with_prefix(
    db_ops: &Arc<DbOperations>,
    matches: impl IntoIterator<Item = KeyedAtomMatch>,
    storage_prefix: Option<&str>,
    molecule_uuid: Option<&str>,
    schema: Option<&str>,
    field: Option<&str>,
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
        if let Some(atom) = atom {
            let value = field_value_from_atom(&atom, atom_uuid, key_meta, writer_pubkey);
            crate::storage::disk_pack::logical_paths::field::admit(
                runtime.clone(),
                storage_prefix,
                molecule_uuid,
                &key,
                &value,
            )?;
            resolved_values.insert(key, value);
        } else {
            let miss = UnresolvedMatch {
                key: &key,
                atom_uuid: &atom_uuid,
            };
            handle_unresolved_atom(db_ops, storage_prefix, molecule_uuid, schema, field, &miss);
        }
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

/// One match whose atom body was not found.
struct UnresolvedMatch<'a> {
    key: &'a KeyValue,
    atom_uuid: &'a str,
}

/// Skip a match whose atom body is missing (the caller drops the row).
fn handle_unresolved_atom(
    db_ops: &Arc<DbOperations>,
    storage_prefix: Option<&str>,
    molecule_uuid: Option<&str>,
    schema: Option<&str>,
    field: Option<&str>,
    miss: &UnresolvedMatch<'_>,
) {
    let UnresolvedMatch { key, atom_uuid } = miss;
    if storage_prefix.is_some() {
        // Intentional filter (not a bug-skip): the function-level
        // doc comment on the fetch function explains that org-scoped reads must
        // tolerate orphan molecule refs whose pre-tag atoms never
        // replayed through the org log, otherwise every
        // unfiltered query on a shared schema is unusable. Deliberately
        // NOT counted as an unresolved skip — `unresolved_rows` is the
        // integrity signal for a genuinely dangling tip -> atom edge,
        // and counting the expected org-scope filtering here would keep
        // it permanently non-zero on shared schemas.
        tracing::warn!(
            "Filtering orphan atom ref from org-scoped read — pre-tag \
             molecule ref has no atom data, intentionally skipped per \
             alpha BLOCKER 4b171 design (see fetch_atoms_with_key_metadata_async_with_prefix docs)"
        );
        return;
    }
    // A genuinely dangling tip -> atom edge. The edge is already broken
    // on disk; erroring here amplified one bad row into an unreadable
    // partition, so skip the row and let the caller report the count.
    db_ops.record_unresolved_atom_skip(
        atom_uuid,
        key,
        crate::db_operations::core::UnresolvedAtomContext {
            molecule_uuid,
            schema,
            field,
            tip_storage_key: None,
        },
    );
    if key.to_string().is_empty() {
        tracing::warn!(
            atom_uuid = %atom_uuid,
            "Skipping unresolved atom ref from query result"
        );
        return;
    }
    tracing::warn!(
        atom_uuid = %atom_uuid,
        key = %key,
        "Skipping unresolved atom ref from query result"
    );
}
