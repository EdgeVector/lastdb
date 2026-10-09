//! Atom UUID resolution for filtered field values.

use crate::db_operations::DbOperations;
use crate::schema::types::field::FieldValue;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;
use std::collections::HashMap;
use std::sync::Arc;

/// One filtered match, carried from tip scan to body hydrate:
/// `(key, atom_uuid, key_metadata, writer_pubkey, partition_hint)`.
///
/// The partition hint is optional and is derived from the **storage-form**
/// slot (before any OPE range remapping). When present, body hydrate uses
/// [`crate::db_operations::atom_store::AtomStore::get_atoms_located`] so a
/// partition-prefixed body resolves in one bounded batch without a locator
/// hop. A wrong or absent hint is only a slower read, never a wrong answer.
pub type KeyedAtomMatch = (
    KeyValue,
    String,
    Option<crate::atom::KeyMetadata>,
    Option<String>,
    Option<crate::atom::AtomPartition>,
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

    let matches: Vec<_> = matches.into_iter().collect();
    let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = matches
        .iter()
        .map(|(_, atom_uuid, _, _, partition)| (atom_uuid.as_str(), partition.clone()))
        .collect();
    let atoms = crate::db_operations::resident_read::get_atoms_located_resident_first(
        db_ops,
        &slots,
        storage_prefix,
    )
    .await?;

    for ((key, atom_uuid, key_meta, writer_pubkey, partition), atom) in
        matches.into_iter().zip(atoms)
    {
        if let Some(atom) = atom {
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
            resolved_values.insert(
                key,
                FieldValue {
                    value: atom.content().clone(),
                    atom_uuid: atom_uuid.clone(),
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
                },
            );
        } else {
            let key_str = key.to_string();
            if storage_prefix.is_some() {
                // Intentional filter (not a bug-skip): the function-level
                // doc comment above explains that org-scoped reads must
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
                continue;
            }
            // A genuinely dangling tip -> atom edge. The edge is already broken
            // on disk; erroring here amplified one bad row into an unreadable
            // partition, so skip the row and let the caller report the count.
            db_ops.record_unresolved_atom_skip(
                &atom_uuid,
                &key,
                crate::db_operations::core::UnresolvedAtomContext {
                    molecule_uuid,
                    schema,
                    field,
                    atom_partition: partition.as_ref(),
                    tip_storage_key: None,
                },
            );
            if key_str.is_empty() {
                tracing::warn!(
                    atom_uuid = %atom_uuid,
                    "Skipping unresolved atom ref from query result"
                );
                continue;
            }
            tracing::warn!(
                atom_uuid = %atom_uuid,
                key = %key,
                "Skipping unresolved atom ref from query result"
            );
            continue;
        }
    }

    Ok(resolved_values)
}
