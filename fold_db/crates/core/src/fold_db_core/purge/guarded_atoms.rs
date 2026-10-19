//! Atom body accounting and the namespace-specific keys of a guarded erasure.

use super::*;

#[derive(Default)]
pub(super) struct AtomErasureAccounting {
    pub(super) file_pointer_atoms_purged: u64,
    pub(super) live_head_debits: Vec<(String, String, u64)>,
    pub(super) schema_markers: Vec<(String, String)>,
}

/// Capture each deleted atom's own source schema before its body disappears.
/// Marker removal and body removal use this same selected database scope.
pub(super) async fn account_atom_erasures(
    db_ops: &Arc<DbOperations>,
    atoms_to_delete: &[String],
    live_head_atoms: &HashSet<String>,
    storage_prefix: Option<&str>,
    acct: &mut PurgeCommitAccounting,
) -> Result<AtomErasureAccounting, SchemaError> {
    let started = std::time::Instant::now();
    let slots: Vec<_> = atoms_to_delete
        .iter()
        .map(|uuid| (uuid.as_str(), None))
        .collect();
    let atoms = db_ops
        .atoms()
        .get_atoms_located(&slots, storage_prefix)
        .await?;
    if atoms.len() != atoms_to_delete.len() {
        return Err(SchemaError::InvalidData(
            "the atom batch is incomplete before hard-delete".into(),
        ));
    }
    let mut accounting = AtomErasureAccounting::default();
    for (uuid, atom) in atoms_to_delete.iter().zip(atoms) {
        let Some(atom) = atom else { continue };
        let source_schema = atom.source_schema_name().to_string();
        if live_head_atoms.contains(uuid) {
            let logical = db_ops
                .atoms()
                .keep_small()
                .live_atom_bytes(uuid)
                .unwrap_or_else(|| serde_json::to_vec(&atom).map_or(0, |v| v.len() as u64));
            accounting
                .live_head_debits
                .push((source_schema.clone(), uuid.clone(), logical));
        }
        if !crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata()).is_empty()
        {
            accounting.file_pointer_atoms_purged =
                accounting.file_pointer_atoms_purged.saturating_add(1);
        }
        accounting
            .schema_markers
            .push((source_schema, uuid.clone()));
    }
    acct.record(
        crate::request_phases::RequestPhase::PurgeDelete,
        started.elapsed(),
    );
    Ok(accounting)
}

/// Main atom storage owns bodies and locators. Schema markers belong to their
/// separate namespace and never enter this batch.
pub(super) async fn push_atom_storage_keys(
    db_ops: &Arc<DbOperations>,
    storage_prefix: Option<&str>,
    atoms_to_delete: &[String],
    keys_to_delete: &mut Vec<Vec<u8>>,
) -> Result<(), SchemaError> {
    let atom_key_encoding = db_ops.atoms().atom_key_encoding();
    for uuid in atoms_to_delete {
        keys_to_delete.push(
            build_storage_key(storage_prefix, &crate::atom::atom_key_codec::flat_key(uuid))
                .into_bytes(),
        );
        if atom_key_encoding.writes_partition_prefix() {
            if let Some(partition) = db_ops
                .atoms()
                .lookup_atom_partition(uuid, storage_prefix)
                .await?
            {
                let key = crate::atom::atom_key_codec::storage_key(
                    crate::atom::AtomKeyEncoding::PartitionPrefix,
                    Some(&partition),
                    uuid,
                );
                keys_to_delete.push(build_storage_key(storage_prefix, &key).into_bytes());
            }
        }
        keys_to_delete.push(
            build_storage_key(
                storage_prefix,
                &crate::atom::atom_locator_codec::locator_key(uuid),
            )
            .into_bytes(),
        );
    }
    Ok(())
}
