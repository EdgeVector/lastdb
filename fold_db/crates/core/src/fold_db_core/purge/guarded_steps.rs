//! Helper steps of the guarded purge; see `guarded.rs`.

use super::guarded_prepare::GuardedRemovalInputs;
use super::*;

/// Live-head atoms, captured before `remove_key_from_field` drops them.
/// Keep-small only charges the current tip body; superseded chain atoms
/// already left the live gauge on replace_live_atom.
pub(super) fn collect_live_head_atoms(
    schema: &crate::schema::types::Schema,
    storage_slots: &[PlannedStorageSlot],
) -> HashSet<String> {
    let mut live_head_atoms: HashSet<String> = HashSet::new();
    for slot in storage_slots {
        let Some(field) = schema.runtime_fields.get(&slot.field_name) else {
            continue;
        };
        if let Some(uuid) = current_atom_for_key(field, &slot.storage_key) {
            live_head_atoms.insert(uuid);
        }
    }
    live_head_atoms
}

/// Which exact slots each field actually gave up, in storage form. Recorded
/// here rather than derived later because `remove_key_from_field`'s return
/// value is the only witness that a slot was present at all, and the
/// persistence must address exactly the slots that were removed -- no more (it
/// would erase a live sibling) and no fewer (it would leave a row naming a
/// purged record). `plan_label` names the plan in the missing-field error.
pub(super) fn remove_slots_from_schema(
    schema: &mut crate::schema::types::Schema,
    schema_name: &str,
    storage_slots: &[PlannedStorageSlot],
    plan_label: &str,
) -> Result<std::collections::HashMap<String, Vec<(String, String)>>, SchemaError> {
    let mut removed_by_field: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for slot in storage_slots {
        let field = schema
            .runtime_fields
            .get_mut(&slot.field_name)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "{plan_label} field '{}' disappeared from schema '{schema_name}'",
                    slot.field_name
                ))
            })?;
        if remove_key_from_field(field, &slot.storage_key) {
            removed_by_field
                .entry(slot.field_name.clone())
                .or_default()
                .push((slot.disk_hash.clone(), slot.disk_range.clone()));
        }
    }
    Ok(removed_by_field)
}

/// Candidate atoms no live head, retained history, or retained chain still
/// references (GuardedComplement reachability only).
pub(super) fn unreferenced_candidate_atoms(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::types::Schema,
    candidate_atoms: &HashSet<String>,
    history_referenced_by_retained: &HashSet<String>,
    chain_referenced_by_retained: &HashSet<String>,
    acct: &mut PurgeCommitAccounting,
) -> Vec<String> {
    let live_head_start = std::time::Instant::now();
    let mut still_referenced = collect_live_atom_uuids(schema);
    still_referenced.extend(collect_resident_live_atom_uuids(db_ops, schema));
    acct.record(
        crate::request_phases::RequestPhase::PurgeRetentionGuard,
        live_head_start.elapsed(),
    );
    candidate_atoms
        .iter()
        .filter(|uuid| {
            !still_referenced.contains(*uuid)
                && !history_referenced_by_retained.contains(*uuid)
                && !chain_referenced_by_retained.contains(*uuid)
        })
        .cloned()
        .collect()
}

/// Snapshot each touched field's molecule data with the slots it gave up.
pub(super) fn build_owned_molecule_removals(
    schema: &crate::schema::types::Schema,
    removed_by_field: &std::collections::HashMap<String, Vec<(String, String)>>,
) -> Vec<OwnedMoleculeKeyRemoval> {
    let mut owned_for_batch: Vec<OwnedMoleculeKeyRemoval> =
        Vec::with_capacity(removed_by_field.len());
    for (field_name, removed_slots) in removed_by_field {
        let field = schema
            .runtime_fields
            .get(field_name)
            .expect("removed_by_field was sourced from runtime_fields above");
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        let Some(data) = field.clone_molecule_data() else {
            continue;
        };
        owned_for_batch.push((
            molecule_uuid,
            data,
            removed_slots.clone(),
            field.common().storage_prefix().map(str::to_string),
        ));
    }
    owned_for_batch
}

/// Storage keys of the tips that the owned removals delete.
pub(super) fn removed_tip_keys_of(owned_for_batch: &[OwnedMoleculeKeyRemoval]) -> Vec<String> {
    let mut removed_tip_keys: Vec<String> = Vec::new();
    for (molecule_uuid, _, removed_slots, storage_prefix) in owned_for_batch {
        for (hash, range) in removed_slots {
            removed_tip_keys.push(build_storage_key(
                storage_prefix.as_deref(),
                &crate::atom::molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range),
            ));
        }
    }
    removed_tip_keys
}

/// Batched molecule-key removal, plus derived keys where the reachability
/// mode or compact edges require them.
pub(super) async fn persist_molecule_removals(
    db_ops: &Arc<DbOperations>,
    owned_for_batch: &[OwnedMoleculeKeyRemoval],
    inputs: &GuardedRemovalInputs<'_>,
    tip_version_backref_keys: &[Vec<u8>],
) -> Result<(), SchemaError> {
    let GuardedRemovalInputs {
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
        history_keys,
        tip_version_keys,
        reachability,
        ..
    } = *inputs;
    if owned_for_batch.is_empty() {
        return Ok(());
    }
    let batch_refs: Vec<_> = owned_for_batch
        .iter()
        .map(|(uuid, data, slots, prefix)| {
            (uuid.as_str(), data, slots.as_slice(), prefix.as_deref())
        })
        .collect();
    if reachability == PurgeReachability::AtomRefEdges || !atom_ref_v2_edge_keys.is_empty() {
        let mut derived_keys: Vec<Vec<u8>> = if reachability == PurgeReachability::AtomRefEdges {
            atom_ref_edge_keys
                .iter()
                .chain(history_keys)
                .chain(tip_version_keys)
                .map(|key| key.as_bytes().to_vec())
                .collect()
        } else {
            Vec::new()
        };
        if reachability == PurgeReachability::AtomRefEdges {
            derived_keys.extend(tip_version_backref_keys.iter().cloned());
        }
        let compact_edge_keys = atom_ref_v2_edge_keys
            .iter()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        db_ops
            .atoms()
            .remove_molecules_keys_with_extra_and_trailing_batch(
                &batch_refs,
                derived_keys,
                compact_edge_keys,
            )
            .await?;
    } else {
        db_ops
            .atoms()
            .remove_molecules_keys_batch(&batch_refs)
            .await?;
    }
    Ok(())
}

/// Disk reverse edges lag T0. A concurrent ack of a shared atom has no durable
/// edge yet; GuardedComplement already unions the resident set. Appends the
/// unreferenced candidates to `atoms_to_delete` and returns the read count.
pub(super) async fn collect_unreferenced_by_reverse_edges(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::types::Schema,
    candidate_atoms: &HashSet<String>,
    atoms_to_delete: &mut Vec<String>,
) -> Result<u64, SchemaError> {
    let mut reverse_edge_reads = 0u64;
    let resident_live = collect_resident_live_atom_uuids(db_ops, schema);
    let prefixes = helpers::schema_storage_prefixes(schema);
    for atom_uuid in candidate_atoms {
        if helpers::atom_ref_edges_keep_resident_live(atom_uuid, &resident_live) {
            continue;
        }
        let mut referenced = false;
        for prefix in &prefixes {
            reverse_edge_reads = reverse_edge_reads.saturating_add(1);
            if db_ops
                .atoms()
                .has_any_active_atom_refs(atom_uuid, prefix.as_deref())
                .await?
            {
                referenced = true;
                break;
            }
        }
        if !referenced {
            atoms_to_delete.push(atom_uuid.clone());
        }
    }
    Ok(reverse_edge_reads)
}

/// File-blob accounting happens after reverse-edge decisions and before atom
/// deletion. A still-referenced candidate never contributes a debit.
/// Returns `(file_pointer_atoms_purged, live_head_debits)`.
pub(super) async fn account_blob_and_live_head_debits(
    db_ops: &Arc<DbOperations>,
    atoms_to_delete: &[String],
    live_head_atoms: &HashSet<String>,
    acct: &mut PurgeCommitAccounting,
) -> Result<(u64, Vec<(String, String, u64)>), SchemaError> {
    let mut file_pointer_atoms_purged: u64 = 0;
    let mut live_head_debits: Vec<(String, String, u64)> = Vec::new();
    let blob_accounting_start = std::time::Instant::now();
    for uuid in atoms_to_delete {
        let Some(atom) = helpers::atom_load_for_hard_erase(
            db_ops.atoms().get_atom_by_uuid(uuid, None).await,
            uuid,
        )?
        else {
            continue;
        };
        if live_head_atoms.contains(uuid) {
            let logical = db_ops
                .atoms()
                .keep_small()
                .live_atom_bytes(uuid)
                .unwrap_or_else(|| serde_json::to_vec(&atom).map_or(0, |v| v.len() as u64));
            live_head_debits.push((atom.source_schema_name().to_string(), uuid.clone(), logical));
        }
        let refs = crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata());
        if !refs.is_empty() {
            file_pointer_atoms_purged = file_pointer_atoms_purged.saturating_add(1);
        }
    }
    acct.record(
        crate::request_phases::RequestPhase::PurgeDelete,
        blob_accounting_start.elapsed(),
    );
    Ok((file_pointer_atoms_purged, live_head_debits))
}

/// Append the per-atom storage keys (flat, partition-prefix, locator, schema
/// index) to `keys_to_delete`.
pub(super) async fn push_atom_storage_keys(
    db_ops: &Arc<DbOperations>,
    schema_name: &str,
    atoms_to_delete: &[String],
    keys_to_delete: &mut Vec<Vec<u8>>,
) -> Result<(), SchemaError> {
    let atom_key_encoding = db_ops.atoms().atom_key_encoding();
    for uuid in atoms_to_delete {
        keys_to_delete.push(
            build_storage_key(None, &crate::atom::atom_key_codec::flat_key(uuid)).into_bytes(),
        );
        if atom_key_encoding.writes_partition_prefix() {
            if let Some(partition) = db_ops.atoms().lookup_atom_partition(uuid, None).await? {
                let key = crate::atom::atom_key_codec::storage_key(
                    crate::atom::AtomKeyEncoding::PartitionPrefix,
                    Some(&partition),
                    uuid,
                );
                keys_to_delete.push(build_storage_key(None, &key).into_bytes());
            }
        }
        keys_to_delete.push(
            build_storage_key(None, &crate::atom::atom_locator_codec::locator_key(uuid))
                .into_bytes(),
        );
        keys_to_delete.push(
            crate::db_operations::atom_store::AtomStore::schema_index_key(None, schema_name, uuid)
                .into_bytes(),
        );
    }
    Ok(())
}

/// Evict the purged tips and atoms from the resident set.
pub(super) fn evict_resident_state(
    db_ops: &Arc<DbOperations>,
    storage_slots: &[PlannedStorageSlot],
    resident_api_keys: &std::collections::HashMap<StorageSlotIdentity, ResidentApiKey>,
    atoms_to_delete: &[String],
) {
    let resident = db_ops.resident();
    for slot in storage_slots {
        if let Some(resident_key) = slot.resident_key.as_ref() {
            resident.purge_tip(
                &slot.molecule_uuid,
                resident_key.hash.as_deref().unwrap_or_default(),
                resident_key.range.as_deref().unwrap_or_default(),
            );
        } else if let Some((api_hash, api_range)) = resident_api_keys.get(&slot.identity()) {
            resident.purge_tip(&slot.molecule_uuid, api_hash, api_range);
        }
    }
    for uuid in atoms_to_delete {
        resident.purge_atom(uuid);
    }
}

/// Store, reload, and flush the schema after the destructive writes.
pub(super) async fn finalize_schema_after_purge(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mut schema: crate::schema::types::Schema,
    acct: &mut PurgeCommitAccounting,
) -> Result<(), SchemaError> {
    let finalize_start = std::time::Instant::now();
    schema.sync_molecule_uuids();
    let stored = db_ops.store_schema(schema_name, &schema).await;
    let reloaded = match stored {
        Ok(()) => schema_manager.load_schema_internal(schema).await,
        Err(error) => Err(error),
    };
    let flushed = match reloaded {
        Ok(()) => db_ops.flush().await.map_err(|error| {
            SchemaError::InvalidData(format!("Flush failed after bulk purge: {error}"))
        }),
        Err(error) => Err(error),
    };
    // Attributed BEFORE `?`, for the same reason `purge_commit` is: a
    // finalize that errors still ran inside the guarded critical section.
    acct.record(
        crate::request_phases::RequestPhase::PurgeFinalize,
        finalize_start.elapsed(),
    );
    flushed
}

/// Build the audit-ledger entry for a guarded erasure.
pub(super) fn build_erasure_ledger_entry(
    (verb, caller): (HardEraseVerb, &str),
    schema_name: &str,
    ledger_descriptor: &str,
    counts: &BulkPurgeReport,
    storage_keys_deleted: usize,
    file_pointer_atoms_purged: u64,
) -> crate::db_operations::AtomDeleteLedgerEntry {
    let mut ledger_entry = crate::db_operations::AtomDeleteLedgerEntry::erasure(
        verb.ledger_verb(),
        caller,
        schema_name,
        ledger_descriptor,
    );
    ledger_entry.atoms_deleted = counts.atom_rows_deleted as u64;
    ledger_entry.history_rows_deleted = counts.history_rows_deleted as u64;
    ledger_entry.tip_versions_pruned = counts.tip_versions_pruned as u64;
    ledger_entry.storage_keys_deleted = storage_keys_deleted as u64;
    ledger_entry.file_pointer_atoms_purged = file_pointer_atoms_purged;
    ledger_entry
}

/// Batch-delete the collected keys from the main store.
pub(super) async fn batch_delete_keys(
    db_ops: &Arc<DbOperations>,
    keys_to_delete: Vec<Vec<u8>>,
    acct: &mut PurgeCommitAccounting,
) -> Result<(), SchemaError> {
    if keys_to_delete.is_empty() {
        return Ok(());
    }
    let main_store = db_ops.atoms().raw().inner().clone();
    let batch_delete_start = std::time::Instant::now();
    let deleted = main_store.batch_delete(keys_to_delete).await;
    acct.record(
        crate::request_phases::RequestPhase::PurgeDelete,
        batch_delete_start.elapsed(),
    );
    deleted.map_err(|error| {
        SchemaError::InvalidData(format!("Failed to batch_delete during bulk purge: {error}"))
    })
}
