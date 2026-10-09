//! Moved verbatim out of the parent module; see the parent for context.
// lint:file-size-ok verbatim move; the one oversized function is separate split work

use super::*;

/// The single destructive implementation shared by logical API-key purge and
/// exact storage-slot purge. Everything above this boundary is target planning;
/// everything below it is reachability-guarded erasure.
#[allow(clippy::too_many_arguments)]
// lint:fn-size-ok verbatim move from purge/mod.rs; splitting this function is separate work
pub(super) async fn execute_guarded_purge(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mut schema: crate::schema::types::Schema,
    storage_slots: &[PlannedStorageSlot],
    candidate_atoms: HashSet<String>,
    history_keys: Vec<String>,
    tip_version_keys: Vec<String>,
    atom_ref_edge_keys: Vec<String>,
    atom_ref_v2_edge_keys: Vec<String>,
    history_referenced_by_retained: HashSet<String>,
    records_purged: usize,
    ledger_descriptor: &str,
    verb: HardEraseVerb,
    acct: &mut PurgeCommitAccounting,
    evict_resident: bool,
    reachability: PurgeReachability,
    precomputed_retained: Option<HashSet<String>>,
) -> Result<BulkPurgeReport, SchemaError> {
    let target_slot_ids: HashSet<StorageSlotIdentity> = storage_slots
        .iter()
        .map(PlannedStorageSlot::identity)
        .collect();
    // Resolve resident aliases before the first destructive write. Encoding
    // failure must refuse the purge while every durable row is still intact.
    let resident_api_keys = resident_api_keys_for_storage_slots(db_ops.as_ref(), &target_slot_ids)?;
    let candidate_atom_count = u64::try_from(candidate_atoms.len()).unwrap_or(u64::MAX);
    let chain_referenced_by_retained = match reachability {
        PurgeReachability::GuardedComplement => {
            if let Some(retained) = precomputed_retained {
                retained
            } else {
                let guard_start = std::time::Instant::now();
                let retained = collect_retained_chain_atoms_for_storage_slots(
                    db_ops,
                    &schema,
                    &candidate_atoms,
                    &target_slot_ids,
                )
                .await;
                acct.record(
                    crate::request_phases::RequestPhase::PurgeRetentionGuard,
                    guard_start.elapsed(),
                );
                retained?
            }
        }
        PurgeReachability::AtomRefEdges => HashSet::new(),
    };

    // Live-head atoms, captured before `remove_key_from_field` drops them.
    // Keep-small only charges the current tip body; superseded chain atoms
    // already left the live gauge on replace_live_atom.
    let mut live_head_atoms: HashSet<String> = HashSet::new();
    for slot in storage_slots {
        let Some(field) = schema.runtime_fields.get(&slot.field_name) else {
            continue;
        };
        if let Some(uuid) = current_atom_for_key(field, &slot.storage_key) {
            live_head_atoms.insert(uuid);
        }
    }

    // Which exact slots each field actually gave up, in storage form. Recorded
    // here rather than derived later because `remove_key_from_field`'s return
    // value is the only witness that a slot was present at all, and the
    // persistence below must address exactly the slots that were removed —
    // no more (it would erase a live sibling) and no fewer (it would leave a
    // row naming a purged record).
    let mut removed_by_field: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for slot in storage_slots {
        let field = schema
            .runtime_fields
            .get_mut(&slot.field_name)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Purge plan field '{}' disappeared from schema '{schema_name}'",
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

    let mut atoms_to_delete: Vec<String> = if reachability == PurgeReachability::GuardedComplement {
        let live_head_start = std::time::Instant::now();
        let mut still_referenced = collect_live_atom_uuids(&schema);
        still_referenced.extend(collect_resident_live_atom_uuids(db_ops, &schema));
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
    } else {
        Vec::new()
    };

    // Persist each field's removal by deleting exactly the purged slots' rows.
    //
    // This used to delete the whole molecule and rewrite it. That made the cost
    // of purging ONE record O(cardinality of every field molecule it touched):
    // measured on the primary 2026-08-09, a single-record purge of a 24-field
    // schema holding ~19.2k keys per field molecule rewrote ~347.7k `mk:` rows
    // and grew the `tips` plane ~442 MiB, with the store flat whenever no purge
    // was running. Purge is the only primitive that can shrink a plane, so the
    // reclaim path was the growth path and an operator answering disk pressure
    // by purging harder made it worse. See
    // `AtomStore::remove_molecule_keys` for what it does and does not touch.
    let remove_keys_start = std::time::Instant::now();
    //
    // The per-field path still paid one durable delete + one header put per
    // field (×24 on BoardCards) under the exclusive purge barrier. Batch them
    // into a single store round-trip pair via `remove_molecules_keys_batch`.
    let mut owned_for_batch: Vec<OwnedMoleculeKeyRemoval> =
        Vec::with_capacity(removed_by_field.len());
    for (field_name, removed_slots) in &removed_by_field {
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
    let mut removed_tip_keys: Vec<String> = Vec::new();
    for (molecule_uuid, _, removed_slots, storage_prefix) in &owned_for_batch {
        for (hash, range) in removed_slots {
            removed_tip_keys.push(build_storage_key(
                storage_prefix.as_deref(),
                &crate::atom::molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range),
            ));
        }
    }
    let tip_debits = db_ops
        .atoms()
        .keep_small_measure_tip_keys(&removed_tip_keys)
        .await;

    let tip_version_backref_keys = db_ops
        .atoms()
        .tip_version_backref_delete_keys_for_tv_keys(&tip_version_keys)
        .await?;

    if !owned_for_batch.is_empty() {
        let batch_refs: Vec<_> = owned_for_batch
            .iter()
            .map(|(uuid, data, slots, prefix)| {
                (uuid.as_str(), data, slots.as_slice(), prefix.as_deref())
            })
            .collect();
        if reachability == PurgeReachability::AtomRefEdges || !atom_ref_v2_edge_keys.is_empty() {
            let mut derived_keys: Vec<Vec<u8>> = if reachability == PurgeReachability::AtomRefEdges
            {
                atom_ref_edge_keys
                    .iter()
                    .chain(&history_keys)
                    .chain(&tip_version_keys)
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
    }

    acct.record(
        crate::request_phases::RequestPhase::PurgeDelete,
        remove_keys_start.elapsed(),
    );

    let main_store = db_ops.atoms().raw().inner().clone();
    let history_rows_deleted = history_keys.len();
    let tip_versions_pruned = tip_version_keys.len();
    let mut reverse_edge_reads = 0u64;
    if reachability == PurgeReachability::AtomRefEdges {
        // Disk reverse edges lag T0. A concurrent ack of a shared atom has
        // no durable edge yet; GuardedComplement already unions this set.
        let resident_live = collect_resident_live_atom_uuids(db_ops, &schema);
        let prefixes = helpers::schema_storage_prefixes(&schema);
        for atom_uuid in &candidate_atoms {
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
    }

    // File-blob accounting happens after reverse-edge decisions and before
    // atom deletion. A still-referenced candidate never contributes a debit.
    let mut file_pointer_atoms_purged: u64 = 0;
    let mut live_head_debits: Vec<(String, String, u64)> = Vec::new();
    let blob_accounting_start = std::time::Instant::now();
    for uuid in &atoms_to_delete {
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

    let mut keys_to_delete: Vec<Vec<u8>> = if reachability == PurgeReachability::GuardedComplement {
        let mut keys: Vec<Vec<u8>> = history_keys
            .iter()
            .chain(&tip_version_keys)
            .map(|key| key.as_bytes().to_vec())
            .collect();
        keys.extend(tip_version_backref_keys);
        keys
    } else {
        Vec::new()
    };
    let atom_rows_deleted = atoms_to_delete.len();
    let atom_key_encoding = db_ops.atoms().atom_key_encoding();
    let key_set_start = std::time::Instant::now();
    for uuid in &atoms_to_delete {
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

    acct.record(
        crate::request_phases::RequestPhase::PurgeDelete,
        key_set_start.elapsed(),
    );

    let mut ledger_entry = crate::db_operations::AtomDeleteLedgerEntry::erasure(
        verb.ledger_verb(),
        "mutation-pipeline",
        schema_name,
        ledger_descriptor,
    );
    ledger_entry.atoms_deleted = atom_rows_deleted as u64;
    ledger_entry.history_rows_deleted = history_rows_deleted as u64;
    ledger_entry.tip_versions_pruned = tip_versions_pruned as u64;
    ledger_entry.storage_keys_deleted = keys_to_delete.len() as u64;
    ledger_entry.file_pointer_atoms_purged = file_pointer_atoms_purged;
    let ledger_handle = db_ops
        .atoms()
        .begin_delete_ledger_row(None, ledger_entry)
        .await?;
    // Resolve the debit once. `store_schema` below rebinds shared molecules,
    // so a second resolution at commit could name a different schema.
    let meter_plan =
        db_ops
            .atoms()
            .plan_keep_small_hard_erase(schema_name, &live_head_debits, &tip_debits)?;
    db_ops
        .atoms()
        .prepare_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;

    if !keys_to_delete.is_empty() {
        let batch_delete_start = std::time::Instant::now();
        let deleted = main_store.batch_delete(keys_to_delete).await;
        acct.record(
            crate::request_phases::RequestPhase::PurgeDelete,
            batch_delete_start.elapsed(),
        );
        deleted.map_err(|error| {
            SchemaError::InvalidData(format!("Failed to batch_delete during bulk purge: {error}"))
        })?;
    }

    if evict_resident {
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
        for uuid in &atoms_to_delete {
            resident.purge_atom(uuid);
        }
    }

    let finalize_start = std::time::Instant::now();
    schema.sync_molecule_uuids();
    let stored = db_ops.store_schema(schema_name, &schema).await;
    let reloaded = match stored {
        Ok(()) => schema_manager.load_schema_internal(schema).await,
        Err(error) => Err(error),
    };
    let embedding_rows_deleted = records_purged;
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
    flushed?;
    // The destructive flush precedes the durable meter debit. A crash in
    // this short gap leaves meter trust incomplete rather than a false debit.
    db_ops
        .atoms()
        .commit_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;
    db_ops
        .atoms()
        .commit_delete_ledger_row(ledger_handle, |entry| {
            entry.embedding_rows_deleted = embedding_rows_deleted as u64;
        })
        .await;

    tracing::info!(
        schema = %schema_name,
        records_purged,
        history_rows_deleted,
        tip_versions_pruned,
        atom_rows_deleted,
        embedding_rows_deleted,
        "purge core: hard-removed target batch"
    );

    Ok(BulkPurgeReport {
        records_purged,
        history_rows_deleted,
        tip_versions_pruned,
        atom_rows_deleted,
        embedding_rows_deleted,
        target_slots: u64::try_from(storage_slots.len()).unwrap_or(u64::MAX),
        candidate_atoms: candidate_atom_count,
        reverse_edge_reads,
    })
}
