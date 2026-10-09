//! Moved verbatim out of the parent module; see the parent for context.

use super::*;

// lint:fn-size-ok verbatim move from purge/mod.rs; splitting this function is separate work
pub(in crate::fold_db_core) async fn converge_delete_tips(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    mutations: &[crate::schema::types::Mutation],
    storage_prefix: Option<&str>,
) -> Result<BulkPurgeReport, SchemaError> {
    if mutations.is_empty() {
        return Ok(BulkPurgeReport::empty());
    }

    let mut schema = schema_manager
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' not found for delete converge"
            ))
        })?;
    crate::fold_db_core::mutation_manager::helpers::apply_storage_prefix_to_schema(
        &mut schema,
        storage_prefix,
    );

    let keys: Vec<KeyValue> = mutations
        .iter()
        .map(|mutation| mutation.key_value.clone())
        .collect();
    for key in &keys {
        validate_purge_key_shape(&schema, schema_name, key)?;
    }

    let planned_barriers = plan_normal_delete_barriers(db_ops, &schema, mutations)?;
    let tip_keys: Vec<String> = planned_barriers
        .iter()
        .map(|barrier| barrier.mk_key.clone())
        .collect();
    let tip_guards = db_ops.atoms().lock_tip_commits(&tip_keys).await;
    // The memory Delete registered its pending barrier before this lane ran.
    // The durable lock covers barrier flush and source removal. A newer Put
    // can publish in memory while its durable batch waits for this lock.
    let mut removable_tip_keys = HashSet::new();
    let mut barrier_items = Vec::new();
    let targets = db_ops
        .atoms()
        .delete_converge_targets(&planned_barriers)
        .await?;
    for (mut barrier, tip) in targets {
        if let Some(tip) = &tip {
            barrier.displaced_atom_uuid = Some(tip.entry.atom_uuid.clone());
            if barrier.blocks_tip(&tip.entry) {
                removable_tip_keys.insert(barrier.mk_key.clone());
            }
        }
        let barrier_key =
            crate::atom::delete_barrier::delete_barrier_key(barrier.mk_key.as_bytes());
        let value = serde_json::to_value(&barrier).map_err(|error| {
            SchemaError::InvalidData(format!("serialize Delete barrier: {error}"))
        })?;
        barrier_items.push((barrier_key, value));
    }
    delete_barrier_flush::flush_delete_barriers(db_ops, barrier_items).await?;
    if removable_tip_keys.is_empty() {
        db_ops
            .atoms()
            .clear_pending_delete_barriers(&planned_barriers);
        drop(tip_guards);
        return Ok(BulkPurgeReport::empty());
    }

    // Same dedup rationale as `purge_records_bulk`: a batch is a SET of
    // records, and a repeated key must not trip a stale count.
    let mut seen: HashSet<String> = HashSet::new();
    let deduped: Vec<KeyValue> = keys
        .iter()
        .filter(|k| seen.insert(describe_key(k)))
        .cloned()
        .collect();
    let keys: &[KeyValue] = &deduped;

    // Keyed, per-touched-key molecule load — never the O(field size) full
    // scan. See `refresh_runtime_field_molecules_for_purge_keys`.
    let step_started = std::time::Instant::now();
    refresh_runtime_field_molecules_for_purge_keys(db_ops, &mut schema, keys).await?;
    let refresh_elapsed = step_started.elapsed();

    // This is not a compliance purge. collect_purge_trace reads a retired
    // history: prefix for every field. A cold empty prefix still touches every
    // storage group. Live Delete needs only the named tips and their own
    // archived tv: chains, not work proportional to the whole home.
    let step_started = std::time::Instant::now();
    let mut traced = HashSet::new();
    let mut tip_version_keys = Vec::new();
    let mut atom_ref_edge_keys = Vec::new();
    let mut atom_ref_v2_edge_keys = Vec::new();
    for field in schema.runtime_fields.values() {
        if field.common().molecule_uuid().is_none() {
            continue;
        }
        for (idx, key) in storage_form_keys(db_ops, field, keys)?.iter().enumerate() {
            let Some((hash, range)) = field.disk_slot_for_key(key) else {
                continue;
            };
            let mk_key = build_storage_key(
                field.common().storage_prefix(),
                &crate::atom::molecule_key_codec::hash_range_record_key(
                    field
                        .common()
                        .molecule_uuid()
                        .expect("checked molecule uuid"),
                    &hash,
                    &range,
                ),
            );
            if !removable_tip_keys.contains(&mk_key) {
                continue;
            }
            if current_atom_for_key(field, key).is_some() {
                traced.insert(idx);
            }
            let trace = collect_target_tip_chain(db_ops, field, key).await?;
            tip_version_keys.extend(trace.tip_version_keys);
            atom_ref_edge_keys.extend(trace.atom_ref_edge_keys);
            atom_ref_v2_edge_keys.extend(trace.atom_ref_v2_edge_keys);
        }
    }
    let trace_elapsed = step_started.elapsed();

    let keys_present: Vec<KeyValue> = keys
        .iter()
        .enumerate()
        .filter(|(idx, _)| traced.contains(idx))
        .map(|(_, k)| k.clone())
        .collect();
    if keys_present.is_empty() {
        db_ops
            .atoms()
            .clear_pending_delete_barriers(&planned_barriers);
        drop(tip_guards);
        return Ok(BulkPurgeReport::empty());
    }
    let keys: &[KeyValue] = &keys_present;

    let mut storage_slots = Vec::new();
    for (field_name, field) in &schema.runtime_fields {
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        for (api_key, storage_key) in keys.iter().zip(storage_form_keys(db_ops, field, keys)?) {
            let Some((disk_hash, disk_range)) = field.disk_slot_for_key(&storage_key) else {
                continue;
            };
            let mk_key = build_storage_key(
                field.common().storage_prefix(),
                &crate::atom::molecule_key_codec::hash_range_record_key(
                    &molecule_uuid,
                    &disk_hash,
                    &disk_range,
                ),
            );
            if !removable_tip_keys.contains(&mk_key) {
                continue;
            }
            storage_slots.push(PlannedStorageSlot {
                field_name: field_name.clone(),
                molecule_uuid: molecule_uuid.clone(),
                storage_key,
                resident_key: Some(api_key.clone()),
                disk_hash,
                disk_range,
            });
        }
    }

    // Same shape the inline path fingerprints: one key by name, a batch by
    // size. Computed on `keys_present`, so the row describes what is actually
    // being removed rather than what was asked for.
    let ledger_descriptor = if keys.len() == 1 {
        describe_key(&keys[0])
    } else {
        format!("<bulk:{} keys>", keys.len())
    };

    let mut removed_by_field: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for slot in &storage_slots {
        let field = schema
            .runtime_fields
            .get_mut(&slot.field_name)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Delete converge plan field '{}' disappeared from schema '{schema_name}'",
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

    if removed_by_field.is_empty() {
        db_ops
            .atoms()
            .clear_pending_delete_barriers(&planned_barriers);
        drop(tip_guards);
        return Ok(BulkPurgeReport::empty());
    }

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
    let measure_elapsed = step_started.elapsed();
    let tip_version_backref_keys = db_ops
        .atoms()
        .tip_version_backref_delete_keys_for_tv_keys(&tip_version_keys)
        .await?;

    let batch_refs: Vec<_> = owned_for_batch
        .iter()
        .map(|(uuid, data, slots, prefix)| {
            (uuid.as_str(), data, slots.as_slice(), prefix.as_deref())
        })
        .collect();
    let mut extra_keys: Vec<Vec<u8>> = tip_version_keys
        .iter()
        .map(|key| key.as_bytes().to_vec())
        .collect();
    extra_keys.extend(tip_version_backref_keys);
    let trailing_keys: Vec<Vec<u8>> = atom_ref_edge_keys
        .iter()
        .chain(&atom_ref_v2_edge_keys)
        .map(|key| key.as_bytes().to_vec())
        .collect();

    // Write-ahead audit row, before anything is destroyed. Converge reclaims
    // no atom body, so `atoms_deleted` stays 0 and the verb is
    // `delete-converge` rather than `delete` — see the `delete_ledger` module
    // docs for why a row that deletes nothing still has to exist. Without it
    // the ledger answers "no deletes in this window" for every window on a
    // node whose live delete path is this function, which is the shape that
    // let a dangling-tip investigation read a dead instrument as evidence.
    let step_started = std::time::Instant::now();
    let mut ledger_entry = crate::db_operations::AtomDeleteLedgerEntry::delete_converge(
        "mutation-pipeline",
        schema_name,
        &ledger_descriptor,
    );
    ledger_entry.tips_removed = removed_tip_keys.len() as u64;
    ledger_entry.records_converged = keys.len() as u64;
    ledger_entry.tip_versions_pruned = tip_version_keys.len() as u64;
    ledger_entry.storage_keys_deleted =
        (removed_tip_keys.len() + extra_keys.len() + trailing_keys.len()) as u64;
    let ledger_started = std::time::Instant::now();
    let ledger_handle = db_ops
        .atoms()
        .begin_delete_ledger_row(None, ledger_entry)
        .await?;
    // Resolve the debit once. `store_schema` below rebinds shared molecules,
    // so a second resolution at commit could name a different schema.
    let meter_plan = db_ops
        .atoms()
        .plan_keep_small_hard_erase(schema_name, &[], &tip_debits)?;
    db_ops
        .atoms()
        .prepare_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;
    let ledger_elapsed = ledger_started.elapsed();

    let batch_started = std::time::Instant::now();
    db_ops
        .atoms()
        .remove_molecules_keys_with_extra_and_trailing_batch(&batch_refs, extra_keys, trailing_keys)
        .await?;
    let batch_elapsed = batch_started.elapsed();

    let remove_elapsed = step_started.elapsed();

    let step_started = std::time::Instant::now();
    let flushed = db_ops.flush().await.map_err(|error| {
        SchemaError::InvalidData(format!("Flush failed after delete converge: {error}"))
    });
    let flush_elapsed = step_started.elapsed();
    flushed?;
    db_ops
        .atoms()
        .clear_pending_delete_barriers(&planned_barriers);
    drop(tip_guards);

    // Schema metadata can take molecule locks. Release every exact tip lock
    // after source removal reaches disk, then refresh the schema cache.
    let step_started = std::time::Instant::now();
    crate::fold_db_core::mutation_manager::helpers::clear_storage_prefix_on_schema(&mut schema);
    schema.sync_molecule_uuids();
    let schema_unchanged = matches!(
        db_ops.schemas().get_schema(schema_name).await,
        Ok(Some(ref old)) if *old == schema
    );
    if !schema_unchanged {
        db_ops.store_schema(schema_name, &schema).await?;
        db_ops.flush().await.map_err(|error| {
            SchemaError::InvalidData(format!("flush schema after delete converge: {error}"))
        })?;
    }
    schema_manager.load_schema_internal(schema).await?;
    let schema_elapsed = step_started.elapsed();
    // Flush the deletion first. A debit that survives a crash must never
    // describe rows whose deletion did not reach durable storage.
    db_ops
        .atoms()
        .commit_keep_small_hard_erase_plan(ledger_handle.key(), &meter_plan)
        .await?;
    // Confirm only after both the delete and its small meter debit are durable.
    db_ops
        .atoms()
        .commit_delete_ledger_row(ledger_handle, |_| {})
        .await;

    let tip_versions_pruned = tip_version_keys.len();
    tracing::info!(
        schema = %schema_name,
        records_converged = keys.len(),
        tip_versions_pruned,
        refresh_ms = refresh_elapsed.as_millis() as u64,
        trace_ms = trace_elapsed.as_millis() as u64,
        remove_ms = remove_elapsed.as_millis() as u64,
        measure_ms = measure_elapsed.as_millis() as u64,
        ledger_ms = ledger_elapsed.as_millis() as u64,
        batch_ms = batch_elapsed.as_millis() as u64,
        schema_ms = schema_elapsed.as_millis() as u64,
        schema_store_skipped = schema_unchanged,
        flush_ms = flush_elapsed.as_millis() as u64,
        "delete converge: tip absence persisted"
    );

    Ok(BulkPurgeReport {
        records_purged: keys.len(),
        history_rows_deleted: 0,
        tip_versions_pruned,
        atom_rows_deleted: 0,
        embedding_rows_deleted: 0,
        target_slots: u64::try_from(storage_slots.len()).unwrap_or(u64::MAX),
        candidate_atoms: 0,
        reverse_edge_reads: 0,
    })
}
