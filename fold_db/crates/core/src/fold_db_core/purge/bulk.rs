//! Scoped target planning for the explicit atom-erasure operation.

use super::*;

/// Remove the requested keys from one schema in one database scope.
///
/// Resolve the schema once, load the selected tips, and prove all required
/// targets exist before the destructive stage. The target trace and exact
/// encoded slots keep every later operation in the same scope.
///
/// Both reachability modes check current shared references after target tip
/// removal. A failed or incomplete final reference check prevents publication.
/// The guarded-complement mode can retain extra atoms when a complete reverse
/// reference proof is unavailable. Its caller holds the schema purge barrier.
pub(in crate::fold_db_core) async fn purge_records_bulk(
    scope: PurgeScope<'_>,
    keys: &[KeyValue],
    missing: PurgeMissingPolicy,
    verb: HardEraseVerb,
    acct: &mut PurgeCommitAccounting,
    mode: PurgeMode,
) -> Result<BulkPurgeReport, SchemaError> {
    let PurgeScope {
        db_ops,
        schema_name,
        ..
    } = scope;
    let reachability = mode.reachability;
    if keys.is_empty() {
        return Ok(BulkPurgeReport::empty());
    }

    let mut schema = scope.schema()?;

    for key in keys {
        validate_purge_key_shape(&schema, schema_name, key)?;
    }

    let deduped = dedup_purge_keys(keys);
    let keys: &[KeyValue] = &deduped;

    let materialize_start = std::time::Instant::now();
    let materialized = match reachability {
        PurgeReachability::GuardedComplement => {
            refresh_runtime_field_molecules(db_ops, &mut schema).await
        }
        PurgeReachability::AtomRefEdges => {
            refresh_runtime_field_molecules_for_purge_keys(db_ops, &mut schema, keys).await
        }
    };
    acct.record(
        crate::request_phases::RequestPhase::PurgeMaterialize,
        materialize_start.elapsed(),
    );
    materialized?;

    let trace_start = std::time::Instant::now();
    let traced_batch = collect_purge_trace(db_ops, &schema, keys).await;
    acct.record(
        crate::request_phases::RequestPhase::PurgeTrace,
        trace_start.elapsed(),
    );
    let PurgeTrace {
        candidate_atoms,
        history_keys,
        atom_ref_v2_edge_keys,
        history_referenced_by_retained,
        traced,
    } = traced_batch?;

    // Missing-target policy, BEFORE any mutation: see `filter_missing_targets`.
    let keys_present = filter_missing_targets(schema_name, keys, &traced, missing, verb)?;
    let keys: &[KeyValue] = match &keys_present {
        Some(present) if present.is_empty() => return Ok(BulkPurgeReport::empty()),
        Some(present) => present,
        None => keys,
    };

    let described_key = if keys.len() == 1 {
        describe_key(&keys[0])
    } else {
        format!("<bulk:{} keys>", keys.len())
    };
    let storage_slots = plan_storage_slots(db_ops, &schema, keys)?;

    let purge = GuardedPurge {
        candidate_atoms,
        history_keys,
        atom_ref_v2_edge_keys,
        history_referenced_by_retained,
        records_purged: keys.len(),
        mode,
    };
    execute_guarded_purge(
        scope,
        schema,
        &storage_slots,
        purge,
        &described_key,
        verb,
        acct,
    )
    .await
}

/// A batch is a SET of records.
/// The per-record path made a repeated key a loud error by accident — the
/// second purge found nothing where the first had just erased it — and that is
/// not a contract worth reconstructing at batch scale: the caller asked for the
/// record to be gone, and after the first removal it is. Deduping keeps
/// `records_purged` a count of distinct records and stops a repeated key from
/// tripping the not-found check against a target this same batch erased.
///
/// `describe_key` is the dedup token because it renders exactly the
/// `(hash, range)` pair that identifies a slot; on a Single schema every key
/// collapses to `<single>`, which is correct — there is only one record.
fn dedup_purge_keys(keys: &[KeyValue]) -> Vec<KeyValue> {
    let mut seen: HashSet<String> = HashSet::new();
    keys.iter()
        .filter(|k| seen.insert(describe_key(k)))
        .cloned()
        .collect()
}

/// Missing-target policy. Compliance (`Refuse`) keeps the loud contract from
/// `purge_record`. Delete (`Skip`) filters to the keys that left a trace so
/// re-delete is a no-op without weakening compliance purge. Candidate atoms /
/// history / tip-version keys were only collected for traced targets, so
/// filtering keys keeps those sets consistent with the destructive half of the
/// batch. Returns `None` when every key was traced.
fn filter_missing_targets(
    schema_name: &str,
    keys: &[KeyValue],
    traced: &HashSet<usize>,
    missing: PurgeMissingPolicy,
    verb: HardEraseVerb,
) -> Result<Option<Vec<KeyValue>>, SchemaError> {
    if traced.len() == keys.len() {
        return Ok(None);
    }
    match missing {
        PurgeMissingPolicy::Refuse => Err(missing_targets_error(schema_name, keys, traced, verb)),
        PurgeMissingPolicy::Skip => Ok(Some(
            keys.iter()
                .enumerate()
                .filter(|(idx, _)| traced.contains(idx))
                .map(|(_, k)| k.clone())
                .collect(),
        )),
    }
}

/// One planned slot per (field with a molecule, key) pair.
fn plan_storage_slots(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::Schema,
    keys: &[KeyValue],
) -> Result<Vec<PlannedStorageSlot>, SchemaError> {
    let mut storage_slots = Vec::new();
    for (field_name, field) in &schema.runtime_fields {
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        for storage_key in storage_form_keys(db_ops, field, keys)? {
            let Some((disk_hash, disk_range)) = field.slot_coordinates(&storage_key) else {
                continue;
            };
            storage_slots.push(PlannedStorageSlot {
                field_name: field_name.clone(),
                molecule_uuid: molecule_uuid.clone(),
                storage_key,
                disk_hash,
                disk_range,
            });
        }
    }
    Ok(storage_slots)
}

/// Read-only. Callers must have already refreshed `schema`'s runtime molecules.
pub(super) async fn collect_purge_trace(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::Schema,
    keys: &[KeyValue],
) -> Result<PurgeTrace, SchemaError> {
    let mut candidate_atoms: HashSet<String> = HashSet::new();
    let mut history_keys: Vec<String> = Vec::new();
    let mut atom_ref_v2_edge_keys: Vec<String> = Vec::new();
    let mut history_referenced_by_retained: HashSet<String> = HashSet::new();
    let mut traced: HashSet<usize> = HashSet::new();

    for field in schema.runtime_fields.values() {
        let Some(mol_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };

        // Read existing physical conflict/history references once per field.
        // Ordinary logical writes do not create atom-version history.
        let storage_prefix = field.common().storage_prefix();
        let events = db_ops
            .atoms()
            .get_mutation_event_rows(&mol_uuid, storage_prefix)
            .await?;
        for (event_key, ev) in &events {
            // Physical conflict records can retain source atoms. Match them
            // against the exact target set before body-reachability checks.
            let matched = keys
                .iter()
                .position(|k| field_key_matches_value(&ev.field_key, k));
            match matched {
                Some(idx) => {
                    traced.insert(idx);
                    candidate_atoms.insert(ev.new_atom_uuid.clone());
                    if let Some(old) = &ev.old_atom_uuid {
                        candidate_atoms.insert(old.clone());
                    }
                    history_keys.push(event_key.clone());

                    atom_ref_v2_edge_keys.extend(
                        db_ops.atoms().mutation_history_atom_ref_v2_edge_keys(
                            event_key,
                            ev,
                            storage_prefix,
                        )?,
                    );
                }
                None => collect_event_atom_uuids(ev, &mut history_referenced_by_retained),
            }
        }

        // The molecule is keyed in STORAGE form; `keys` are API form. Translate
        // once per field — both encodings are molecule-uuid-domain-separated, so
        // the mapping differs field to field. See `storage_form_key`.
        let storage_keys = storage_form_keys(db_ops, field, keys)?;

        for (idx, key) in storage_keys.iter().enumerate() {
            if let Some(a) = current_atom_for_key(field, key) {
                candidate_atoms.insert(a);
                traced.insert(idx);
            }
            atom_ref_v2_edge_keys.extend(collect_target_edges(db_ops, field, key)?);
        }
    }

    Ok(PurgeTrace {
        candidate_atoms,
        history_keys,
        atom_ref_v2_edge_keys,
        history_referenced_by_retained,
        traced,
    })
}

/// Check every target in one loud hard-erasure envelope before any target is
/// changed. The lane records a successful check and replays all later durable
/// attempts with `Skip`, which makes a partial storage failure idempotent.
pub(in crate::fold_db_core) async fn validate_purge_targets_present(
    scope: PurgeScope<'_>,
    keys: &[KeyValue],
    verb: HardEraseVerb,
) -> Result<PurgeTargetPresence, SchemaError> {
    if keys.is_empty() {
        return Ok(PurgeTargetPresence::Present);
    }
    let PurgeScope {
        db_ops,
        schema_name,
        ..
    } = scope;
    let mut schema = scope.schema()?;
    for key in keys {
        validate_purge_key_shape(&schema, schema_name, key)?;
    }
    let mut seen = HashSet::new();
    let keys: Vec<KeyValue> = keys
        .iter()
        .filter(|key| seen.insert(describe_key(key)))
        .cloned()
        .collect();

    let materialize_start = std::time::Instant::now();
    let materialized =
        refresh_runtime_field_molecules_for_purge_keys(db_ops, &mut schema, &keys).await;
    crate::request_phases::add_phase(
        crate::request_phases::RequestPhase::PurgeMaterialize,
        materialize_start.elapsed(),
    );
    materialized?;

    let trace_start = std::time::Instant::now();
    let trace = collect_purge_trace(db_ops, &schema, &keys).await;
    crate::request_phases::add_phase(
        crate::request_phases::RequestPhase::PurgeTrace,
        trace_start.elapsed(),
    );
    let trace = trace?;
    if trace.traced.len() == keys.len() {
        Ok(PurgeTargetPresence::Present)
    } else {
        Ok(PurgeTargetPresence::Missing(missing_targets_error(
            schema_name,
            &keys,
            &trace.traced,
            verb,
        )))
    }
}

pub(super) fn missing_targets_error(
    schema_name: &str,
    keys: &[KeyValue],
    traced: &HashSet<usize>,
    verb: HardEraseVerb,
) -> SchemaError {
    // A batch of one keeps the exact wording `purge_record` has always
    // returned. That string is the operator-facing compliance contract.
    if keys.len() == 1 {
        return SchemaError::InvalidData(format!(
            "{} target not found: schema '{}', key {} — refusing to silently no-op {}",
            verb.miss_noun(),
            schema_name,
            describe_key(&keys[0]),
            verb.miss_suffix(),
        ));
    }
    let absent: Vec<String> = keys
        .iter()
        .enumerate()
        .filter(|(idx, _)| !traced.contains(idx))
        .map(|(_, key)| describe_key(key))
        .take(10)
        .collect();
    SchemaError::InvalidData(format!(
        "{} targets not found: schema '{}', {} of {} keys absent (first: {}) — refusing to \
         silently no-op {}",
        verb.miss_noun(),
        schema_name,
        keys.len() - traced.len(),
        keys.len(),
        absent.join(", "),
        verb.miss_suffix(),
    ))
}
