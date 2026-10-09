//! Moved verbatim out of the parent module; see the parent for context.
// lint:file-size-ok verbatim move; the one oversized function is separate split work

use super::*;

/// Hard-remove every trace of `keys` from ONE schema, in a single pass. See the
/// module-level docstring for the erasure contract.
///
/// Returns `Err(SchemaError::InvalidData)` when:
/// - the schema is unknown
/// - a key's shape doesn't match the schema type (Hash schema with no hash,
///   Range with no range, HashRange missing either)
/// - any target doesn't exist (no molecule entry AND no history rows match it,
///   across every field of the schema) — checked for the whole batch **before**
///   anything is mutated, so a batch naming a missing key changes nothing
///
/// # Why this is batched
///
/// The per-record predecessor (`purge_record`, removed in the same change that
/// added this) was correct and quadratic. Its work splits into what is
/// genuinely per-record — a key's own live head, its own `tv:` chain, its own
/// molecule entry — and what is per-*schema* but was redone for every record:
///
/// - `refresh_runtime_field_molecules` — reloads every molecule of the schema
/// - `get_mutation_events(mol_uuid, None)` — reads **all** events per field
/// - the sibling-chain guard — walks the tip chain of **every per-key record of
///   every field**
/// - re-persisting molecules, `store_schema`, `load_schema_internal`, `flush`
///
/// With `R` records in the schema and `F` fields, one purge is `O(F·R)`, so
/// purging `N` of them one at a time is `O(N·F·R)`. Draining the node's own
/// telemetry plane (`N = R ≈ 74k`, `F = 39`) is ~10^11 chain walks — which is
/// the real reason 47.6% of the tip plane has no shipped path that can reclaim
/// it, and why "just call the prune" never finished rather than merely being
/// slow. Hoisting the per-schema work out of the loop makes the batch `O(F·R)`
/// total: the same cost as a **single** purge.
///
/// # The comment this replaces
///
/// `process_hard_erasures` said: *"One pass per mutation — purge is rare and there's
/// no batching benefit from grouping by schema."* That was true while purge was
/// only the compliance verb, answering one subject-erasure request at a time.
/// It stopped being true the moment purge became the only primitive that can
/// shrink a plane, and the rationale outlived the fact by long enough that the
/// cost was read as inherent. Both halves were wrong: the benefit is not merely
/// present, it is the difference between `O(N·F·R)` and `O(F·R)`.
///
/// # What is NOT relaxed
///
/// Every guard the per-record path had holds here, computed once over the whole
/// batch instead of once per record:
///
/// - the live-head guard, evaluated **after** all targets are removed;
/// - the history guard, over events belonging to keys outside the batch;
/// - the chain guard, over the complement of the **entire** target set
///   ([`collect_retained_chain_atoms`] — the one guard whose meaning changes
///   with batch size, and the only way to get this wrong in a direction that
///   deletes a live atom);
/// - the write-ahead delete-ledger row, before anything is destroyed;
/// - resident (T0) eviction *after* the durable rows are gone;
/// - the loud not-found error, still evaluated **before** any mutation.
///
/// A batch of one is not a special case in this code — it is this code with
/// `keys.len() == 1`, and it keeps the single-record path's exact error wording
/// and delete-ledger row so nothing operator-facing changed when `purge_record`
/// went away. Every existing purge test (erasure, concurrency, delete-ledger)
/// reaches this function through `process_hard_erasures` and pins that.
///
/// Callers select the reachability proof explicitly. The guarded complement
/// mode requires the schema purge barrier. The exact-edge mode runs in the
/// persist lane after its molecule manifests report replay-complete.
#[allow(clippy::too_many_arguments)]
// lint:fn-size-ok verbatim move from purge/mod.rs; splitting this function is separate work
pub(in crate::fold_db_core) async fn purge_records_bulk(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    keys: &[KeyValue],
    missing: PurgeMissingPolicy,
    verb: HardEraseVerb,
    acct: &mut PurgeCommitAccounting,
    evict_resident: bool,
    reachability: PurgeReachability,
    precomputed_retained: Option<HashSet<String>>,
) -> Result<BulkPurgeReport, SchemaError> {
    if keys.is_empty() {
        return Ok(BulkPurgeReport::empty());
    }

    let mut schema = schema_manager
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found for purge"))
        })?;

    for key in keys {
        validate_purge_key_shape(&schema, schema_name, key)?;
    }

    // A batch is a SET of records. The per-record path made a repeated key a
    // loud error by accident — the second purge found nothing where the first
    // had just erased it — and that is not a contract worth reconstructing at
    // batch scale: the caller asked for the record to be gone, and after the
    // first removal it is. Deduping here keeps `records_purged` a count of
    // distinct records and stops a repeated key from tripping the not-found
    // check against a target this same batch erased.
    //
    // `describe_key` is the dedup token because it renders exactly the
    // `(hash, range)` pair that identifies a slot; on a Single schema every key
    // collapses to `<single>`, which is correct — there is only one record.
    let mut seen: HashSet<String> = HashSet::new();
    let deduped: Vec<KeyValue> = keys
        .iter()
        .filter(|k| seen.insert(describe_key(k)))
        .cloned()
        .collect();
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
        tip_version_keys,
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
        history_referenced_by_retained,
        traced,
    } = traced_batch?;

    // Missing-target policy, BEFORE any mutation. Compliance (`Refuse`) keeps
    // the loud contract from `purge_record`. Delete (`Skip`) filters to the
    // keys that left a trace so re-delete is a no-op without weakening
    // compliance purge. Candidate atoms / history / tip-version keys were
    // only collected for traced targets, so filtering keys keeps those sets
    // consistent with the destructive half of the batch.
    let keys_present: Vec<KeyValue>;
    let keys: &[KeyValue] = if traced.len() == keys.len() {
        keys
    } else {
        match missing {
            PurgeMissingPolicy::Refuse => {
                return Err(missing_targets_error(schema_name, keys, &traced, verb));
            }
            PurgeMissingPolicy::Skip => {
                keys_present = keys
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| traced.contains(idx))
                    .map(|(_, k)| k.clone())
                    .collect();
                if keys_present.is_empty() {
                    return Ok(BulkPurgeReport::empty());
                }
                &keys_present
            }
        }
    };

    let described_key = if keys.len() == 1 {
        describe_key(&keys[0])
    } else {
        format!("<bulk:{} keys>", keys.len())
    };
    let mut storage_slots = Vec::new();
    for (field_name, field) in &schema.runtime_fields {
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        for (api_key, storage_key) in keys.iter().zip(storage_form_keys(db_ops, field, keys)?) {
            let Some((disk_hash, disk_range)) = field.disk_slot_for_key(&storage_key) else {
                continue;
            };
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

    execute_guarded_purge(
        db_ops,
        schema_manager,
        schema_name,
        schema,
        &storage_slots,
        candidate_atoms,
        history_keys,
        tip_version_keys,
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
        history_referenced_by_retained,
        keys.len(),
        &described_key,
        verb,
        acct,
        evict_resident,
        reachability,
        precomputed_retained,
    )
    .await
}

/// Read-only. Callers must have already refreshed `schema`'s runtime molecules.
pub(super) async fn collect_purge_trace(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::Schema,
    keys: &[KeyValue],
) -> Result<PurgeTrace, SchemaError> {
    let mut candidate_atoms: HashSet<String> = HashSet::new();
    let mut history_keys: Vec<String> = Vec::new();
    let mut tip_version_keys: Vec<String> = Vec::new();
    let mut atom_ref_edge_keys: Vec<String> = Vec::new();
    let mut atom_ref_v2_edge_keys: Vec<String> = Vec::new();
    let mut history_referenced_by_retained: HashSet<String> = HashSet::new();
    let mut traced: HashSet<usize> = HashSet::new();

    for field in schema.runtime_fields.values() {
        let Some(mol_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };

        // ONCE per field, not once per record — this read is the bulk of what
        // made the per-record path quadratic. It is also an unprunable prefix
        // walk (see `crate::atom::legacy_history_memo`), so on a current store,
        // where it can only ever return nothing, the memo is what keeps it from
        // sweeping the collection on every purge.
        let storage_prefix = field.common().storage_prefix();
        let events = db_ops
            .atoms()
            .get_mutation_event_rows(&mol_uuid, storage_prefix)
            .await?;
        for (event_key, ev) in &events {
            // Linear over targets, but only for stores that still carry legacy
            // `history:` rows: the production write path retired them when the
            // `tv:` chain replaced them, so `events` is empty on any current
            // store and this loop does not run at all.
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
                    atom_ref_edge_keys.extend(db_ops.atoms().mutation_history_atom_ref_edge_keys(
                        event_key,
                        ev,
                        storage_prefix,
                    ));
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
            let chain = collect_target_chain(db_ops, field, key).await?;
            if !chain.atom_uuids.is_empty() || !chain.tip_version_keys.is_empty() {
                traced.insert(idx);
            }
            candidate_atoms.extend(chain.atom_uuids);
            tip_version_keys.extend(chain.tip_version_keys);
            atom_ref_edge_keys.extend(chain.atom_ref_edge_keys);
            atom_ref_v2_edge_keys.extend(chain.atom_ref_v2_edge_keys);
        }
    }

    Ok(PurgeTrace {
        candidate_atoms,
        history_keys,
        tip_version_keys,
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
        history_referenced_by_retained,
        traced,
    })
}

/// Check every target in one loud hard-erasure envelope before any target is
/// changed. The lane records a successful check and replays all later durable
/// attempts with `Skip`, which makes a partial storage failure idempotent.
pub(in crate::fold_db_core) async fn validate_purge_targets_present(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    keys: &[KeyValue],
    verb: HardEraseVerb,
) -> Result<PurgeTargetPresence, SchemaError> {
    if keys.is_empty() {
        return Ok(PurgeTargetPresence::Present);
    }

    let mut schema = schema_manager
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found for purge"))
        })?;
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

/// Walk retained chains **without** the exclusive schema barrier and **without**
/// occupying the persist lane.
///
/// The persist worker used to run this walk under `schema_purge_barrier` while
/// holding the head of the schema FIFO. A one-record purge then occupied the
/// lane for ~328 s, `reserved_bytes` froze, and every later writer on that
/// schema got `503 persist_queue_full kind=entries`. Planning here (TTL /
/// request thread) lets the lane drain during the walk. The persist envelope
/// only commits.
pub(in crate::fold_db_core) async fn plan_guarded_complement_retained(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    keys: &[KeyValue],
) -> Result<HashSet<String>, SchemaError> {
    if keys.is_empty() {
        return Ok(HashSet::new());
    }
    let mut schema = schema_manager
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' not found for purge retain-plan"
            ))
        })?;
    for key in keys {
        validate_purge_key_shape(&schema, schema_name, key)?;
    }
    // This walk runs on the request thread, before the lane slot is reserved,
    // so none of it sits inside the exclusive hold that `PurgeCommit` is the
    // residual of. Each step records the phase the destructive path records
    // for the same work; otherwise ~30% of a purge request was in no phase.
    let materialize_start = std::time::Instant::now();
    let materialized = refresh_runtime_field_molecules(db_ops, &mut schema).await;
    crate::request_phases::add_phase(
        crate::request_phases::RequestPhase::PurgeMaterialize,
        materialize_start.elapsed(),
    );
    materialized?;
    let mut seen: HashSet<String> = HashSet::new();
    let deduped: Vec<KeyValue> = keys
        .iter()
        .filter(|k| seen.insert(describe_key(k)))
        .cloned()
        .collect();
    let trace_start = std::time::Instant::now();
    let trace = collect_purge_trace(db_ops, &schema, &deduped).await;
    crate::request_phases::add_phase(
        crate::request_phases::RequestPhase::PurgeTrace,
        trace_start.elapsed(),
    );
    let trace = trace?;
    let mut target_slots: HashSet<(String, String, String)> = HashSet::new();
    for field in schema.runtime_fields.values() {
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        for storage_key in storage_form_keys(db_ops, field, &deduped)? {
            let Some((disk_hash, disk_range)) = field.disk_slot_for_key(&storage_key) else {
                continue;
            };
            target_slots.insert((molecule_uuid.clone(), disk_hash, disk_range));
        }
    }
    let guard_start = std::time::Instant::now();
    let retained = collect_retained_chain_atoms_for_storage_slots(
        db_ops,
        &schema,
        &trace.candidate_atoms,
        &target_slots,
    )
    .await;
    crate::request_phases::add_phase(
        crate::request_phases::RequestPhase::PurgeRetentionGuard,
        guard_start.elapsed(),
    );
    retained
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
