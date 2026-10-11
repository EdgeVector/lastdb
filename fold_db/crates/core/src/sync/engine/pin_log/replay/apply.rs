use super::*;

/// Apply encrypted mutation-log segments newer than an incorporated snapshot
/// frontier, then flush every touched namespace before returning.
///
/// Snapshot restore and log replay intentionally remain separate cloud fetch
/// phases, but share this apply boundary. Segment metadata is authenticated by
/// the encrypted record payload: writer, through-id, and object key must agree
/// before any record is applied. A segment that straddles the snapshot frontier
/// replays only its newer records.
pub async fn replay_mutation_log_segments(
    engine: &SyncEngine,
    segments: &[MutationLogSegment],
    incorporated_frontier: &Frontier,
) -> SyncResult<MutationLogReplayReport> {
    replay_mutation_log_segments_with_crypto(
        engine,
        segments,
        incorporated_frontier,
        &engine.crypto,
    )
    .await
}

pub async fn replay_mutation_log_segments_with_crypto(
    engine: &SyncEngine,
    segments: &[MutationLogSegment],
    incorporated_frontier: &Frontier,
    crypto: &Arc<dyn crate::crypto::CryptoProvider>,
) -> SyncResult<MutationLogReplayReport> {
    replay_mutation_log_segments_with_progress(
        engine,
        segments,
        incorporated_frontier,
        None,
        crypto,
        None,
    )
    .await
}

pub(in super::super) async fn replay_mutation_log_segments_with_progress(
    engine: &SyncEngine,
    segments: &[MutationLogSegment],
    incorporated_frontier: &Frontier,
    progress: Option<&RestoreProgress>,
    crypto: &Arc<dyn crate::crypto::CryptoProvider>,
    scope_target: Option<&SyncTarget>,
) -> SyncResult<MutationLogReplayReport> {
    progress::phase(progress, RestorePhase::TailReplay);
    // Files are packed by kind, so one file's through-id can sit above another
    // file that holds the frontiers in between. Apply records in
    // (writer, frontier) order. Applying a whole file first would advance the
    // writer frontier past those in-between records and skip them.
    let ordered = open_mutation_log_replay_units(segments, crypto)
        .await
        .map_err(SyncError::Storage)?;

    let mut frontier_after = match incorporated_frontier {
        Frontier::Scalar { .. } => BTreeMap::new(),
        Frontier::Vector { through } => through.clone(),
    };
    // Only a true Scalar F (S0 cut) seeds missing writers. A one-entry Vector
    // must not: `as_scalar_through` would copy writer A's published through-id
    // onto writer B and skip B's stream whenever B's seq is <= A's F.
    let scalar_base = match incorporated_frontier {
        Frontier::Scalar { through } => Some(*through),
        Frontier::Vector { .. } => None,
    };
    let mut touched_namespaces = BTreeSet::new();
    let mut schema_barrier = super::super::schema_replay::SchemaReplayBarrier::default();
    let mut report = MutationLogReplayReport {
        segments_considered: ordered.len(),
        ..MutationLogReplayReport::default()
    };

    progress::update(progress, |p| p.replay_segments_total = Some(ordered.len()));
    let validated = validate_replay_units(ordered)?;
    let apply_order = order_replay_records(&validated);

    let mut unit_applied = vec![false; validated.len()];
    for (unit_index, record_index) in apply_order {
        let record = &validated[unit_index][record_index];
        let writer_frontier = frontier_after
            .entry(record.writer_id.clone())
            .or_insert_with(|| scalar_base.unwrap_or(0));
        if record.frontier_after <= *writer_frontier {
            report.records_skipped_at_or_below_frontier += 1;
            continue;
        }
        let target = super::super::replay_scope::target(engine, record, scope_target).await?;
        engine
            .replay_entry_with_schema_refresh(
                &record.entry,
                target.as_ref(),
                schema_barrier.requires_refresh(&record.entry),
            )
            .await
            .map_err(|error| {
                error.with_replay_operation(record.entry.seq, (&record.entry.op).into())
            })?;
        schema_barrier.applied(&record.entry, &mut touched_namespaces);
        touched_namespaces.insert(log_entry_namespace(&record.entry).to_string());
        *writer_frontier = record.frontier_after;
        report.records_applied += 1;
        unit_applied[unit_index] = true;
        progress::update(progress, |p| {
            p.replay_records_applied = report.records_applied;
        });
    }
    for applied in unit_applied {
        if applied {
            report.segments_applied += 1;
            progress::update(progress, |p| {
                p.replay_segments_applied = report.segments_applied;
            });
        }
    }

    flush_touched_namespaces(engine, touched_namespaces).await?;
    schema_barrier.finish(engine).await?;
    report.frontier_after = frontier_after;
    Ok(report)
}

/// Check every replay unit's identity, writer stream and record order before
/// any record is applied.
fn validate_replay_units(
    ordered: Vec<MutationLogReplayUnit>,
) -> SyncResult<Vec<Vec<PinLogRecord>>> {
    let mut validated: Vec<Vec<PinLogRecord>> = Vec::with_capacity(ordered.len());
    for segment in ordered {
        let records = segment.records;
        let last = records.last().ok_or_else(|| {
            SyncError::Storage("mutation-log segment decoded empty during replay".to_string())
        })?;
        let writer_id = last.writer_id.as_str();
        if records.iter().any(|record| record.writer_id != writer_id) {
            return Err(SyncError::Storage(format!(
                "mutation-log segment {} mixes writer streams",
                segment.segment.object_key
            )));
        }
        if segment.segment.writer_id.as_deref() != Some(writer_id)
            || segment.segment.through_id != last.frontier_after
            || segment.segment.object_key != segment.segment.expected_object_key()
        {
            return Err(SyncError::Storage(format!(
                "mutation-log segment identity mismatch for {}",
                segment.segment.object_key
            )));
        }
        let has_typed_identity = segment.segment.schema_name.is_some()
            || segment.segment.utc_nanos.is_some()
            || segment.segment.sequence.is_some();
        if has_typed_identity && !segment.transaction_group {
            let (schema, utc_nanos) = mutation_log_batch_identity(&records)
                .map_err(SyncError::Storage)?
                .ok_or_else(|| {
                    SyncError::Storage(format!(
                        "mutation-log segment typed identity has a legacy payload for {}",
                        segment.segment.object_key
                    ))
                })?;
            if segment.segment.schema_name.as_deref() != Some(schema)
                || segment.segment.utc_nanos != Some(utc_nanos)
                || segment.segment.sequence != Some(last.frontier_after)
            {
                return Err(SyncError::Storage(format!(
                    "mutation-log segment typed identity mismatch for {}",
                    segment.segment.object_key
                )));
            }
        }
        if records
            .windows(2)
            .any(|pair| pair[0].frontier_after >= pair[1].frontier_after)
        {
            return Err(SyncError::Storage(format!(
                "mutation-log segment {} has non-monotonic records",
                segment.segment.object_key
            )));
        }
        validated.push(records);
    }
    Ok(validated)
}

/// Order `(unit, record)` pairs by writer, then frontier, so a record that
/// sits between two files is not skipped by an earlier frontier advance.
fn order_replay_records(validated: &[Vec<PinLogRecord>]) -> Vec<(usize, usize)> {
    let mut apply_order: Vec<(usize, usize)> = Vec::new();
    for (unit_index, records) in validated.iter().enumerate() {
        for record_index in 0..records.len() {
            apply_order.push((unit_index, record_index));
        }
    }
    apply_order.sort_by(|&(left_unit, left_rec), &(right_unit, right_rec)| {
        let left = &validated[left_unit][left_rec];
        let right = &validated[right_unit][right_rec];
        left.writer_id
            .cmp(&right.writer_id)
            .then(left.frontier_after.cmp(&right.frontier_after))
            .then(left_unit.cmp(&right_unit))
            .then(left_rec.cmp(&right_rec))
    });
    apply_order
}

async fn flush_touched_namespaces(
    engine: &SyncEngine,
    touched_namespaces: BTreeSet<String>,
) -> SyncResult<()> {
    for namespace in touched_namespaces {
        engine
            .store
            .open_namespace(&namespace)
            .await?
            .flush()
            .await?;
    }
    Ok(())
}

/// Production restore apply boundary after an S0 base is installed.
///
/// Snapshot/S0 install and continuous-log apply remain separate cloud fetch
/// phases, but product restore (LastStore home restore, device bootstrap after
/// S0) share this entry. Callers that already hold sealed segment payloads
/// (local cloud storage, pre-downloaded objects) pass them here; cloud listing/download
/// is [`SyncEngine::download_mutation_log_segments_above`].
pub async fn restore_mutation_log_after_s0(
    engine: &SyncEngine,
    segments: &[MutationLogSegment],
    incorporated_frontier: &Frontier,
) -> SyncResult<MutationLogReplayReport> {
    replay_mutation_log_segments(engine, segments, incorporated_frontier).await
}

/// Local-cloud variant of [`restore_mutation_log_after_s0`] for sealed
/// segments published into [`MutationLogLocalCloud`].
pub async fn restore_mutation_log_after_s0_from_plane(
    engine: &SyncEngine,
    plane: &MutationLogLocalCloud,
    incorporated_frontier: &Frontier,
) -> SyncResult<MutationLogReplayReport> {
    let segments = plane.segments_above(incorporated_frontier);
    restore_mutation_log_after_s0(engine, &segments, incorporated_frontier).await
}
