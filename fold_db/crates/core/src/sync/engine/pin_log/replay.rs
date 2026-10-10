use super::*;

/// Decrypt, verify hash, deserialize — inverse of [`seal_mutation_log_segment`].
///
/// Exists so the sealed bytes are provably openable rather than write-only
/// ciphertext. `PinLogRecord` had no cloud reader at all when the plaintext
/// defect was found, which is a large part of why nobody noticed the writer
/// was emitting bare JSON: nothing ever tried to open what it wrote.
///
/// `transfer/s3_io/fetch.rs` calls this on the production replay path to
/// recognize a mutation-log segment sharing the legacy flat `log/{seq}.enc`
/// namespace with replayable `LogEntry` objects.
pub async fn unseal_mutation_log_segment(
    sealed: &[u8],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<PinLogRecord>, String> {
    let json = open_mutation_log_json(sealed, crypto).await?;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WirePayload {
        Batch(Vec<PinLogRecord>),
        LegacySingle(Box<PinLogRecord>),
    }

    let records = match serde_json::from_slice(&json)
        .map_err(|e| format!("decode mutation log segment: {e}"))?
    {
        WirePayload::Batch(records) => records,
        WirePayload::LegacySingle(record) => vec![*record],
    };
    if records.is_empty() {
        return Err("mutation log segment decoded to an empty record batch".to_string());
    }
    Ok(records)
}

pub(super) async fn open_mutation_log_replay_units(
    segments: &[MutationLogSegment],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<MutationLogReplayUnit>, String> {
    // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
    let mut units = Vec::new();
    let mut groups = BTreeMap::<String, TransactionGroupAssembly>::new();

    for object in segments {
        let json = open_mutation_log_json(&object.payload, crypto).await?;
        let Ok(wire) = serde_json::from_slice::<TransactionGroupWireV2>(&json) else {
            units.push(MutationLogReplayUnit {
                segment: object.segment.clone(),
                records: decode_legacy_mutation_log_records(&json)?,
                transaction_group: false,
            });
            continue;
        };
        let raw_wire = serde_json::from_slice::<TransactionGroupWireV2Raw>(&json)
            .map_err(|error| format!("decode raw transaction group wire: {error}"))?;
        match (wire, raw_wire) {
            (
                TransactionGroupWireV2::Shard {
                    format_version,
                    group_id,
                    writer_id,
                    frontier_after,
                    schema_name,
                    shard_index,
                    shard_count,
                    operations,
                },
                TransactionGroupWireV2Raw {
                    wire_type,
                    operations: Some(raw_operations),
                    record_template: None,
                },
            ) if wire_type == "shard" => {
                if format_version != TRANSACTION_GROUP_WIRE_VERSION {
                    return Err(format!(
                        "unsupported transaction group shard version {format_version}"
                    ));
                }
                validate_transaction_group_segment_identity(
                    &object.segment,
                    &writer_id,
                    &schema_name,
                    frontier_after,
                )?;
                if group_id.is_empty()
                    || operations.is_empty()
                    || operations.iter().any(|operation| {
                        operation.mutation.schema_name.trim() != schema_name
                            || operation.mutation.written_at == 0
                    })
                    || object.segment.utc_nanos
                        != operations
                            .iter()
                            .map(|operation| operation.mutation.written_at)
                            .max()
                    || operations.len() != raw_operations.len()
                    || operations
                        .iter()
                        .zip(&raw_operations)
                        .any(|(operation, raw)| operation.original_index != raw.original_index)
                {
                    return Err(format!(
                        "transaction group shard identity does not match its operations for {}",
                        object.segment.object_key
                    ));
                }
                groups
                    .entry(group_id)
                    .or_default()
                    .shards
                    .push(TransactionGroupShardObject {
                        segment: object.segment.clone(),
                        ciphertext_sha256: sha256_hex(&object.payload),
                        writer_id,
                        frontier_after,
                        schema_name,
                        shard_index,
                        shard_count,
                        operations,
                        raw_operations,
                    });
            }
            (
                TransactionGroupWireV2::Manifest {
                    format_version,
                    group_id,
                    writer_id,
                    frontier_after,
                    record_digest_version,
                    record_sha256,
                    operation_count,
                    shard_count,
                    record_template,
                    shards,
                },
                TransactionGroupWireV2Raw {
                    wire_type,
                    operations: None,
                    record_template: Some(raw_record_template),
                },
            ) if wire_type == "manifest" => {
                if format_version != TRANSACTION_GROUP_WIRE_VERSION {
                    return Err(format!(
                        "unsupported transaction group manifest version {format_version}"
                    ));
                }
                validate_transaction_group_segment_identity(
                    &object.segment,
                    &writer_id,
                    TRANSACTION_GROUP_MANIFEST_SCHEMA,
                    frontier_after,
                )?;
                if group_id.is_empty() || operation_count == 0 || shard_count == 0 {
                    return Err(format!(
                        "transaction group manifest is empty for {}",
                        object.segment.object_key
                    ));
                }
                let assembly = groups.entry(group_id.clone()).or_default();
                if assembly.manifest.is_some() {
                    return Err(format!("transaction group {group_id} has two manifests"));
                }
                assembly.manifest = Some(TransactionGroupManifestObject {
                    segment: object.segment.clone(),
                    writer_id,
                    frontier_after,
                    record_digest_version,
                    record_sha256,
                    operation_count,
                    shard_count,
                    record_template,
                    raw_record_template,
                    shards,
                });
            }
            _ => {
                return Err(format!(
                    "transaction group typed and raw wire forms disagree for {}",
                    object.segment.object_key
                ));
            }
        }
    }

    for (group_id, mut assembly) in groups {
        let manifest = assembly.manifest.ok_or_else(|| {
            format!("transaction group {group_id} has shards but no commit manifest")
        })?;
        if manifest.shard_count as usize != manifest.shards.len()
            || manifest.shard_count as usize != assembly.shards.len()
        {
            return Err(format!(
                "transaction group {group_id} shard count does not match its manifest"
            ));
        }
        if !matches!(
            manifest.record_digest_version,
            TRANSACTION_GROUP_RECORD_DIGEST_LEGACY_JSON
                | TRANSACTION_GROUP_RECORD_DIGEST_SORTED_JSON_V1
        ) {
            return Err(format!(
                "unsupported transaction group record digest version {}",
                manifest.record_digest_version
            ));
        }
        assembly.shards.sort_by_key(|shard| shard.shard_index);
        let mut operations = vec![None; manifest.operation_count as usize];
        let mut raw_operations = std::iter::repeat_with(|| None)
            .take(manifest.operation_count as usize)
            .collect::<Vec<Option<Box<RawValue>>>>();
        let mut seen_shards = BTreeSet::new();
        for shard in assembly.shards {
            if shard.writer_id != manifest.writer_id
                || shard.frontier_after != manifest.frontier_after
                || shard.shard_count != manifest.shard_count
                || !seen_shards.insert(shard.shard_index)
            {
                return Err(format!(
                    "transaction group {group_id} has inconsistent shard identity"
                ));
            }
            let reference = manifest
                .shards
                .iter()
                .find(|reference| reference.shard_index == shard.shard_index)
                .ok_or_else(|| {
                    format!(
                        "transaction group {group_id} shard {} is absent from its manifest",
                        shard.shard_index
                    )
                })?;
            let shard_indexes = shard
                .operations
                .iter()
                .map(|operation| operation.original_index)
                .collect::<Vec<_>>();
            if reference.schema_name != shard.schema_name
                || reference.object_key != shard.segment.object_key
                || reference.ciphertext_sha256 != shard.ciphertext_sha256
                || reference.operation_indexes != shard_indexes
            {
                return Err(format!(
                    "transaction group {group_id} shard {} fails manifest validation",
                    shard.shard_index
                ));
            }
            for (operation, raw_operation) in shard.operations.into_iter().zip(shard.raw_operations)
            {
                let index = operation.original_index as usize;
                let slot = operations.get_mut(index).ok_or_else(|| {
                    format!(
                        "transaction group {group_id} operation index {} is out of range",
                        operation.original_index
                    )
                })?;
                if slot.replace(operation.mutation).is_some() {
                    return Err(format!(
                        "transaction group {group_id} repeats operation index {}",
                        operation.original_index
                    ));
                }
                let raw_slot = raw_operations.get_mut(index).ok_or_else(|| {
                    format!(
                        "transaction group {group_id} raw operation index {} is out of range",
                        raw_operation.original_index
                    )
                })?;
                if raw_operation.original_index != operation.original_index
                    || raw_slot.replace(raw_operation.mutation).is_some()
                {
                    return Err(format!(
                        "transaction group {group_id} repeats or misaligns raw operation index {}",
                        raw_operation.original_index
                    ));
                }
            }
        }
        let mutations = operations
            .into_iter()
            .enumerate()
            .map(|(index, mutation)| {
                mutation.ok_or_else(|| {
                    format!("transaction group {group_id} omits operation index {index}")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let raw_mutations = raw_operations
            .into_iter()
            .enumerate()
            .map(|(index, mutation)| {
                mutation.ok_or_else(|| {
                    format!("transaction group {group_id} omits raw operation index {index}")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut record = *manifest.record_template;
        if record.writer_id != manifest.writer_id
            || record.frontier_after != manifest.frontier_after
        {
            return Err(format!(
                "transaction group {group_id} record identity does not match its manifest"
            ));
        }
        let LogOp::MutationIntent {
            mutations: template_mutations,
        } = &mut record.entry.op
        else {
            return Err(format!(
                "transaction group {group_id} template is not a MutationIntent"
            ));
        };
        if !template_mutations.is_empty() {
            return Err(format!(
                "transaction group {group_id} template already contains operations"
            ));
        }
        *template_mutations = mutations;
        let manifest_t0 = template_mutations
            .iter()
            .map(|mutation| mutation.written_at)
            .max();
        if manifest.segment.utc_nanos != manifest_t0 {
            return Err(format!(
                "transaction group {group_id} manifest T0 does not match its operations"
            ));
        }
        let reconstructed_record_sha256 = match manifest.record_digest_version {
            TRANSACTION_GROUP_RECORD_DIGEST_LEGACY_JSON => {
                let record_json = legacy_transaction_group_record_json(
                    manifest.raw_record_template.as_ref(),
                    &raw_mutations,
                )?;
                sha256_hex(&record_json)
            }
            TRANSACTION_GROUP_RECORD_DIGEST_SORTED_JSON_V1 => {
                transaction_group_record_sha256(&record)?
            }
            _ => unreachable!("unsupported record digest versions fail before assembly"),
        };
        if reconstructed_record_sha256 != manifest.record_sha256 {
            return Err(format!(
                "transaction group {group_id} reconstructed record hash mismatch"
            ));
        }
        if transaction_group_id(
            &record,
            manifest.record_digest_version,
            &manifest.record_sha256,
        ) != group_id
        {
            return Err(format!(
                "transaction group {group_id} stable identity does not match its record"
            ));
        }
        units.push(MutationLogReplayUnit {
            segment: manifest.segment,
            records: vec![record],
            transaction_group: true,
        });
    }
    Ok(units)
}

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
    )
    .await
}

pub(super) async fn replay_mutation_log_segments_with_progress(
    engine: &SyncEngine,
    segments: &[MutationLogSegment],
    incorporated_frontier: &Frontier,
    progress: Option<&RestoreProgress>,
    crypto: &Arc<dyn crate::crypto::CryptoProvider>,
) -> SyncResult<MutationLogReplayReport> {
    // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
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
    let mut report = MutationLogReplayReport {
        segments_considered: ordered.len(),
        ..MutationLogReplayReport::default()
    };

    progress::update(progress, |p| p.replay_segments_total = Some(ordered.len()));
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
        let target = if record.target_prefix.is_empty() {
            None
        } else {
            Some(
                engine
                    .pin_log
                    .sync_target_by_prefix(&record.target_prefix)
                    .await
                    .map_err(SyncError::Storage)?,
            )
        };
        engine
            .replay_entry(&record.entry, target.as_ref())
            .await
            .map_err(|error| {
                error.with_replay_operation(record.entry.seq, (&record.entry.op).into())
            })?;
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

    for namespace in touched_namespaces {
        engine
            .store
            .open_namespace(&namespace)
            .await?
            .flush()
            .await?;
    }
    report.frontier_after = frontier_after;
    Ok(report)
}

/// Production restore apply boundary after an S0 base is installed.
///
/// Snapshot/S0 install and continuous-log apply remain separate cloud fetch
/// phases, but product restore (LastStore home restore, device bootstrap after
/// S0) and CoW proofs share this entry so `replay_mutation_log_segments` is not
/// test-harness-only. Callers that already hold sealed segment payloads (local
/// test plane, pre-downloaded objects) pass them here; cloud listing/download
/// is [`SyncEngine::download_mutation_log_segments_above`].
pub async fn restore_mutation_log_after_s0(
    engine: &SyncEngine,
    segments: &[MutationLogSegment],
    incorporated_frontier: &Frontier,
) -> SyncResult<MutationLogReplayReport> {
    replay_mutation_log_segments(engine, segments, incorporated_frontier).await
}

/// Local-plane variant of [`restore_mutation_log_after_s0`] for tests and CoW
/// harnesses that publish sealed segments into [`MutationLogLocalCloud`].
pub async fn restore_mutation_log_after_s0_from_plane(
    engine: &SyncEngine,
    plane: &MutationLogLocalCloud,
    incorporated_frontier: &Frontier,
) -> SyncResult<MutationLogReplayReport> {
    let segments = plane.segments_above(incorporated_frontier);
    restore_mutation_log_after_s0(engine, &segments, incorporated_frontier).await
}

pub(crate) use super::helpers::parse_mutation_log_object_key;

pub(super) fn mutation_log_segment_id_from_object_key(
    listed_object_key: &str,
    through_id: u64,
) -> Result<MutationLogSegmentId, String> {
    let relative =
        super::helpers::relative_mutation_log_key(listed_object_key).unwrap_or(listed_object_key);
    let body = relative
        .strip_prefix("log/")
        .ok_or_else(|| format!("mutation-log object key is outside log/: {listed_object_key}"))?;
    let parts = body.split('/').collect::<Vec<_>>();
    let segment = if let [writer, filename] = parts.as_slice() {
        let sequence = filename
            .strip_suffix(".enc")
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| format!("invalid mutation-log object key: {listed_object_key}"))?;
        MutationLogSegmentId {
            writer_id: Some((*writer).to_string()),
            schema_name: None,
            utc_nanos: None,
            sequence: None,
            through_id: sequence,
            object_key: MutationLogSegmentId::default_object_key(Some(writer), sequence),
        }
    } else if parts.len() >= 3 {
        let writer = parts[0];
        let filename = parts[parts.len() - 1];
        let schema_parts = &parts[1..parts.len() - 1];
        if writer.is_empty()
            || schema_parts
                .iter()
                .any(|component| component.is_empty() || component.contains(".."))
        {
            return Err(format!(
                "invalid mutation-log object key shape: {listed_object_key}"
            ));
        }
        let schema = schema_parts.join("/");
        let stem = filename
            .strip_suffix(".enc")
            .ok_or_else(|| format!("invalid mutation-log object key: {listed_object_key}"))?;
        let (utc_nanos, sequence) = stem
            .rsplit_once('_')
            .and_then(|(utc_nanos, sequence)| {
                Some((
                    utc_nanos.parse::<u64>().ok()?,
                    sequence.parse::<u64>().ok()?,
                ))
            })
            .ok_or_else(|| format!("invalid mutation-log object key: {listed_object_key}"))?;
        MutationLogSegmentId::schema_folder(writer, schema, utc_nanos, sequence, sequence)
    } else {
        return Err(format!(
            "invalid mutation-log object key shape: {listed_object_key}"
        ));
    };
    if segment.through_id != through_id || segment.object_key != relative {
        return Err(format!(
            "mutation-log object key {listed_object_key} does not match through-id {through_id}"
        ));
    }
    Ok(segment)
}

/// Pure helper: pick the writer-scoped mutation-log segment objects one
/// peer-apply / restore cycle must actually download.
///
/// Returns `(candidates, flat_classic_skipped)`, where each candidate is
/// `(writer_id, through_id, listed_object_key)`, sorted by writer then
/// through_id.
///
/// Only `log/{writer}/{seq}.enc` keys name a mutation-log segment. Every
/// segment upload keys itself with `default_object_key(Some(writer), ..)`, so
/// the flat `log/{seq}.enc` shape -- which `parse_mutation_log_object_key`
/// also accepts -- is always classic `LogEntry` ciphertext owned by the
/// ordinary download cursor, never a segment.
///
/// Dropping flat keys *before* the presign+download fan-out is the fix for a
/// cycle-time defect. They used to reach the fan-out and were rejected only
/// afterwards, when `unseal_mutation_log_segment` failed and the loop hit
/// `continue`, so every cycle re-downloaded the whole flat log prefix and threw
/// it away. Measured on the primary 2026-08-26 with `remote_log_entries=11358`:
/// one `do_sync` spent ~38 minutes inside this download for
/// `segments_considered=1`, so the configured 30s `sync_interval_ms` was never
/// observable and `rpo_secs` climbed at wall-clock rate. The returned segment
/// set is unchanged -- those objects could never unseal as segments -- only the
/// wasted downloads are gone.
pub(crate) fn select_peer_apply_candidates<'a, I>(
    listed_keys: I,
    incorporated_frontier: &Frontier,
) -> (Vec<(String, u64, String)>, u64)
where
    I: IntoIterator<Item = &'a str>,
{
    let mut candidates: Vec<(String, u64, String)> = Vec::new();
    let mut flat_classic_skipped: u64 = 0;
    for key in listed_keys {
        let Some((writer, through_id)) = parse_mutation_log_object_key(key) else {
            continue;
        };
        let Some(writer) = writer else {
            flat_classic_skipped = flat_classic_skipped.saturating_add(1);
            continue;
        };
        if incorporated_frontier.covers_log(Some(writer.as_str()), through_id) {
            continue;
        }
        candidates.push((writer, through_id, key.to_string()));
    }
    candidates.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    (candidates, flat_classic_skipped)
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
