use super::*;

/// Seal one durable pin-log record into a segment object under
/// `log/{writer_id}/{seq}.enc` (design-lastdb-cloud-sync-mutation-log-first).
///
/// Serialize, hash, encrypt — byte-for-byte the shape [`LogEntry::seal`]
/// produces, because these objects land in the SAME `log/` prefix that replay
/// and the decryptability prover read as sealed envelopes.
///
/// This function used to be `serde_json::to_vec(record)` and nothing else,
/// despite being called "seal" and writing to a `.enc` key. It took no crypto
/// provider, so there was no argument at the call site to notice was missing.
/// On 2026-08-09 that put 3,171 objects / 1.20 GB of plaintext user records
/// into production R2 — `atom:`/`aloc:` keys with values readable as
/// `{"cont…` — and simultaneously locked the engine out of its own prefix,
/// because the prover read the leading `{` (0x7B = 123) as an envelope version
/// byte and refused every subsequent cycle with "unsupported log envelope
/// version: 123". Both symptoms are this one missing encrypt.
///
/// See `papercut-lastdb-cloud-log-segments-uploaded-unencrypted-plaintext-to-prod-r2`.
pub async fn seal_mutation_log_segment(
    record: &PinLogRecord,
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<MutationLogSegment, String> {
    seal_mutation_log_segment_batch(std::slice::from_ref(record), crypto).await
}

pub(super) async fn seal_mutation_log_json<T: Serialize + ?Sized>(
    value: &T,
    crypto: &Arc<dyn CryptoProvider>,
    label: &str,
) -> Result<Vec<u8>, String> {
    let json = serde_json::to_vec(value).map_err(|e| format!("encode {label}: {e}"))?;
    let hash = Sha256::digest(&json);
    let mut plaintext = Vec::with_capacity(MUTATION_LOG_SEGMENT_HASH_SIZE + json.len());
    plaintext.extend_from_slice(hash.as_slice());
    plaintext.extend_from_slice(&json);
    crypto
        .encrypt(&plaintext)
        .await
        .map_err(|e| format!("seal {label}: {e}"))
}

pub(super) async fn open_mutation_log_json(
    sealed: &[u8],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<u8>, String> {
    let plaintext = crypto
        .decrypt(sealed)
        .await
        .map_err(|e| format!("unseal mutation log segment: {e}"))?;
    if plaintext.len() < MUTATION_LOG_SEGMENT_HASH_SIZE {
        return Err(format!(
            "mutation log segment too short to carry its hash: {} bytes",
            plaintext.len()
        ));
    }
    let (hash, json) = plaintext.split_at(MUTATION_LOG_SEGMENT_HASH_SIZE);
    let actual = Sha256::digest(json);
    if actual.as_slice() != hash {
        return Err("mutation log segment hash mismatch after decrypt".to_string());
    }
    Ok(json.to_vec())
}

/// Seal one object containing a bounded run of records from one writer.
///
/// New payloads are JSON arrays. [`unseal_mutation_log_segment`] also accepts
/// the historical single-record JSON object, so already-published objects stay
/// readable across the batching cutover.
pub async fn seal_mutation_log_segment_batch(
    records: &[PinLogRecord],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<MutationLogSegment, String> {
    let Some(last) = records.last() else {
        return Err("cannot seal an empty mutation log segment".to_string());
    };
    if records.len() > MUTATION_LOG_SEGMENT_MAX_RECORDS {
        return Err(format!(
            "mutation log segment has {} records; maximum is {MUTATION_LOG_SEGMENT_MAX_RECORDS}",
            records.len()
        ));
    }
    let writer = if last.writer_id.is_empty() {
        "unknown-writer"
    } else {
        last.writer_id.as_str()
    };
    if records.iter().any(|record| {
        let record_writer = if record.writer_id.is_empty() {
            "unknown-writer"
        } else {
            record.writer_id.as_str()
        };
        record_writer != writer
    }) {
        return Err("mutation log segment cannot mix writer streams".to_string());
    }
    if records
        .windows(2)
        .any(|pair| pair[0].frontier_after >= pair[1].frontier_after)
    {
        return Err("mutation log segment frontiers must be strictly increasing".to_string());
    }

    let identity = mutation_log_batch_identity(records)?;
    let segment = if let Some((schema, utc_nanos)) = identity {
        MutationLogSegmentId::schema_folder(
            writer,
            schema,
            utc_nanos,
            last.frontier_after,
            last.frontier_after,
        )
    } else {
        let object_key =
            MutationLogSegmentId::default_object_key(Some(writer), last.frontier_after);
        MutationLogSegmentId {
            writer_id: Some(writer.to_string()),
            schema_name: None,
            utc_nanos: None,
            sequence: None,
            through_id: last.frontier_after,
            object_key,
        }
    };

    let payload = seal_mutation_log_json(records, crypto, "mutation log segment batch").await?;

    Ok(MutationLogSegment { segment, payload })
}

pub(super) async fn seal_transaction_group(
    record: &PinLogRecord,
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<MutationLogSegment>, String> {
    // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
    let LogOp::MutationIntent { mutations } = &record.entry.op else {
        return Err("transaction group requires a MutationIntent".to_string());
    };
    let Some((schemas, utc_nanos)) = mutation_log_record_schemas(record)? else {
        return Err("transaction group requires typed mutations".to_string());
    };
    if schemas.len() < 2 {
        return Err("transaction group requires more than one schema".to_string());
    }
    let operation_count = u32::try_from(mutations.len())
        .map_err(|_| "transaction group has too many operations".to_string())?;
    let shard_count = u32::try_from(schemas.len())
        .map_err(|_| "transaction group has too many schemas".to_string())?;
    let record_digest_version = TRANSACTION_GROUP_RECORD_DIGEST_SORTED_JSON_V1;
    let record_sha256 = transaction_group_record_sha256(record)?;
    let group_id = transaction_group_id(record, record_digest_version, &record_sha256);

    let writer = if record.writer_id.is_empty() {
        "unknown-writer"
    } else {
        record.writer_id.as_str()
    };
    let mut by_schema: BTreeMap<String, Vec<TransactionGroupIndexedMutation>> = BTreeMap::new();
    for (index, mutation) in mutations.iter().cloned().enumerate() {
        let original_index = u32::try_from(index)
            .map_err(|_| "transaction group operation index exceeds u32".to_string())?;
        by_schema
            .entry(mutation.schema_name.trim().to_string())
            .or_default()
            .push(TransactionGroupIndexedMutation {
                original_index,
                mutation,
            });
    }

    let mut objects = Vec::with_capacity(by_schema.len() + 1);
    let mut shard_refs = Vec::with_capacity(by_schema.len());
    for (shard_index, (schema_name, operations)) in by_schema.into_iter().enumerate() {
        let shard_index = u32::try_from(shard_index)
            .map_err(|_| "transaction group shard index exceeds u32".to_string())?;
        let shard_t0 = operations
            .iter()
            .map(|operation| operation.mutation.written_at)
            .max()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("transaction group shard {schema_name} has no positive T0"))?;
        let segment = MutationLogSegmentId::schema_folder(
            writer,
            schema_name.as_str(),
            shard_t0,
            record.frontier_after,
            record.frontier_after,
        );
        let operation_indexes = operations
            .iter()
            .map(|operation| operation.original_index)
            .collect::<Vec<_>>();
        let wire = TransactionGroupWireV2::Shard {
            format_version: TRANSACTION_GROUP_WIRE_VERSION,
            group_id: group_id.clone(),
            writer_id: writer.to_string(),
            frontier_after: record.frontier_after,
            schema_name: schema_name.clone(),
            shard_index,
            shard_count,
            operations,
        };
        let payload = seal_mutation_log_json(&wire, crypto, "transaction group shard").await?;
        shard_refs.push(TransactionGroupShardRef {
            schema_name,
            shard_index,
            object_key: segment.object_key.clone(),
            ciphertext_sha256: sha256_hex(&payload),
            operation_indexes,
        });
        objects.push(MutationLogSegment { segment, payload });
    }

    let mut record_template = record.clone();
    let LogOp::MutationIntent { mutations } = &mut record_template.entry.op else {
        return Err("transaction group template lost its MutationIntent".to_string());
    };
    mutations.clear();
    let manifest_wire = TransactionGroupWireV2::Manifest {
        format_version: TRANSACTION_GROUP_WIRE_VERSION,
        group_id,
        writer_id: writer.to_string(),
        frontier_after: record.frontier_after,
        record_digest_version,
        record_sha256,
        operation_count,
        shard_count,
        record_template: Box::new(record_template),
        shards: shard_refs,
    };
    let manifest_segment = MutationLogSegmentId::schema_folder(
        writer,
        TRANSACTION_GROUP_MANIFEST_SCHEMA,
        utc_nanos,
        record.frontier_after,
        record.frontier_after,
    );
    let manifest_payload =
        seal_mutation_log_json(&manifest_wire, crypto, "transaction group manifest").await?;
    objects.push(MutationLogSegment {
        segment: manifest_segment,
        payload: manifest_payload,
    });
    Ok(objects)
}

/// Group one cycle's sealed publish units into as few cloud upload calls as
/// the ordering rules allow, in the order the calls must run.
///
/// Every `upload_mutation_log_segments` call pays a prefix decrypt proof
/// (keycheck presign + GET) and a presign round trip through the auth Lambda
/// before its first PUT. The cycle used to make one call per unit, and a
/// backlog of small writes seals one unit per record, so a 1,300-segment
/// cycle made ~2,600 serial Lambda round trips. On the primary 2026-10-05 that
/// was 30-45 min per cycle for ~30 s of log time, a backlog that grew at wall
/// clock, and a safe-upgrade soak that could never see fresh cloud progress.
///
/// Two ordering rules survive the batching:
/// - A transaction-group manifest is the last segment of its unit and must
///   land after its shards, so every manifest goes in a later call than every
///   shard.
/// - Presign attaches the typed identity list only when every segment in the
///   request has one, so typed and legacy segments never share a call.
///
/// Nothing else depends on order: published F advances only after every call
/// in the cycle returns, and a re-upload of the same object key is idempotent.
pub(crate) fn plan_publish_unit_upload_batches(
    units: &[Vec<MutationLogSegment>],
) -> Vec<Vec<&MutationLogSegment>> {
    fn typed(segment: &MutationLogSegment) -> bool {
        let id = &segment.segment;
        id.writer_id.is_some()
            && id.schema_name.is_some()
            && id.utc_nanos.is_some()
            && id.sequence.is_some()
    }
    let mut bodies: [Vec<&MutationLogSegment>; 2] = [Vec::new(), Vec::new()];
    let mut manifests: [Vec<&MutationLogSegment>; 2] = [Vec::new(), Vec::new()];
    for unit in units {
        let manifest_last = unit.len() > 1
            && unit.last().is_some_and(|object| {
                object.segment.schema_name.as_deref() == Some(TRANSACTION_GROUP_MANIFEST_SCHEMA)
            });
        let (body, manifest) = if manifest_last {
            unit.split_at(unit.len() - 1)
        } else {
            (unit.as_slice(), &[][..])
        };
        for segment in body {
            bodies[usize::from(typed(segment))].push(segment);
        }
        for segment in manifest {
            manifests[usize::from(typed(segment))].push(segment);
        }
    }
    let [legacy_bodies, typed_bodies] = bodies;
    let [legacy_manifests, typed_manifests] = manifests;
    [
        typed_bodies,
        legacy_bodies,
        typed_manifests,
        legacy_manifests,
    ]
    .into_iter()
    .filter(|batch| !batch.is_empty())
    .collect()
}

/// Publish one cycle's sealed units with the call plan from
/// [`plan_publish_unit_upload_batches`]. Returns the bytes uploaded. The first
/// failed call stops the cycle; the caller then advances nothing.
pub(crate) async fn upload_publish_units_batched(
    engine: &SyncEngine,
    target: &SyncTarget,
    units: &[Vec<MutationLogSegment>],
) -> Result<u64, String> {
    let objects_in_cycle: usize = units.iter().map(Vec::len).sum();
    let policy_concurrency = engine.active_upload_caps().await.concurrency;
    let put_concurrency = mutation_log_put_concurrency(
        policy_concurrency,
        objects_in_cycle,
        mutation_log_put_concurrency_env(),
    );
    if put_concurrency != policy_concurrency {
        tracing::info!(
            target: "fold_db::sync::mutation_log",
            objects = objects_in_cycle,
            policy_concurrency,
            put_concurrency,
            "mutation-log PUT fan-out raised above the adaptive policy"
        );
    }
    let mut uploaded = 0u64;
    for batch in plan_publish_unit_upload_batches(units) {
        let is_manifest = batch.iter().all(|object| {
            object.segment.schema_name.as_deref() == Some(TRANSACTION_GROUP_MANIFEST_SCHEMA)
        });
        let owned: Vec<MutationLogSegment> = batch.into_iter().cloned().collect();
        uploaded = uploaded.saturating_add(
            engine
                .upload_mutation_log_segments_with_put_concurrency(
                    target,
                    &owned,
                    Some(put_concurrency),
                )
                .await
                .map_err(|e| {
                    if is_manifest {
                        format!("mutation-log group manifest upload failed: {e}")
                    } else {
                        format!("mutation-log segment upload failed: {e}")
                    }
                })?,
        );
    }
    Ok(uploaded)
}

pub(super) async fn seal_mutation_log_publish_unit(
    records: &[PinLogRecord],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<MutationLogSegment>, String> {
    if records.len() == 1
        && matches!(
            mutation_log_record_stream(&records[0])?,
            MutationLogRecordStream::MultiSchema
        )
    {
        return seal_transaction_group(&records[0], crypto).await;
    }
    Ok(vec![
        seal_mutation_log_segment_batch(records, crypto).await?,
    ])
}

pub(super) fn pack_writer_id(record: &PinLogRecord) -> &str {
    if record.writer_id.is_empty() {
        "unknown-writer"
    } else {
        record.writer_id.as_str()
    }
}

/// `None` is a multi-schema record. It stays its own file so the transaction
/// group sealer still sees one record.
pub(super) fn pack_kind_key(record: &PinLogRecord) -> Result<Option<String>, String> {
    let stream = mutation_log_record_stream(record)?;
    let key = match stream {
        MutationLogRecordStream::MultiSchema => return Ok(None),
        MutationLogRecordStream::Legacy => "legacy".to_string(),
        MutationLogRecordStream::SingleSchema(schema) => format!("schema:{schema}"),
    };
    Ok(Some(format!("{}\0{key}", pack_writer_id(record))))
}

/// One open file: the kind key, the record count, and the JSON byte count.
pub(super) struct PackSlot {
    pub(super) key: String,
    pub(super) records: usize,
    pub(super) json_bytes: usize,
}

/// Open-file state for one `(writer, kind)` run: record count and JSON bytes,
/// counting the `[]` wrapper.
pub(super) fn next_pack_state(
    runs: &HashMap<String, (usize, usize)>,
    batch_count: usize,
    record: &PinLogRecord,
    encoded_len: usize,
    segment_byte_target: usize,
) -> Result<(usize, Option<PackSlot>), String> {
    let Some(key) = pack_kind_key(record)? else {
        return Ok((batch_count.saturating_add(1), None));
    };
    if let Some((len, json_bytes)) = runs.get(&key).copied() {
        let added = encoded_len.saturating_add(1);
        if len >= MUTATION_LOG_SEGMENT_MAX_RECORDS
            || json_bytes.saturating_add(added) > segment_byte_target
        {
            Ok((
                batch_count.saturating_add(1),
                Some(PackSlot {
                    key,
                    records: 1,
                    json_bytes: 2usize.saturating_add(encoded_len),
                }),
            ))
        } else {
            Ok((
                batch_count,
                Some(PackSlot {
                    key,
                    records: len + 1,
                    json_bytes: json_bytes.saturating_add(added),
                }),
            ))
        }
    } else {
        Ok((
            batch_count.saturating_add(1),
            Some(PackSlot {
                key,
                records: 1,
                json_bytes: 2usize.saturating_add(encoded_len),
            }),
        ))
    }
}

/// Sort one frontier-ordered page by kind and pack each kind into files of at
/// most [`MUTATION_LOG_SEGMENT_MAX_RECORDS`] (or `segment_byte_target`).
///
/// A page is in frontier order. Brain writes alternate schemas, so packing in
/// that order used to seal one record per file. The published frontier is the
/// max frontier of the records returned here. When `max_segments > 0` the
/// return value is a prefix of `records`: a later record is never sealed
/// while an earlier one is left behind.
pub(super) fn plan_mutation_log_record_batches(
    records: Vec<PinLogRecord>,
    max_segments: usize,
    segment_byte_target: usize,
) -> Result<Vec<Vec<PinLogRecord>>, String> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let mut sized = Vec::with_capacity(records.len());
    for record in records {
        let encoded_len = serde_json::to_vec(&record)
            .map_err(|e| format!("size mutation log record for batching: {e}"))?
            .len();
        sized.push((record, encoded_len));
    }

    let mut keep = sized.len();
    if max_segments > 0 {
        let mut runs: HashMap<String, (usize, usize)> = HashMap::new();
        let mut batch_count = 0usize;
        for (index, (record, encoded_len)) in sized.iter().enumerate() {
            let (next_count, update) = next_pack_state(
                &runs,
                batch_count,
                record,
                *encoded_len,
                segment_byte_target,
            )?;
            if next_count > max_segments {
                keep = index;
                break;
            }
            batch_count = next_count;
            if let Some(slot) = update {
                runs.insert(slot.key, (slot.records, slot.json_bytes));
            }
        }
    }

    let mut multi_indexes = Vec::new();
    let mut rest_indexes = Vec::new();
    for (index, (record, _)) in sized[..keep].iter().enumerate() {
        if pack_kind_key(record)?.is_none() {
            multi_indexes.push(index);
        } else {
            rest_indexes.push(index);
        }
    }
    rest_indexes.sort_by(|&left, &right| {
        let (left_record, _) = &sized[left];
        let (right_record, _) = &sized[right];
        pack_writer_id(left_record)
            .cmp(pack_writer_id(right_record))
            .then_with(|| {
                let left_kind = pack_kind_key(left_record).ok().flatten();
                let right_kind = pack_kind_key(right_record).ok().flatten();
                left_kind.cmp(&right_kind)
            })
            .then(left_record.frontier_after.cmp(&right_record.frontier_after))
            .then(left.cmp(&right))
    });

    let mut batches: Vec<Vec<PinLogRecord>> = Vec::new();
    let mut current: Vec<PinLogRecord> = Vec::new();
    let mut current_json_bytes = 2usize;
    let mut current_kind: Option<String> = None;
    for index in rest_indexes {
        let (record, encoded_len) = &sized[index];
        let Some(kind) = pack_kind_key(record)? else {
            return Err("multi-schema record was packed with a single-schema file".to_string());
        };
        let added = encoded_len + usize::from(!current.is_empty());
        let must_flush = !current.is_empty()
            && (current_kind.as_ref() != Some(&kind)
                || current.len() >= MUTATION_LOG_SEGMENT_MAX_RECORDS
                || current_json_bytes.saturating_add(added) > segment_byte_target);
        if must_flush {
            batches.push(std::mem::take(&mut current));
            current_json_bytes = 2;
        }
        let comma = usize::from(!current.is_empty());
        current_json_bytes = current_json_bytes.saturating_add(encoded_len + comma);
        current_kind = Some(kind);
        current.push(record.clone());
    }
    if !current.is_empty() {
        batches.push(current);
    }
    for index in multi_indexes {
        batches.push(vec![sized[index].0.clone()]);
    }
    Ok(batches)
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
