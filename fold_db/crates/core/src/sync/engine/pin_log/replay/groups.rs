use super::*;

pub(in super::super) async fn open_mutation_log_replay_units(
    segments: &[MutationLogSegment],
    crypto: &Arc<dyn CryptoProvider>,
) -> Result<Vec<MutationLogReplayUnit>, String> {
    let mut units = Vec::new();
    let mut groups = BTreeMap::<String, TransactionGroupAssembly>::new();

    for object in segments {
        let json = open_mutation_log_json(&object.payload, crypto).await?;
        let Ok(wire) = serde_json::from_slice::<TransactionGroupWireV2>(&json) else {
            units.push(MutationLogReplayUnit {
                segment: object.segment.clone(),
                records: decode_legacy_mutation_log_records(&json)?,
                transaction_group: false,
                source_object_keys: vec![object.segment.object_key.clone()],
            });
            continue;
        };
        let raw_wire = serde_json::from_slice::<TransactionGroupWireV2Raw>(&json)
            .map_err(|error| format!("decode raw transaction group wire: {error}"))?;
        stage_transaction_group_object(object, wire, raw_wire, &mut groups)?;
    }

    for (group_id, assembly) in groups {
        units.push(assemble_transaction_group(&group_id, assembly)?);
    }
    Ok(units)
}

/// True when a shard's operations agree with its declared identity, its
/// segment T0, and its raw (digest-preserving) operation list.
pub(super) fn transaction_group_shard_matches_operations(
    object: &MutationLogSegment,
    group_id: &str,
    schema_name: &str,
    operations: &[TransactionGroupIndexedMutation],
    raw_operations: &[TransactionGroupRawIndexedMutation],
) -> bool {
    !(group_id.is_empty()
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
            .zip(raw_operations)
            .any(|(operation, raw)| operation.original_index != raw.original_index))
}

/// Check a group's shards against its commit manifest and rebuild the single
/// `MutationIntent` record the group was split from.
fn assemble_transaction_group(
    group_id: &str,
    mut assembly: TransactionGroupAssembly,
) -> Result<MutationLogReplayUnit, String> {
    let manifest = assembly
        .manifest
        .ok_or_else(|| format!("transaction group {group_id} has shards but no commit manifest"))?;
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
    let mut source_object_keys: Vec<_> = assembly
        .shards
        .iter()
        .map(|shard| shard.segment.object_key.clone())
        .collect();
    source_object_keys.push(manifest.segment.object_key.clone());
    let (mutations, raw_mutations) =
        collect_group_operations(group_id, &manifest, assembly.shards)?;
    let record = rebuild_group_record(group_id, &manifest, mutations, &raw_mutations)?;
    Ok(MutationLogReplayUnit {
        segment: manifest.segment,
        records: vec![record],
        transaction_group: true,
        source_object_keys,
    })
}

/// Place every shard operation (typed and raw) at its original index,
/// rejecting inconsistent, repeated, out-of-range, or missing operations.
fn collect_group_operations(
    group_id: &str,
    manifest: &TransactionGroupManifestObject,
    shards: Vec<TransactionGroupShardObject>,
) -> Result<(Vec<crate::sync::log::MutationEnvelope>, Vec<Box<RawValue>>), String> {
    let mut operations = vec![None; manifest.operation_count as usize];
    let mut raw_operations = std::iter::repeat_with(|| None)
        .take(manifest.operation_count as usize)
        .collect::<Vec<Option<Box<RawValue>>>>();
    let mut seen_shards = BTreeSet::new();
    for shard in shards {
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
        for (operation, raw_operation) in shard.operations.into_iter().zip(shard.raw_operations) {
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
    Ok((mutations, raw_mutations))
}

/// Fill the manifest's record template with the collected mutations and
/// verify its T0, content hash, and stable group identity.
fn rebuild_group_record(
    group_id: &str,
    manifest: &TransactionGroupManifestObject,
    mutations: Vec<crate::sync::log::MutationEnvelope>,
    raw_mutations: &[Box<RawValue>],
) -> Result<PinLogRecord, String> {
    let mut record = (*manifest.record_template).clone();
    if record.writer_id != manifest.writer_id || record.frontier_after != manifest.frontier_after {
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
                raw_mutations,
            )?;
            sha256_hex(&record_json)
        }
        TRANSACTION_GROUP_RECORD_DIGEST_SORTED_JSON_V1 => transaction_group_record_sha256(&record)?,
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
    Ok(record)
}
