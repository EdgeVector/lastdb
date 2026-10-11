use super::*;

/// Validate one transaction-group shard or manifest object and file it under
/// its group. The typed and raw wire forms must describe the same object.
pub(super) fn stage_transaction_group_object(
    object: &MutationLogSegment,
    wire: TransactionGroupWireV2,
    raw_wire: TransactionGroupWireV2Raw,
    groups: &mut BTreeMap<String, TransactionGroupAssembly>,
) -> Result<(), String> {
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
        ) if wire_type == "shard" => stage_transaction_group_shard(
            object,
            groups,
            (format_version, group_id, writer_id, frontier_after),
            (schema_name, shard_index, shard_count),
            (operations, raw_operations),
        ),
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
        ) if wire_type == "manifest" => stage_transaction_group_manifest(
            object,
            groups,
            (format_version, &group_id, writer_id, frontier_after),
            (
                record_digest_version,
                record_sha256,
                operation_count,
                shard_count,
            ),
            (record_template, raw_record_template, shards),
        ),
        _ => Err(format!(
            "transaction group typed and raw wire forms disagree for {}",
            object.segment.object_key
        )),
    }
}

/// Validate one transaction-group shard object and file it under its group.
fn stage_transaction_group_shard(
    object: &MutationLogSegment,
    groups: &mut BTreeMap<String, TransactionGroupAssembly>,
    (format_version, group_id, writer_id, frontier_after): (u32, String, String, u64),
    (schema_name, shard_index, shard_count): (String, u32, u32),
    (operations, raw_operations): (
        Vec<TransactionGroupIndexedMutation>,
        Vec<TransactionGroupRawIndexedMutation>,
    ),
) -> Result<(), String> {
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
    if !transaction_group_shard_matches_operations(
        object,
        &group_id,
        &schema_name,
        &operations,
        &raw_operations,
    ) {
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
    Ok(())
}

/// Validate one transaction-group manifest object and file it under its group.
fn stage_transaction_group_manifest(
    object: &MutationLogSegment,
    groups: &mut BTreeMap<String, TransactionGroupAssembly>,
    (format_version, group_id, writer_id, frontier_after): (u32, &str, String, u64),
    (record_digest_version, record_sha256, operation_count, shard_count): (u32, String, u32, u32),
    (record_template, raw_record_template, shards): (
        Box<PinLogRecord>,
        Box<RawValue>,
        Vec<TransactionGroupShardRef>,
    ),
) -> Result<(), String> {
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
    let assembly = groups.entry(group_id.to_string()).or_default();
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

    Ok(())
}
