//! Shard and manifest sealing for a transaction group.

use super::*;

/// Seal one encrypted shard object per schema; returns the objects and the manifest's shard refs.
pub(super) async fn seal_transaction_group_shards(
    record: &PinLogRecord,
    crypto: &Arc<dyn CryptoProvider>,
    by_schema: BTreeMap<String, Vec<TransactionGroupIndexedMutation>>,
    group_id: &str,
    writer: &str,
    shard_count: u32,
) -> Result<(Vec<MutationLogSegment>, Vec<TransactionGroupShardRef>), String> {
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
            group_id: group_id.to_string(),
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
    Ok((objects, shard_refs))
}

/// Seal the manifest and append it after the shard objects.
pub(super) async fn push_transaction_group_manifest(
    objects: &mut Vec<MutationLogSegment>,
    record: &PinLogRecord,
    crypto: &Arc<dyn CryptoProvider>,
    writer: &str,
    utc_nanos: u64,
    manifest_wire: TransactionGroupWireV2,
) -> Result<(), String> {
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
    Ok(())
}
