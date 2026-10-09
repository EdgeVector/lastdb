use super::*;

pub(super) const TRANSACTION_GROUP_WIRE_VERSION: u32 = 2;
pub(super) const TRANSACTION_GROUP_MANIFEST_SCHEMA: &str = "__lastdb_transaction_group_v2__";
pub(super) const TRANSACTION_GROUP_RECORD_DIGEST_LEGACY_JSON: u32 = 0;
pub(super) const TRANSACTION_GROUP_RECORD_DIGEST_SORTED_JSON_V1: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct TransactionGroupIndexedMutation {
    pub(super) original_index: u32,
    pub(super) mutation: crate::sync::log::MutationEnvelope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct TransactionGroupShardRef {
    pub(super) schema_name: String,
    pub(super) shard_index: u32,
    pub(super) object_key: String,
    pub(super) ciphertext_sha256: String,
    pub(super) operation_indexes: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "wire_type", rename_all = "snake_case")]
pub(super) enum TransactionGroupWireV2 {
    Shard {
        format_version: u32,
        group_id: String,
        writer_id: String,
        frontier_after: u64,
        schema_name: String,
        shard_index: u32,
        shard_count: u32,
        operations: Vec<TransactionGroupIndexedMutation>,
    },
    Manifest {
        format_version: u32,
        group_id: String,
        writer_id: String,
        frontier_after: u64,
        /// Zero or absent identifies the historical raw-JSON digest.
        /// Version one sorts every JSON object key before it hashes the record.
        #[serde(default)]
        record_digest_version: u32,
        record_sha256: String,
        operation_count: u32,
        shard_count: u32,
        record_template: Box<PinLogRecord>,
        shards: Vec<TransactionGroupShardRef>,
    },
}

/// Raw fragments required to verify historical v2 record digests.
///
/// The old digest covered `HashMap` iteration order. Typed deserialization
/// creates new maps with a new order. Each shard still holds the exact JSON
/// emitted from the source record, so retain those bytes for the legacy check.
#[derive(Debug, Deserialize)]
pub(super) struct TransactionGroupRawIndexedMutation {
    pub(super) original_index: u32,
    pub(super) mutation: Box<RawValue>,
}

#[derive(Debug, Deserialize)]
pub(super) struct TransactionGroupWireV2Raw {
    pub(super) wire_type: String,
    #[serde(default)]
    pub(super) operations: Option<Vec<TransactionGroupRawIndexedMutation>>,
    #[serde(default)]
    pub(super) record_template: Option<Box<RawValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MutationLogRecordStream {
    Legacy,
    SingleSchema(String),
    MultiSchema,
}

pub(super) fn mutation_log_record_schemas(
    record: &PinLogRecord,
) -> Result<Option<(Vec<&str>, u64)>, String> {
    let LogOp::MutationIntent { mutations } = &record.entry.op else {
        return Ok(None);
    };
    if mutations.is_empty() {
        return Err("mutation-log intent has no mutations".to_string());
    }
    let mut schemas = BTreeSet::new();
    for mutation in mutations {
        let schema = mutation.schema_name.trim();
        if schema.is_empty() {
            return Err("mutation-log record cannot omit schema names".to_string());
        }
        schemas.insert(schema);
    }
    let utc_nanos = mutations
        .iter()
        .map(|mutation| mutation.written_at)
        .max()
        .filter(|value| *value > 0)
        .ok_or_else(|| "mutation-log record has no positive T0".to_string())?;
    Ok(Some((schemas.into_iter().collect(), utc_nanos)))
}

pub(super) fn mutation_log_record_stream(
    record: &PinLogRecord,
) -> Result<MutationLogRecordStream, String> {
    match mutation_log_record_schemas(record)? {
        None => Ok(MutationLogRecordStream::Legacy),
        Some((schemas, _)) if schemas.len() == 1 => Ok(MutationLogRecordStream::SingleSchema(
            schemas[0].to_string(),
        )),
        Some(_) => Ok(MutationLogRecordStream::MultiSchema),
    }
}

pub(super) fn mutation_log_record_identity(
    record: &PinLogRecord,
) -> Result<Option<(&str, u64)>, String> {
    let Some((schemas, utc_nanos)) = mutation_log_record_schemas(record)? else {
        return Ok(None);
    };
    if schemas.len() != 1 {
        return Err("mutation-log record has more than one schema identity".to_string());
    }
    Ok(Some((schemas[0], utc_nanos)))
}

pub(super) fn mutation_log_batch_identity(
    records: &[PinLogRecord],
) -> Result<Option<(&str, u64)>, String> {
    let mut identity: Option<(&str, u64)> = None;
    let mut saw_legacy = false;
    for record in records {
        let record_identity = mutation_log_record_identity(record)?;
        if let Some((schema, utc_nanos)) = record_identity {
            if saw_legacy {
                return Err("mutation log segment cannot mix typed and legacy records".to_string());
            }
            match identity {
                Some((current_schema, _)) if current_schema != schema => {
                    return Err("mutation log segment cannot mix schema streams".to_string());
                }
                Some((_, current_t0)) => identity = Some((schema, current_t0.max(utc_nanos))),
                None => identity = Some((schema, utc_nanos)),
            }
        } else {
            if identity.is_some() {
                return Err("mutation log segment cannot mix typed and legacy records".to_string());
            }
            saw_legacy = true;
        }
    }
    Ok(identity)
}

pub(super) fn sort_json_object_keys(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            for (_, child) in &mut entries {
                sort_json_object_keys(child);
            }
            object.extend(entries);
        }
        serde_json::Value::Array(values) => {
            for child in values {
                sort_json_object_keys(child);
            }
        }
        _ => {}
    }
}

pub(super) fn canonical_transaction_group_record_json(
    record: &PinLogRecord,
) -> Result<Vec<u8>, String> {
    let mut value = serde_json::to_value(record)
        .map_err(|error| format!("encode canonical transaction group record: {error}"))?;
    sort_json_object_keys(&mut value);
    serde_json::to_vec(&value)
        .map_err(|error| format!("serialize canonical transaction group record: {error}"))
}

pub(super) fn transaction_group_record_sha256(record: &PinLogRecord) -> Result<String, String> {
    canonical_transaction_group_record_json(record).map(|json| sha256_hex(&json))
}

pub(super) fn transaction_group_id(
    record: &PinLogRecord,
    record_digest_version: u32,
    record_sha256: &str,
) -> String {
    if record_digest_version == TRANSACTION_GROUP_RECORD_DIGEST_LEGACY_JSON {
        let mut group_seed = Vec::new();
        group_seed.extend_from_slice(b"lastdb-transaction-group-v2\0");
        group_seed.extend_from_slice(record.target_id.as_bytes());
        group_seed.push(0);
        group_seed.extend_from_slice(record.writer_id.as_bytes());
        group_seed.extend_from_slice(&record.frontier_after.to_be_bytes());
        group_seed.extend_from_slice(record_sha256.as_bytes());
        return sha256_hex(&group_seed);
    }

    let group_seed = crate::canonical::CanonicalWriter::new()
        .field(b"lastdb-transaction-group-v2-versioned-record-digest")
        .u64(u64::from(record_digest_version))
        .field(record.target_id.as_bytes())
        .field(record.writer_id.as_bytes())
        .u64(record.frontier_after)
        .field(record_sha256.as_bytes())
        .finish();
    sha256_hex(&group_seed)
}

pub(super) fn legacy_transaction_group_record_json(
    record_template: &RawValue,
    mutations: &[Box<RawValue>],
) -> Result<Vec<u8>, String> {
    const EMPTY_MUTATIONS: &[u8] = br#""MutationIntent":{"mutations":[]}"#;

    let template = record_template.get().as_bytes();
    let matches = template
        .windows(EMPTY_MUTATIONS.len())
        .enumerate()
        .filter_map(|(offset, value)| (value == EMPTY_MUTATIONS).then_some(offset))
        .collect::<Vec<_>>();
    let [match_offset] = matches.as_slice() else {
        return Err(
            "legacy transaction group template does not contain one empty MutationIntent"
                .to_string(),
        );
    };
    let array_offset = match_offset + EMPTY_MUTATIONS.len() - 3;
    let mutation_bytes = mutations
        .iter()
        .map(|mutation| mutation.get().len())
        .sum::<usize>();
    let mut record_json =
        Vec::with_capacity(template.len() + mutation_bytes + mutations.len().saturating_sub(1));
    record_json.extend_from_slice(&template[..array_offset]);
    record_json.push(b'[');
    for (index, mutation) in mutations.iter().enumerate() {
        if index > 0 {
            record_json.push(b',');
        }
        record_json.extend_from_slice(mutation.get().as_bytes());
    }
    record_json.push(b']');
    record_json.extend_from_slice(&template[array_offset + 2..]);
    Ok(record_json)
}

#[derive(Debug)]
pub(super) struct MutationLogReplayUnit {
    pub(super) segment: MutationLogSegmentId,
    pub(super) records: Vec<PinLogRecord>,
    pub(super) transaction_group: bool,
}

#[derive(Debug)]
pub(super) struct TransactionGroupShardObject {
    pub(super) segment: MutationLogSegmentId,
    pub(super) ciphertext_sha256: String,
    pub(super) writer_id: String,
    pub(super) frontier_after: u64,
    pub(super) schema_name: String,
    pub(super) shard_index: u32,
    pub(super) shard_count: u32,
    pub(super) operations: Vec<TransactionGroupIndexedMutation>,
    pub(super) raw_operations: Vec<TransactionGroupRawIndexedMutation>,
}

#[derive(Debug)]
pub(super) struct TransactionGroupManifestObject {
    pub(super) segment: MutationLogSegmentId,
    pub(super) writer_id: String,
    pub(super) frontier_after: u64,
    pub(super) record_digest_version: u32,
    pub(super) record_sha256: String,
    pub(super) operation_count: u32,
    pub(super) shard_count: u32,
    pub(super) record_template: Box<PinLogRecord>,
    pub(super) raw_record_template: Box<RawValue>,
    pub(super) shards: Vec<TransactionGroupShardRef>,
}

#[derive(Debug, Default)]
pub(super) struct TransactionGroupAssembly {
    pub(super) manifest: Option<TransactionGroupManifestObject>,
    pub(super) shards: Vec<TransactionGroupShardObject>,
}

pub(super) fn decode_legacy_mutation_log_records(json: &[u8]) -> Result<Vec<PinLogRecord>, String> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WirePayload {
        Batch(Vec<PinLogRecord>),
        LegacySingle(Box<PinLogRecord>),
    }

    let records = match serde_json::from_slice(json)
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

pub(super) fn validate_transaction_group_segment_identity(
    segment: &MutationLogSegmentId,
    writer_id: &str,
    schema_name: &str,
    frontier_after: u64,
) -> Result<(), String> {
    if segment.writer_id.as_deref() != Some(writer_id)
        || segment.schema_name.as_deref() != Some(schema_name)
        || segment.sequence != Some(frontier_after)
        || segment.through_id != frontier_after
        || segment.object_key != segment.expected_object_key()
    {
        return Err(format!(
            "transaction group object identity mismatch for {}",
            segment.object_key
        ));
    }
    Ok(())
}
