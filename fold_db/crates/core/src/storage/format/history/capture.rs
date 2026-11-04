//! Encode an old field value and compose its optional expiry queue rows.

use super::{digest, invalid, keys, queue, QueueEntry};
use crate::records::field_history::{FieldHistoryPolicy, RecordHistoryEntry};
use crate::schema::SchemaError;
use crate::storage::format::MAX_HISTORY_ENTRY_BYTES;
use crate::storage::KvMutation;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

pub(crate) struct HistoryCapture {
    pub schema: String,
    pub record: String,
    pub field: String,
    pub identity: String,
    pub source: Arc<StoredRecord>,
    pub policy: FieldHistoryPolicy,
    pub now: u64,
    pub replaced: bool,
}

pub(super) fn encode(value: HistoryCapture, rows: &mut Vec<KvMutation>) -> Result<(), SchemaError> {
    super::policy::validate_policy(&value.identity, value.policy)?;
    if value.policy == FieldHistoryPolicy::Off {
        return Err(invalid("a disabled history policy cannot capture a value"));
    }
    let old = value
        .source
        .fields
        .get(&value.field)
        .ok_or_else(|| invalid("the capture source has no field value"))?;
    let stamp = value
        .source
        .stamps
        .get(&value.field)
        .ok_or_else(|| invalid("the capture source has no field stamp"))?;
    let key = keys::entry_key(&value.record, &value.identity, stamp)?;
    let expires_at = expiry(value.policy, value.now)?;
    let queue_key = match expires_at {
        Some(at) => {
            let id = digest(key.as_bytes());
            let queue_key = queue().entry_key(at, &id);
            let queued = serde_json::to_value(QueueEntry {
                history: key.clone(),
                queue: queue_key.clone(),
            })
            .map_err(invalid)?;
            for (key, value) in queue().rows(at, &id, &queued) {
                rows.push(KvMutation::put(
                    key,
                    serde_json::to_vec(&value).map_err(invalid)?,
                ));
            }
            Some(queue_key)
        }
        None => None,
    };
    let entry = RecordHistoryEntry {
        v: RecordHistoryEntry::VERSION,
        schema: value.schema,
        field: value.field,
        field_identity: value.identity,
        record: value.record,
        value: old.clone(),
        stamp: stamp.clone(),
        archived_at: value.now,
        expires_at,
        reason: if value.replaced { "replace" } else { "delete" }.into(),
        queue_key,
    };
    let bytes = serde_json::to_vec(&entry).map_err(invalid)?;
    if bytes.len() > MAX_HISTORY_ENTRY_BYTES {
        return Err(SchemaError::RecordTooLarge {
            size: bytes.len(),
            limit: MAX_HISTORY_ENTRY_BYTES,
        });
    }
    rows.push(KvMutation::put(key, bytes));
    Ok(())
}

fn expiry(policy: FieldHistoryPolicy, now: u64) -> Result<Option<u64>, SchemaError> {
    Ok(match policy {
        FieldHistoryPolicy::Ttl { seconds } => Some(
            now.checked_add(
                seconds
                    .checked_mul(1_000_000_000)
                    .ok_or_else(|| SchemaError::InvalidData("history TTL overflow".into()))?,
            )
            .ok_or_else(|| SchemaError::InvalidData("history expiry overflow".into()))?,
        ),
        _ => None,
    })
}
