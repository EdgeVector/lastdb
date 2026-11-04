//! Release intents bind the history key and its queue pointer to one entry.

use super::{digest, invalid, keys, queue, QueueEntry};
use crate::records::field_history::RecordHistoryEntry;
use crate::schema::SchemaError;
use crate::storage::KvMutation;

pub(crate) struct HistoryRelease {
    key: String,
    queue: Option<String>,
}

pub(super) fn prepare(
    key: &str,
    entry: &RecordHistoryEntry,
) -> Result<HistoryRelease, SchemaError> {
    let expected = keys::entry_key(&entry.record, &entry.field_identity, &entry.stamp)?;
    if key != expected {
        return Err(invalid("the history key does not match its entry"));
    }
    let expected_queue = entry
        .expires_at
        .map(|at| queue().entry_key(at, &digest(key.as_bytes())));
    if entry.queue_key != expected_queue {
        return Err(invalid("the history queue does not match the entry expiry"));
    }
    Ok(HistoryRelease {
        key: expected,
        queue: expected_queue,
    })
}

pub(super) fn encode(value: HistoryRelease, rows: &mut Vec<KvMutation>) {
    rows.push(KvMutation::delete(value.key));
    if let Some(key) = value.queue {
        rows.push(KvMutation::delete(key));
    }
}

pub(super) fn require_queue_entry(entry: &QueueEntry) -> Result<(), SchemaError> {
    crate::storage::current_keys::validate(super::super::MAIN_NAMESPACE, entry.history.as_bytes())?;
    if crate::records::field_history::record_history_suffix(&entry.history).is_none() {
        return Err(invalid("the orphan queue does not name a history entry"));
    }
    let key = entry.queue.as_str();
    let Some(expires) = queue().entry_expiry(key) else {
        return Err(invalid("the orphan key is not a history queue entry"));
    };
    let id = key.rsplit(':').next().unwrap_or_default();
    if id != digest(entry.history.as_bytes()) || queue().entry_key(expires, id) != key {
        return Err(invalid("the orphan history queue key is malformed"));
    }
    Ok(())
}
