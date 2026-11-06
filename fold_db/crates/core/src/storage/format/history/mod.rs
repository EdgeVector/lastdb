//! Format-owned release of legacy history and its queue and count rows.
//!
//! Providers supply data and release intents, never raw keys/value mutations.
//! This component encodes them into a private batch. Both record writes and
//! maintenance use it inside the existing request and commit owner.

mod keys;
mod queue;
mod release;
mod retire;

pub(crate) use keys::{count_key, record_prefix};
pub(crate) use queue::TimeQueue;
pub(crate) use release::HistoryRelease;
pub(in crate::storage::format) use retire::retire_bucket;

use crate::records::field_history::RecordHistoryEntry;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::{KvMutation, KvStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct QueueEntry {
    pub history: String,
    pub queue: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct KeyCount {
    pub records: u64,
}

/// Data-only cleanup changes. Releases are minted from a validated entry.
pub(crate) enum HistoryChange {
    Release(HistoryRelease),
    Count { record: String, records: u64 },
    DeleteCount(String),
    OrphanQueue(QueueEntry),
}

pub(crate) struct EncodedHistory {
    rows: Vec<KvMutation>,
}

pub(crate) fn queue() -> TimeQueue {
    TimeQueue::with_slice(
        super::HISTORY_QUEUE_FAMILY,
        super::HISTORY_BUCKET_FAMILY,
        super::HISTORY_SLICE_NANOS,
    )
}

pub(crate) fn encode(changes: Vec<HistoryChange>) -> Result<EncodedHistory, SchemaError> {
    let mut rows = Vec::with_capacity(changes.len());
    for change in changes {
        match change {
            HistoryChange::Release(value) => release::encode(value, &mut rows),
            HistoryChange::Count { record, records } => {
                let key = keys::write_count_key(&record)?;
                rows.push(if records == 0 {
                    KvMutation::delete(key)
                } else {
                    KvMutation::put(
                        key,
                        serde_json::to_vec(&serde_json::json!(KeyCount { records }))
                            .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
                    )
                });
            }
            HistoryChange::DeleteCount(record) => {
                rows.push(KvMutation::delete(keys::write_count_key(&record)?));
            }
            HistoryChange::OrphanQueue(entry) => {
                release::require_queue_entry(&entry)?;
                rows.push(KvMutation::delete(entry.queue));
            }
        }
    }
    Ok(EncodedHistory { rows })
}

impl EncodedHistory {
    pub(in crate::storage::format) fn append_to(self, batch: &mut Vec<KvMutation>) {
        batch.extend(self.rows);
    }

    pub(in crate::storage::format) async fn stage(
        self,
        main_store: &dyn KvStore,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        if !transaction::publication::active(runtime) {
            return Err(invalid(
                "a history write requires a complete native write scope",
            ));
        }
        main_store.batch_mutate(self.rows).await?;
        Ok(())
    }
}

pub(crate) fn release(key: &str, entry: &RecordHistoryEntry) -> Result<HistoryChange, SchemaError> {
    release::prepare(key, entry).map(HistoryChange::Release)
}

fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("record history: {error}"))
}

pub(crate) fn digest(value: &[u8]) -> String {
    use sha2::Digest as _;
    lastdb_model::hex::hex_lower(sha2::Sha256::digest(value))
}
