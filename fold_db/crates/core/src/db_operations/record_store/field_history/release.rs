use crate::db_operations::record_store::retention::RetentionEntry;
use crate::db_operations::record_store::time_queue::QueueReader;
use crate::db_operations::record_store::RecordStore;
use crate::records::field_history::RecordHistoryEntry;
use crate::schema::SchemaError;
use crate::storage::KvMutation;

impl RecordStore {
    /// One bounded cleanup step. A failed step never advances the cursor.
    pub(crate) async fn field_history_expire_step(
        &self,
        reader: &mut QueueReader,
    ) -> Result<u64, SchemaError> {
        if !self.field_history_enabled().await? {
            return Ok(0);
        }
        let now = self.retention_state().clock_nanos();
        reader.refresh(self, now).await?;
        if let Some(bucket) = reader.candidates(now).into_iter().next() {
            let page = reader.read_due(self, bucket, now).await?;
            let keys: Vec<_> = page.rows.iter().map(|(k, _)| k.clone()).collect();
            let released = self
                .in_background_scope(async {
                    let rows = self.main_store.get_items::<RetentionEntry>(&keys).await?;
                    let records: Vec<RetentionEntry> = rows.into_iter().flatten().collect();
                    if records
                        .iter()
                        .any(|entry| !super::records::is_record_history(&entry.history))
                    {
                        return Err(SchemaError::InvalidData(
                            "foreign history expiry entry".into(),
                        ));
                    }
                    self.expire_record_history(records, now).await
                })
                .await?;
            reader.finish(page);
            if reader.exhausted(bucket) {
                if let Some((key, token)) = reader.retire_token(bucket) {
                    self.retire_bucket(key, token).await?;
                }
                reader.forget(bucket);
            }
            return Ok(released);
        }
        Ok(0)
    }

    /// Release field history rows of a dropped schema in one batch: each
    /// entry, its queue entry and its blob references. The drop releases every
    /// entry of the schema, so the entry count of each named record is deleted,
    /// not lowered: an entry of the record in a later batch deletes the gone
    /// count again, and a count that drifted cannot stop the drop. The caller
    /// read the rows by key in its write scope. A row that does not decode
    /// names no record or queue entry: it is deleted, and its blob counters
    /// stay too high until a recount.
    pub(crate) async fn release_dropped_history(
        &self,
        rows: &[(String, Vec<u8>)],
    ) -> Result<u64, SchemaError> {
        let mut out = super::records::RecordHistoryRows::default();
        let mut unreadable = 0usize;
        for (key, bytes) in rows {
            if let Ok(entry) = serde_json::from_slice::<RecordHistoryEntry>(bytes) {
                super::records::release(key, &entry, &mut out);
            } else {
                unreadable += 1;
                out.rows.push(KvMutation::delete(key.as_bytes().to_vec()));
            }
        }
        if unreadable > 0 {
            tracing::warn!(
                target: "fold_node::database",
                unreadable,
                "field history rows of a dropped schema do not decode; their blob counters stay \
                 too high until a recount"
            );
        }
        if out.rows.is_empty() {
            return Ok(0);
        }
        let mut batch = out.rows;
        batch.extend(super::key_counts::history_count_deletes(out.counts.keys())?);
        batch.extend(self.blob_counter_mutations(out.blobs).await?);
        self.main_store.inner().batch_mutate(batch).await?;
        Ok(out.released + unreadable as u64)
    }
}
