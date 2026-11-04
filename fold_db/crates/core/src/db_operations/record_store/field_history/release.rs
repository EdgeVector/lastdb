use super::records::QueueEntry;
use crate::db_operations::record_store::time_queue::QueueReader;
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;

impl RecordStore {
    /// One bounded cleanup step. A failed step never advances the cursor.
    pub(crate) async fn field_history_expire_step(
        &self,
        reader: &mut QueueReader,
    ) -> Result<u64, SchemaError> {
        if !self.field_history_enabled().await? {
            return Ok(0);
        }
        let now = self.queue_clock().now_nanos();
        reader.refresh(self, now).await?;
        if let Some(bucket) = reader.candidates(now).into_iter().next() {
            let page = reader.read_due(self, bucket, now).await?;
            let keys: Vec<_> = page.rows.iter().map(|(k, _)| k.clone()).collect();
            let released = self
                .in_background_scope(async {
                    let rows = self.main_store.get_items::<QueueEntry>(&keys).await?;
                    let records: Vec<QueueEntry> = rows.into_iter().flatten().collect();
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
}
