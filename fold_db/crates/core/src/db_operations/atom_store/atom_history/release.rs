use crate::db_operations::atom_store::retention::RetentionEntry;
use crate::db_operations::atom_store::time_queue::QueueReader;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;

impl AtomStore {
    /// One bounded cleanup step. A failed step never advances the cursor.
    pub(crate) async fn atom_history_expire_step(
        &self,
        reader: &mut QueueReader,
    ) -> Result<u64, SchemaError> {
        if !self.atom_history_enabled().await? {
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
}
