use super::*;
use crate::db_operations::atom_store::counter::{ReleaseRequest, TipChangeSet};
use crate::db_operations::atom_store::retention::RetentionEntry;
use crate::db_operations::atom_store::time_queue::QueueReader;
use crate::db_operations::atom_store::{AtomStore, RecordSlot};
use crate::schema::SchemaError;
use serde_json::Value;
type ArchiveRows = (Option<String>, Vec<(String, Value)>);

impl AtomStore {
    pub(crate) fn archive_retention_rows(
        &self,
        scope: Option<&str>,
        key: &str,
        slot: &RecordSlot<'_>,
        created: u64,
        expires: Option<u64>,
    ) -> Result<ArchiveRows, SchemaError> {
        let id = digest(key.as_bytes());
        let dir = super::super::retention::archive_dir_key(scope, slot, &id);
        let queue_key = expires.map(|at| queue().entry_key(at, &id));
        let entry = RetentionEntry {
            version: 1,
            history: key.to_string(),
            conflict: None,
            dir: dir.clone(),
            queue: queue_key.clone().unwrap_or_default(),
            slot: crate::schema::types::field::build_storage_key(
                scope,
                &crate::atom::molecule_key_codec::hash_range_record_key(
                    slot.molecule,
                    slot.hash,
                    slot.range,
                ),
            ),
            created,
        };
        let value =
            serde_json::to_value(entry).map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        let mut rows = expires.map_or_else(Vec::new, |at| queue().rows(at, &id, &value));
        rows.push((dir, value));
        Ok((queue_key, rows))
    }

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
                    // A format 7 home keeps copies, not atom references: its
                    // entries release in their own batch.
                    let (records, rows): (Vec<_>, Vec<_>) = rows
                        .into_iter()
                        .flatten()
                        .partition(|entry| super::records::is_record_history(&entry.history));
                    let mut released = self.expire_record_history(records, now).await?;
                    let history_keys: Vec<_> = rows.iter().map(|e| e.history.clone()).collect();
                    let archived: Vec<Option<ArchiveRecord>> =
                        self.main_store.get_items(&history_keys).await?;
                    let mut archived = archived.into_iter();
                    let mut by_scope: std::collections::BTreeMap<
                        Option<String>,
                        Vec<ReleaseRequest>,
                    > = std::collections::BTreeMap::new();
                    for entry in rows {
                        if let Some(record) = archived.next().flatten() {
                            if !record.expired(now)
                                || record.archive.queue_key.as_deref() != Some(&entry.queue)
                            {
                                return Err(SchemaError::InvalidData(
                                    "history queue does not match immutable expiry".into(),
                                ));
                            }
                        }
                        if !is_archive(&entry.history) {
                            return Err(SchemaError::InvalidData(
                                "foreign history expiry entry".into(),
                            ));
                        }
                        by_scope
                            .entry(entry.scope()?.map(str::to_string))
                            .or_default()
                            .push(entry.request());
                    }
                    for (scope, releases) in by_scope {
                        released += self
                            .commit_tip_changes(TipChangeSet {
                                scope: scope.as_deref(),
                                releases,
                                ..TipChangeSet::default()
                            })
                            .await?
                            .records_released;
                    }
                    Ok(released)
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
