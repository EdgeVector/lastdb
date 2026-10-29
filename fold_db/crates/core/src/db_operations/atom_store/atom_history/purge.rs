use super::*;
use crate::db_operations::atom_store::counter::{ReleaseRequest, TipChangeSet};
use crate::db_operations::atom_store::{AtomStore, RecordSlot};
use crate::schema::SchemaError;

impl AtomStore {
    /// One bounded page; the caller repeats while complete is false.
    pub async fn purge_atom_history(
        &self,
        slot: &str,
        cursor: Option<&str>,
    ) -> Result<serde_json::Value, SchemaError> {
        self.in_background_scope(async {
            let mut walk = history_walk(slot, cursor, 32)?;
            let rows = self.walk_step(&mut walk).await?;
            let mut releases = Vec::new();
            for (key, bytes) in rows.rows {
                let key = String::from_utf8(key).map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let record: ArchiveRecord = serde_json::from_slice(&bytes)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                releases.push(self.archive_release(&key, &record)?);
            }
            let scope = crate::storage::current_keys::exact_scope(slot)?;
            let commit = self.commit_tip_changes(TipChangeSet {
                scope, releases, ..TipChangeSet::default()
            }).await?;
            Ok(serde_json::json!({"released": commit.records_released, "complete": walk.done(), "cursor": next_cursor(&walk)}))
        }).await
    }

    pub(crate) fn archive_release(
        &self,
        key: &str,
        record: &ArchiveRecord,
    ) -> Result<ReleaseRequest, SchemaError> {
        let scope = crate::storage::current_keys::exact_scope(key)?;
        let slot = RecordSlot {
            molecule: &record.event.molecule_uuid,
            hash: record.event.field_key.hash.as_deref().unwrap_or(""),
            range: record.event.field_key.range.as_deref().unwrap_or(""),
        };
        let mut deletes =
            vec![
                super::super::retention::archive_dir_key(scope, &slot, &digest(key.as_bytes()))
                    .into_bytes(),
            ];
        if let Some(queue) = &record.archive.queue_key {
            deletes.push(queue.as_bytes().to_vec());
        }
        Ok(ReleaseRequest {
            history: key.to_string(),
            deletes,
        })
    }
}
