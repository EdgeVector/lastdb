//! The reads and the purge of the field history of a format 7 home: one page
//! of the entries of a record field, one entry with its copied value, and one
//! bounded release (`POST /api/atom-history`).

use super::records::{decode_row, record_prefix};
use super::*;
use crate::db_operations::atom_store::AtomStore;
use crate::records::atom_history::RecordHistoryEntry;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};
use serde_json::{json, Value};

impl AtomStore {
    /// The storage key of a record and the history prefix of one field.
    fn record_history_location(
        &self,
        schema: &Schema,
        key: &KeyValue,
        field_identity: &str,
        scope: Option<&str>,
    ) -> Result<(String, String), SchemaError> {
        let record = self.record_key(schema, key, scope)?;
        let prefix = record_prefix(&record, Some(field_identity))?;
        Ok((record, prefix))
    }

    /// One page of the field history of one record, oldest stamp identity
    /// first. `current` names the stamp of the live value.
    pub async fn record_history_page(
        &self,
        schema: &Schema,
        key: &KeyValue,
        (field, field_identity): (&str, &str),
        scope: Option<&str>,
        (cursor, limit): (Option<&str>, usize),
    ) -> Result<AtomHistoryPage, SchemaError> {
        let (record, prefix) = self.record_history_location(schema, key, field_identity, scope)?;
        self.atom_read_scope(async {
            let mut walk = prefix_walk(&prefix, cursor, limit)?;
            let step = self.walk_step(&mut walk).await?;
            let now = self.retention_state().clock_nanos();
            let current = self
                .read_records(&[record])
                .await?
                .into_iter()
                .flatten()
                .find_map(|record| {
                    record.fields.contains_key(field).then(|| {
                        record.stamps.get(field).map(|stamp| {
                            json!({"mutation": stamp.mutation_id, "written_at": stamp.written_at})
                        })
                    })?
                });
            let mut entries = Vec::new();
            for (key, bytes) in step.rows {
                let (key, entry) = decode_row(key, &bytes)?;
                if !entry.expired(now) {
                    entries.push(entry.entry(&key));
                }
            }
            Ok(AtomHistoryPage {
                current,
                entries,
                cursor: next_cursor(&walk),
            })
        })
        .await
    }

    /// One entry with its copied value. `None` when it is absent or expired.
    pub async fn record_history_value(
        &self,
        schema: &Schema,
        key: &KeyValue,
        field_identity: &str,
        scope: Option<&str>,
        id: &str,
    ) -> Result<Option<Value>, SchemaError> {
        super::read::validate_id(id)?;
        let (_, prefix) = self.record_history_location(schema, key, field_identity, scope)?;
        self.atom_read_scope(async {
            let key = format!("{prefix}{id}");
            let Some(entry) = self.main_store.get_item::<RecordHistoryEntry>(&key).await? else {
                return Ok(None);
            };
            if entry.expired(self.retention_state().clock_nanos()) {
                return Ok(None);
            }
            Ok(Some(json!({
                "entry": entry.entry(&key), "value": entry.value, "metadata": Value::Null,
            })))
        })
        .await
    }

    /// Release one bounded page of the field history of one record. The
    /// caller repeats while `complete` is false.
    pub async fn purge_record_history(
        &self,
        schema: &Schema,
        key: &KeyValue,
        field_identity: &str,
        scope: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<Value, SchemaError> {
        let (_, prefix) = self.record_history_location(schema, key, field_identity, scope)?;
        self.in_background_scope(async move {
            let mut walk = prefix_walk(&prefix, cursor, 32)?;
            let mut entries = Vec::new();
            for (key, bytes) in self.walk_step(&mut walk).await?.rows {
                entries.push(decode_row(key, &bytes)?);
            }
            let released = self.commit_history_release(&entries, Vec::new()).await?;
            Ok(json!({"released": released, "complete": walk.done(), "cursor": next_cursor(&walk)}))
        })
        .await
    }
}
