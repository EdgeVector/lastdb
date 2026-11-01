//! A derived per-key count permits one batch presence check for history-only keys.
use super::*;
use crate::db_operations::atom_store::{counter::RecordRefs, AtomStore};
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
struct KeyCount {
    version: u8,
    records: u64,
}
type CountRows = (Vec<(String, Value)>, Vec<Vec<u8>>);

fn count_key(slot: &str) -> Result<String, SchemaError> {
    let scope = crate::storage::current_keys::exact_scope(slot)?;
    Ok(crate::schema::types::field::build_storage_key(
        scope,
        &format!("ahk:v1:{}", digest(slot.as_bytes())),
    ))
}

impl AtomStore {
    pub(crate) async fn history_key_rows(
        &self,
        records: &[RecordRefs],
        new_records: &[bool],
        released: &[String],
    ) -> Result<CountRows, SchemaError> {
        let mut deltas = BTreeMap::<String, i64>::new();
        for (record, is_new) in records.iter().zip(new_records) {
            if *is_new {
                if let Some(slot) = &record.history_slot {
                    *deltas.entry(count_key(slot)?).or_default() += 1;
                }
            }
        }
        for slot in released {
            *deltas.entry(count_key(slot)?).or_default() -= 1;
        }
        self.history_count_rows(deltas).await
    }

    /// The count rows after `deltas`, a change for each count key. A count
    /// that reaches zero is deleted; one that would go below zero is refused.
    async fn history_count_rows(
        &self,
        mut deltas: BTreeMap<String, i64>,
    ) -> Result<CountRows, SchemaError> {
        deltas.retain(|_, value| *value != 0);
        let keys: Vec<_> = deltas.keys().cloned().collect();
        let current: Vec<Option<KeyCount>> = self.main_store.get_items(&keys).await?;
        let mut rows = Vec::new();
        let mut deletes = Vec::new();
        for ((key, delta), count) in deltas.into_iter().zip(current) {
            let count = count.map_or(0, |c| c.records);
            let count = if delta < 0 {
                count.checked_sub(delta.unsigned_abs())
            } else {
                count.checked_add(delta as u64)
            }
            .ok_or_else(|| SchemaError::InvalidData("history key count is inconsistent".into()))?;
            if count == 0 {
                deletes.push(key.into_bytes());
            } else {
                rows.push((
                    key,
                    serde_json::json!(KeyCount {
                        version: 1,
                        records: count
                    }),
                ));
            }
        }
        Ok((rows, deletes))
    }

    /// The count mutations of the field history of format 7 records: `deltas`
    /// maps the storage key of a record to the change of its entry count.
    pub(crate) async fn record_history_count_mutations(
        &self,
        deltas: BTreeMap<String, i64>,
    ) -> Result<Vec<crate::storage::KvMutation>, SchemaError> {
        let mut keyed = BTreeMap::<String, i64>::new();
        for (record, delta) in deltas {
            *keyed.entry(count_key(&record)?).or_default() += delta;
        }
        let (rows, deletes) = self.history_count_rows(keyed).await?;
        let mut mutations = Vec::with_capacity(rows.len() + deletes.len());
        for (key, value) in rows {
            let bytes =
                serde_json::to_vec(&value).map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            mutations.push(crate::storage::KvMutation::put(key.into_bytes(), bytes));
        }
        mutations.extend(deletes.into_iter().map(crate::storage::KvMutation::delete));
        Ok(mutations)
    }

    /// The history entry count of each format 7 record, zero when it has none.
    pub(crate) async fn record_history_counts(
        &self,
        records: &[&str],
    ) -> Result<Vec<u64>, SchemaError> {
        let keys = records
            .iter()
            .map(|record| count_key(record))
            .collect::<Result<Vec<_>, _>>()?;
        let rows: Vec<Option<KeyCount>> = self.main_store.get_items(&keys).await?;
        Ok(rows
            .into_iter()
            .map(|count| count.map_or(0, |count| count.records))
            .collect())
    }

    pub(crate) async fn atom_history_presence(
        &self,
        slots: &[String],
    ) -> Result<Vec<bool>, SchemaError> {
        let keys = slots
            .iter()
            .map(|s| count_key(s))
            .collect::<Result<Vec<_>, _>>()?;
        let rows: Vec<Option<KeyCount>> = self.main_store.get_items(&keys).await?;
        Ok(rows
            .into_iter()
            .map(|c| c.is_some_and(|c| c.records > 0))
            .collect())
    }
}
