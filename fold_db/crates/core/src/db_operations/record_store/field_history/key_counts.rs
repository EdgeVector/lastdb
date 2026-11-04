//! A derived per-key count permits one batch presence check for history-only keys.
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use crate::storage::format::history::{count_key, HistoryChange, KeyCount};
use std::collections::BTreeMap;

impl RecordStore {
    /// The count rows after `deltas`, a change for each count key. A count
    /// that reaches zero is deleted; one that would go below zero is refused.
    async fn history_count_changes(
        &self,
        mut deltas: BTreeMap<String, (String, i64)>,
    ) -> Result<Vec<HistoryChange>, SchemaError> {
        deltas.retain(|_, (_, value)| *value != 0);
        let keys: Vec<_> = deltas.keys().cloned().collect();
        let current: Vec<Option<KeyCount>> = self.main_store.get_items(&keys).await?;
        if current.len() != keys.len() {
            return Err(SchemaError::InvalidData(
                "history count read batch is incomplete".into(),
            ));
        }
        let mut rows = Vec::new();
        let mut deletes = Vec::new();
        for ((_, (record, delta)), count) in deltas.into_iter().zip(current) {
            let count = count.map_or(0, |c| c.records);
            let count = if delta < 0 {
                count.checked_sub(delta.unsigned_abs())
            } else {
                count.checked_add(delta as u64)
            }
            .ok_or_else(|| SchemaError::InvalidData("history key count is inconsistent".into()))?;
            if count == 0 {
                deletes.push(HistoryChange::Count { record, records: 0 });
            } else {
                rows.push(HistoryChange::Count {
                    record,
                    records: count,
                });
            }
        }
        rows.extend(deletes);
        Ok(rows)
    }

    /// The count mutations of the field history of records: `deltas`
    /// maps the storage key of a record to the change of its entry count.
    pub(crate) async fn record_history_count_changes(
        &self,
        deltas: BTreeMap<String, i64>,
    ) -> Result<Vec<HistoryChange>, SchemaError> {
        let mut keyed = BTreeMap::<String, (String, i64)>::new();
        for (record, delta) in deltas {
            keyed.entry(count_key(&record)).or_insert((record, 0)).1 += delta;
        }
        self.history_count_changes(keyed).await
    }

    /// The history entry count of each record, zero when it has none.
    pub(crate) async fn record_history_counts(
        &self,
        records: &[&str],
    ) -> Result<Vec<u64>, SchemaError> {
        let keys: Vec<String> = records.iter().map(|record| count_key(record)).collect();
        let rows: Vec<Option<KeyCount>> = self.main_store.get_items(&keys).await?;
        Ok(rows
            .into_iter()
            .map(|count| count.map_or(0, |count| count.records))
            .collect())
    }
}
