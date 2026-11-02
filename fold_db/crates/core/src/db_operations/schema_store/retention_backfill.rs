//! Explicit, bounded maintenance for keys that predate a retention policy.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::types::KeyValue;
use crate::schema::{SchemaError, SchemaRetentionPolicy};
use serde_json::Value;
use std::collections::HashMap;

impl SchemaStore {
    /// The caller holds the native write scope across the record reads and
    /// these writes. Existing age rows are corrected to the record write times.
    pub(crate) async fn backfill_retention_rows(
        &self,
        name: &str,
        clocks: Vec<(KeyValue, u64)>,
        range_keyed: bool,
    ) -> Result<usize, SchemaError> {
        let anchor = self.retention_anchor(name).await?;
        if self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(&anchor))
            .await?
            .is_none()
        {
            return Err(SchemaError::InvalidData(
                "retention backfill requires an active policy".into(),
            ));
        }
        let mut unique: HashMap<KeyValue, u64> = HashMap::new();
        for (key, clock) in clocks {
            unique
                .entry(key)
                .and_modify(|old| *old = (*old).max(clock))
                .or_insert(clock);
        }
        let mut puts: Vec<(String, Value)> = Vec::new();
        if range_keyed {
            for key in unique.keys() {
                let hash = key
                    .hash
                    .as_ref()
                    .ok_or_else(|| SchemaError::InvalidData("backfill key has no hash".into()))?;
                puts.push((
                    retention_hash_partition_key(&anchor, hash),
                    serde_json::to_value(SchemaRetentionHashPartition { hash: hash.clone() })
                        .map_err(|error| {
                            SchemaError::InvalidData(format!("serialize retention hash: {error}"))
                        })?,
                ));
            }
        } else {
            let ages: Vec<_> = unique
                .iter()
                .map(|(key, age)| (key.clone(), *age))
                .collect();
            self.record_schema_retention_write_ages(&anchor, &ages)
                .await?;
            return Ok(unique.len());
        }
        // Several range keys can name the same hash. The batch writes it once.
        let puts: HashMap<_, _> = puts.into_iter().collect();
        self.schema_states_store
            .batch_put_items(puts.into_iter().collect())
            .await?;
        Ok(unique.len())
    }
}
