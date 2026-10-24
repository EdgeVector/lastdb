//! Explicit, bounded maintenance for keys that predate a retention policy.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::types::KeyValue;
use crate::schema::{SchemaError, SchemaRetentionPolicy};
use serde_json::Value;
use std::collections::HashMap;

impl SchemaStore {
    /// The caller holds the native write scope across the tip-clock reads and
    /// these writes. A repeated page cannot make a newer age row older.
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
            let keys: Vec<_> = unique.keys().collect();
            let reverse: Vec<_> = keys
                .iter()
                .map(|key| retention_age_latest_key(&anchor, key))
                .collect();
            let previous = self
                .schema_states_store
                .get_items::<SchemaRetentionAgeEntry>(&reverse)
                .await?;
            for ((key, latest), old) in keys.into_iter().zip(reverse).zip(previous) {
                let written_at = unique[key].max(old.map_or(0, |old| old.written_at));
                let value = serde_json::to_value(SchemaRetentionAgeEntry {
                    key: key.clone(),
                    written_at,
                })
                .map_err(|error| {
                    SchemaError::InvalidData(format!("serialize retention age: {error}"))
                })?;
                puts.push((retention_age_key(&anchor, written_at, key), value.clone()));
                puts.push((latest, value));
            }
        }
        // Several range keys can name the same hash. The batch writes it once.
        let puts: HashMap<_, _> = puts.into_iter().collect();
        self.schema_states_store
            .batch_put_items(puts.into_iter().collect())
            .await?;
        Ok(unique.len())
    }
}
