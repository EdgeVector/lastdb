//! Retention write-age and hash-partition index tracking inside native write scopes.

use super::retention::latest_keys;
use super::{SchemaStore, StateChange, StateDelete, Tracking};
use crate::schema::types::{KeyValue, SchemaError};
use crate::storage::format::schemas::keys::*;
use std::collections::HashMap;

impl SchemaStore {
    pub(crate) async fn record_schema_retention_write_ages(
        &self,
        schema_name: &str,
        ages: &[(KeyValue, u64)],
    ) -> Result<(), SchemaError> {
        if ages.is_empty() || self.get_policy(schema_name).await?.is_none() {
            return Ok(());
        }
        let mut clocks = HashMap::<KeyValue, u64>::new();
        for (key, written_at) in ages {
            clocks
                .entry(key.clone())
                .and_modify(|old| *old = (*old).max(*written_at))
                .or_insert(*written_at);
        }
        let mut unique: Vec<_> = clocks.keys().cloned().collect();
        unique.sort_by(|a, b| a.cmp_page_order_with(b, str::cmp));
        let previous = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys(schema_name, &unique))
            .await?;
        let (mut puts, mut stale) = (Vec::with_capacity(unique.len() * 2), Vec::new());
        for (key, old) in unique.iter().zip(previous) {
            let written_at = clocks[key];
            let entry = SchemaRetentionAgeEntry {
                key: key.clone(),
                written_at,
            };
            puts.extend([
                StateChange::Age {
                    schema: schema_name,
                    entry: entry.clone(),
                    latest: false,
                },
                StateChange::Age {
                    schema: schema_name,
                    entry,
                    latest: true,
                },
            ]);
            if let Some(old) = old.filter(|old| old.written_at != written_at) {
                stale.push(retention_age_key(schema_name, old.written_at, key));
            }
        }
        self.format.batch_states(puts).await?;
        if !stale.is_empty() {
            self.format
                .delete_states(vec![StateDelete::Selected {
                    schema: schema_name,
                    kind: Tracking::Age,
                    keys: stale,
                }])
                .await?;
        }
        Ok(())
    }

    pub async fn remove_schema_retention_keys(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<(), SchemaError> {
        if keys.is_empty() {
            return Ok(());
        }
        let latest = latest_keys(schema_name, keys);
        let previous = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest)
            .await?;
        let old = keys
            .iter()
            .zip(previous)
            .filter_map(|(k, old)| old.map(|e| retention_age_key(schema_name, e.written_at, k)))
            .collect();
        self.format
            .delete_states(vec![
                StateDelete::Selected {
                    schema: schema_name,
                    kind: Tracking::Latest,
                    keys: latest,
                },
                StateDelete::Selected {
                    schema: schema_name,
                    kind: Tracking::Age,
                    keys: old,
                },
            ])
            .await?;
        Ok(())
    }

    pub async fn record_schema_retention_hash_partitions(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<(), SchemaError> {
        if keys.is_empty() || self.get_policy(schema_name).await?.is_none() {
            return Ok(());
        }
        let mut hashes: Vec<_> = keys.iter().filter_map(|k| k.hash.clone()).collect();
        hashes.sort();
        hashes.dedup();
        if hashes.is_empty() {
            return Ok(());
        }
        let entries = hashes
            .into_iter()
            .map(|hash| StateChange::Partition {
                schema: schema_name,
                entry: SchemaRetentionHashPartition { hash },
            })
            .collect();
        self.format.batch_states(entries).await?;
        Ok(())
    }
}
