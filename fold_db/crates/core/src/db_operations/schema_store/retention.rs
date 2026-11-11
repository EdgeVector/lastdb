//! Operator and manifest retention-policy management on [`SchemaStore`].

use super::{SchemaStore, StateChange, StateDelete, StateKey, Tracking};
use crate::schema::schema_types::{RetentionSource, SchemaRetentionPolicy};
use crate::schema::types::{KeyValue, SchemaError};
use crate::storage::format::schemas::keys::*;
use std::collections::HashMap;

impl SchemaStore {
    pub async fn get_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        self.require_installed_schema(schema_name).await?;
        let policy = self
            .schema_states_store
            .get_item(&retention_policy_key(schema_name))
            .await?;
        if policy.is_some() {
            self.require_retention_format(schema_name).await?;
        }
        Ok(policy)
    }

    pub async fn set_schema_retention_policy(
        &self,
        schema_name: &str,
        policy: SchemaRetentionPolicy,
    ) -> Result<(), SchemaError> {
        self.require_installed_schema(schema_name).await?;
        self.require_retention_format(schema_name).await?;
        if policy.ttl_seconds == 0 {
            return Err(SchemaError::InvalidData(
                "schema retention ttl_seconds must be greater than zero".to_string(),
            ));
        }
        if policy.hash_partitions.iter().any(String::is_empty) {
            return Err(SchemaError::InvalidData(
                "schema retention hash partitions must not be empty".to_string(),
            ));
        }
        let key = retention_policy_key(schema_name);
        let current = self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&key)
            .await?;
        let policy = SchemaRetentionPolicy::operator_over(
            current.as_ref(),
            policy.ttl_seconds,
            policy.hash_partitions,
        );
        if current.as_ref() == Some(&policy) {
            return Ok(());
        }
        self.format
            .put_state(StateChange::Policy {
                schema: schema_name,
                policy: &policy,
            })
            .await?;
        self.format.flush_states().await?;
        Ok(())
    }

    pub async fn clear_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        self.require_installed_schema(schema_name).await?;
        if self.restore_shadowed_manifest_policy(schema_name).await? {
            return Ok(());
        }
        let keys = vec![
            StateDelete::Key {
                schema: schema_name,
                kind: StateKey::Policy,
            },
            StateDelete::Selected {
                schema: schema_name,
                kind: Tracking::Age,
                keys: self
                    .schema_states_store
                    .list_keys_with_prefix(&retention_age_prefix(schema_name))
                    .await?,
            },
            StateDelete::Selected {
                schema: schema_name,
                kind: Tracking::Latest,
                keys: self
                    .schema_states_store
                    .list_keys_with_prefix(&retention_age_latest_prefix(schema_name))
                    .await?,
            },
            StateDelete::Selected {
                schema: schema_name,
                kind: Tracking::Partition,
                keys: self
                    .schema_states_store
                    .list_keys_with_prefix(&retention_hash_partition_prefix(schema_name))
                    .await?,
            },
        ];
        self.format.delete_states(keys).await?;
        self.format.flush_states().await?;
        Ok(())
    }

    pub(crate) async fn record_schema_retention_write_ages(
        &self,
        schema_name: &str,
        ages: &[(KeyValue, u64)],
    ) -> Result<(), SchemaError> {
        if ages.is_empty() || !self.has_retention_policy(schema_name).await? {
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
        unique.sort_by(KeyValue::cmp_storage_order);
        let latest_keys: Vec<_> = unique
            .iter()
            .map(|key| retention_age_latest_key(schema_name, key))
            .collect();
        let previous = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        let mut puts = Vec::with_capacity(unique.len() * 2);
        let mut stale = Vec::new();
        drop(latest_keys);
        for (key, old) in unique.iter().zip(previous) {
            let written_at = clocks[key];
            let entry = SchemaRetentionAgeEntry {
                key: key.clone(),
                written_at,
            };
            puts.push(StateChange::Age {
                schema: schema_name,
                entry: entry.clone(),
                latest: false,
            });
            puts.push(StateChange::Age {
                schema: schema_name,
                entry,
                latest: true,
            });
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
        self.format.flush_states().await?;
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
        let latest_keys: Vec<_> = keys
            .iter()
            .map(|key| retention_age_latest_key(schema_name, key))
            .collect();
        let previous = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        let old = keys
            .iter()
            .zip(previous)
            .filter_map(|(key, old)| {
                old.map(|entry| retention_age_key(schema_name, entry.written_at, key))
            })
            .collect();
        self.format
            .delete_states(vec![
                StateDelete::Selected {
                    schema: schema_name,
                    kind: Tracking::Latest,
                    keys: latest_keys,
                },
                StateDelete::Selected {
                    schema: schema_name,
                    kind: Tracking::Age,
                    keys: old,
                },
            ])
            .await?;
        self.format.flush_states().await?;
        Ok(())
    }

    pub async fn record_schema_retention_hash_partitions(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<(), SchemaError> {
        if keys.is_empty() || !self.has_retention_policy(schema_name).await? {
            return Ok(());
        }
        let mut hashes: Vec<_> = keys.iter().filter_map(|key| key.hash.clone()).collect();
        hashes.sort();
        hashes.dedup();
        if hashes.is_empty() {
            return Ok(());
        }
        let entries: Vec<_> = hashes
            .into_iter()
            .map(|hash| StateChange::Partition {
                schema: schema_name,
                entry: SchemaRetentionHashPartition { hash },
            })
            .collect();
        self.format.batch_states(entries).await?;
        self.format.flush_states().await?;
        Ok(())
    }

    async fn has_retention_policy(&self, schema_name: &str) -> Result<bool, SchemaError> {
        Ok(self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(schema_name))
            .await?
            .is_some())
    }

    pub async fn list_schema_retention_policies(
        &self,
    ) -> Result<Vec<(String, SchemaRetentionPolicy)>, SchemaError> {
        let rows = self
            .schema_states_store
            .scan_items_with_prefix::<SchemaRetentionPolicy>(RETENTION_POLICY_KEY_PREFIX)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, policy) in rows {
            let Some(schema_name) = key.strip_prefix(RETENTION_POLICY_KEY_PREFIX) else {
                continue;
            };
            if !schema_name.is_empty() && self.get_schema(schema_name).await?.is_some() {
                out.push((schema_name.to_string(), policy));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    async fn restore_shadowed_manifest_policy(
        &self,
        schema_name: &str,
    ) -> Result<bool, SchemaError> {
        let Some(current) = self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(schema_name))
            .await?
        else {
            return Ok(false);
        };
        let Some(ttl_seconds) = current
            .overridden_manifest_ttl_seconds
            .filter(|_| current.source == RetentionSource::Operator)
        else {
            return Ok(false);
        };
        let restored = SchemaRetentionPolicy::manifest(ttl_seconds);
        self.format
            .put_state(StateChange::Policy {
                schema: schema_name,
                policy: &restored,
            })
            .await?;
        self.format.flush_states().await?;
        Ok(true)
    }
}
