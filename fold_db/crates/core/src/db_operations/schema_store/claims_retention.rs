//! Retention-policy methods of [`SchemaStore`].
//!
//! Each method takes the schema name the caller used and resolves it to the
//! retention anchor first (see `retention_layers`), so an app schema with two
//! installed names has one policy, one age index and one HashRange registry.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::types::KeyValue;
use crate::schema::{RetentionSource, SchemaError, SchemaRetentionPolicy};

impl SchemaStore {
    /// Read the node-local retention policy for an installed schema.
    pub async fn get_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        self.require_installed_schema(schema_name).await?;
        let anchor = self.retention_anchor(schema_name).await?;
        Ok(self
            .schema_states_store
            .get_item(&retention_policy_key(&anchor))
            .await?)
    }

    /// Set the operator retention policy for an installed schema.
    ///
    /// An operator policy wins over a manifest policy. The manifest TTL is
    /// kept in `overridden_manifest_ttl_seconds` and returns on clear.
    pub async fn set_schema_retention_policy(
        &self,
        schema_name: &str,
        mut policy: SchemaRetentionPolicy,
    ) -> Result<(), SchemaError> {
        self.require_installed_schema(schema_name).await?;
        let anchor = self.retention_anchor(schema_name).await?;
        let schema_name = anchor.as_str();
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
        policy.hash_partitions.sort();
        policy.hash_partitions.dedup();

        let key = retention_policy_key(schema_name);
        let current = self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&key)
            .await?;
        policy.source = RetentionSource::Operator;
        policy.overridden_manifest_ttl_seconds = current
            .as_ref()
            .and_then(SchemaRetentionPolicy::manifest_ttl_seconds);
        if current.as_ref() == Some(&policy) {
            return Ok(());
        }
        self.schema_states_store.put_item(&key, &policy).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Clear the operator retention policy for an installed schema.
    ///
    /// An operator policy over a manifest policy gives way to it. Any other
    /// policy is removed with its age index and partition registry.
    pub async fn clear_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        self.require_installed_schema(schema_name).await?;
        let anchor = self.retention_anchor(schema_name).await?;
        let schema_name = anchor.as_str();
        if self.restore_shadowed_manifest_policy(schema_name).await? {
            return Ok(());
        }
        let mut keys = vec![retention_policy_key(schema_name)];
        keys.extend(
            self.schema_states_store
                .list_keys_with_prefix(&retention_age_prefix(schema_name))
                .await?,
        );
        keys.extend(
            self.schema_states_store
                .list_keys_with_prefix(&retention_age_latest_prefix(schema_name))
                .await?,
        );
        keys.extend(
            self.schema_states_store
                .list_keys_with_prefix(&retention_hash_partition_prefix(schema_name))
                .await?,
        );
        self.schema_states_store.batch_delete_keys(keys).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Refresh the node-local age index for successful writes to a retained
    /// Hash/Single schema. The forward key is age-ordered; the reverse key
    /// makes rewrites and purges O(1). Stale forward rows are harmless because
    /// expiry selection validates them against the reverse row.
    pub async fn record_schema_retention_writes(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
        written_at: u64,
    ) -> Result<(), SchemaError> {
        if keys.is_empty() {
            return Ok(());
        }
        let anchor = self.retention_anchor(schema_name).await?;
        let schema_name = anchor.as_str();
        if self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(schema_name))
            .await?
            .is_none()
        {
            return Ok(());
        }
        let mut unique = keys.to_vec();
        unique.sort_by(KeyValue::cmp_page_order);
        unique.dedup();
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
        for ((key, latest_key), old) in unique.iter().zip(latest_keys).zip(previous) {
            let entry = SchemaRetentionAgeEntry {
                key: key.clone(),
                written_at,
            };
            puts.push((
                retention_age_key(schema_name, written_at, key),
                entry.clone(),
            ));
            puts.push((latest_key, entry));
            if let Some(old) = old.filter(|old| old.written_at != written_at) {
                stale.push(retention_age_key(schema_name, old.written_at, key));
            }
        }
        self.schema_states_store.batch_put_items(puts).await?;
        if !stale.is_empty() {
            self.schema_states_store.batch_delete_keys(stale).await?;
        }
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Remove age-index entries after a successful hard erasure.
    pub async fn remove_schema_retention_keys(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<(), SchemaError> {
        if keys.is_empty() {
            return Ok(());
        }
        let anchor = self.retention_anchor(schema_name).await?;
        let schema_name = anchor.as_str();
        let latest_keys: Vec<_> = keys
            .iter()
            .map(|key| retention_age_latest_key(schema_name, key))
            .collect();
        let previous = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        let mut deletes = latest_keys;
        deletes.extend(keys.iter().zip(previous).filter_map(|(key, old)| {
            old.map(|entry| retention_age_key(schema_name, entry.written_at, key))
        }));
        self.schema_states_store.batch_delete_keys(deletes).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Record HashRange partitions that receive writes after TTL is enabled.
    ///
    /// The registry is operational state. It does not create an engine query
    /// index or change the schema catalog. Each entry names one hash, which
    /// lets the sweeper retain the required hash-scoped range access pattern.
    pub async fn record_schema_retention_hash_partitions(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<(), SchemaError> {
        if keys.is_empty() {
            return Ok(());
        }
        let anchor = self.retention_anchor(schema_name).await?;
        let schema_name = anchor.as_str();
        if self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(schema_name))
            .await?
            .is_none()
        {
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
            .map(|hash| {
                (
                    retention_hash_partition_key(schema_name, &hash),
                    SchemaRetentionHashPartition { hash },
                )
            })
            .collect();
        self.schema_states_store.batch_put_items(entries).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Read observed HashRange partitions for one retained schema.
    ///
    /// This reads only the node-local retention registry. It never enumerates
    /// product rows or crosses HashRange data partitions.
    pub async fn schema_retention_hash_partitions(
        &self,
        schema_name: &str,
    ) -> Result<Vec<String>, SchemaError> {
        let anchor = self.retention_anchor(schema_name).await?;
        let scan = self
            .schema_states_store
            .scan_items_with_prefix_partition_undecodable::<SchemaRetentionHashPartition>(
                &retention_hash_partition_prefix(&anchor),
            )
            .await?;
        for (key, error) in &scan.undecodable {
            tracing::warn!(
                key = %key,
                error = %error,
                "unreadable schema retention HashRange partition; skipping"
            );
        }
        let mut hashes: Vec<_> = scan
            .items
            .into_iter()
            .map(|(_, entry)| entry.hash)
            .collect();
        hashes.sort();
        hashes.dedup();
        Ok(hashes)
    }

    /// Select keys with `written_at < cutoff` from one schema's age partition.
    /// This is a bounded range read, never an enumeration of product rows.
    pub async fn expired_schema_retention_keys(
        &self,
        schema_name: &str,
        cutoff: u64,
    ) -> Result<Vec<KeyValue>, SchemaError> {
        let anchor = self.retention_anchor(schema_name).await?;
        let schema_name = anchor.as_str();
        let prefix = retention_age_prefix(schema_name);
        let rows = self
            .schema_states_store
            .scan_items_in_range::<SchemaRetentionAgeEntry>(
                &format!("{prefix}{:020}", 0),
                &format!("{prefix}{cutoff:020}"),
            )
            .await?;
        let latest_keys: Vec<_> = rows
            .iter()
            .map(|(_, entry)| retention_age_latest_key(schema_name, &entry.key))
            .collect();
        let latest = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        let mut keys: Vec<_> = rows
            .into_iter()
            .zip(latest)
            .filter_map(|((_, entry), current)| {
                (current == Some(entry.clone())).then_some(entry.key)
            })
            .collect();
        keys.sort_by(KeyValue::cmp_page_order);
        keys.dedup();
        Ok(keys)
    }

    /// Enumerate node-local retention policies for schemas that still exist.
    ///
    /// Reads only the reserved `\0retention_policy\0` prefix in `schema_states`.
    /// A policy row whose schema is gone is skipped and does **not** create
    /// that schema — the sweeper must not materialise an absent series in
    /// order to expire it.
    pub async fn list_schema_retention_policies(
        &self,
    ) -> Result<Vec<(String, SchemaRetentionPolicy)>, SchemaError> {
        let scan = self
            .schema_states_store
            .scan_items_with_prefix_partition_undecodable::<SchemaRetentionPolicy>(
                RETENTION_POLICY_KEY_PREFIX,
            )
            .await?;
        for (key, error) in &scan.undecodable {
            tracing::warn!(
                key = %key,
                error = %error,
                "unreadable schema retention policy row; skipping"
            );
        }
        let mut out = Vec::with_capacity(scan.items.len());
        for (key, policy) in scan.items {
            let Some(schema_name) = key.strip_prefix(RETENTION_POLICY_KEY_PREFIX) else {
                continue;
            };
            if schema_name.is_empty() {
                continue;
            }
            if self.get_schema(schema_name).await?.is_none() {
                continue;
            }
            out.push((schema_name.to_string(), policy));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}
