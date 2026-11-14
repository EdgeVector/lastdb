//! Retention policy CRUD and manifest retention planning/application on [`SchemaStore`].

use super::{SchemaStore, StateChange, StateDelete, StateKey, Tracking};
use crate::limits::manifest_retention::{
    ManifestRetentionOutcome, ManifestRetentionPlan, ManifestRetentionStatus,
};
use crate::schema::types::catalog_types::{RetentionSource, SchemaRetentionPolicy};
use crate::schema::types::{KeyValue, SchemaError};
use crate::storage::format::schemas::keys::*;

pub(super) fn invalid(msg: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(msg.into())
}

pub(super) fn latest_keys(schema_name: &str, keys: &[KeyValue]) -> Vec<String> {
    keys.iter()
        .map(|key| retention_age_latest_key(schema_name, key))
        .collect()
}

impl SchemaStore {
    pub(super) async fn get_policy(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        Ok(self
            .schema_states_store
            .get_item(&retention_policy_key(schema_name))
            .await?)
    }

    pub async fn get_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        self.require_installed_schema(schema_name).await?;
        let policy = self.get_policy(schema_name).await?;
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
            return Err(invalid(
                "schema retention ttl_seconds must be greater than zero",
            ));
        }
        if policy.hash_partitions.iter().any(String::is_empty) {
            return Err(invalid(
                "schema retention hash partitions must not be empty",
            ));
        }
        let current = self.get_policy(schema_name).await?;
        let policy = SchemaRetentionPolicy::operator_over(
            current.as_ref(),
            policy.ttl_seconds,
            policy.hash_partitions,
        );
        if current.as_ref() != Some(&policy) {
            self.put_policy(schema_name, &policy).await?;
        }
        Ok(())
    }

    pub async fn plan_manifest_retention(
        &self,
        schema_name: &str,
        ttl_seconds: u64,
    ) -> Result<ManifestRetentionPlan, SchemaError> {
        let current = self.get_policy(schema_name).await?;
        let (next, status, shortened) = next_manifest_policy(current.as_ref(), ttl_seconds);
        Ok(ManifestRetentionPlan {
            anchor: schema_name.to_string(),
            current,
            next,
            status,
            shortened,
        })
    }

    pub async fn apply_manifest_retention(
        &self,
        schema_name: &str,
        ttl_seconds: u64,
        owner_app: &str,
        allow_shorter: bool,
    ) -> Result<ManifestRetentionOutcome, SchemaError> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| SchemaError::NotFound(schema_name.to_string()))?;
        if schema.owner_app_id.as_deref() != Some(owner_app) {
            return Err(invalid(format!(
                "{schema_name} is not owned by {owner_app}"
            )));
        }
        self.require_retention_format(schema_name).await?;
        if ttl_seconds == 0 {
            return Err(invalid(
                "manifest retention ttl_seconds must be greater than zero",
            ));
        }
        let plan = self
            .plan_manifest_retention(schema_name, ttl_seconds)
            .await?;
        if plan.shortened && !allow_shorter {
            return Err(invalid(format!(
                "the manifest TTL of {schema_name} would be shortened to {ttl_seconds}s, and records between the two TTLs would expire. Confirm it explicitly"
            )));
        }
        let effective = match plan.next {
            Some(next) => {
                self.put_policy(schema_name, &next).await?;
                next
            }
            None => plan
                .current
                .clone()
                .ok_or_else(|| invalid("manifest retention left no policy in force"))?,
        };
        Ok(ManifestRetentionOutcome {
            anchor: plan.anchor,
            status: plan.status,
            previous: plan.current,
            effective,
            shortened: plan.shortened,
        })
    }

    pub async fn clear_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<(), SchemaError> {
        self.require_installed_schema(schema_name).await?;
        let previous = self.get_policy(schema_name).await?;
        if let Some(restored) = SchemaRetentionPolicy::after_operator_clear(previous.as_ref()) {
            return self.put_policy(schema_name, &restored).await;
        }
        let mut keys = vec![StateDelete::Key {
            schema: schema_name,
            kind: StateKey::Policy,
        }];
        for (kind, prefix) in [
            (Tracking::Age, retention_age_prefix(schema_name)),
            (Tracking::Latest, retention_age_latest_prefix(schema_name)),
            (
                Tracking::Partition,
                retention_hash_partition_prefix(schema_name),
            ),
        ] {
            keys.push(StateDelete::Selected {
                schema: schema_name,
                kind,
                keys: self
                    .schema_states_store
                    .list_keys_with_prefix(&prefix)
                    .await?,
            });
        }
        self.format.delete_states(keys).await?;
        self.format.flush_states().await?;
        Ok(())
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
            if let Some(name) = key
                .strip_prefix(RETENTION_POLICY_KEY_PREFIX)
                .filter(|s| !s.is_empty())
            {
                if self.get_schema(name).await?.is_some() {
                    out.push((name.to_string(), policy));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    async fn put_policy(
        &self,
        schema_name: &str,
        policy: &SchemaRetentionPolicy,
    ) -> Result<(), SchemaError> {
        self.format
            .put_state(StateChange::Policy {
                schema: schema_name,
                policy,
            })
            .await?;
        self.format.flush_states().await?;
        Ok(())
    }
}

fn next_manifest_policy(
    current: Option<&SchemaRetentionPolicy>,
    ttl_seconds: u64,
) -> (Option<SchemaRetentionPolicy>, ManifestRetentionStatus, bool) {
    match current {
        None => (
            Some(SchemaRetentionPolicy::manifest(ttl_seconds)),
            ManifestRetentionStatus::Applied,
            false,
        ),
        Some(p) if p.source == RetentionSource::Manifest && p.ttl_seconds == ttl_seconds => {
            (None, ManifestRetentionStatus::Unchanged, false)
        }
        Some(p) if p.source == RetentionSource::Manifest => (
            Some(SchemaRetentionPolicy {
                ttl_seconds,
                ..p.clone()
            }),
            ManifestRetentionStatus::Updated,
            ttl_seconds < p.ttl_seconds,
        ),
        Some(p) => (
            (p.overridden_manifest_ttl_seconds != Some(ttl_seconds)).then(|| {
                SchemaRetentionPolicy {
                    overridden_manifest_ttl_seconds: Some(ttl_seconds),
                    ..p.clone()
                }
            }),
            ManifestRetentionStatus::OperatorOverride,
            false,
        ),
    }
}
