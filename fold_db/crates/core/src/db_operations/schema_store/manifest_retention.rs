//! Manifest-declared retention methods of [`SchemaStore`].

use super::catalog_keys::*;
use super::{SchemaStore, StateChange};
use crate::schema::manifest_retention::{
    ManifestRetentionOutcome, ManifestRetentionPlan, ManifestRetentionStatus,
};
use crate::schema::{RetentionSource, SchemaError, SchemaRetentionPolicy};

impl SchemaStore {
    /// Read what a manifest TTL of `ttl_seconds` would do to `schema_name`.
    pub async fn plan_manifest_retention(
        &self,
        schema_name: &str,
        ttl_seconds: u64,
    ) -> Result<ManifestRetentionPlan, SchemaError> {
        let current = self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(schema_name))
            .await?;
        let (next, status, shortened) = next_manifest_policy(current.as_ref(), ttl_seconds);
        Ok(ManifestRetentionPlan {
            anchor: schema_name.to_string(),
            current,
            next,
            status,
            shortened,
        })
    }

    /// Apply the TTL of an app manifest `retention` key to one declared schema.
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
            return Err(SchemaError::InvalidData(format!(
                "{schema_name} is not owned by {owner_app}"
            )));
        }
        self.require_retention_format(schema_name).await?;
        if ttl_seconds == 0 {
            return Err(SchemaError::InvalidData(
                "manifest retention ttl_seconds must be greater than zero".to_string(),
            ));
        }
        let plan = self
            .plan_manifest_retention(schema_name, ttl_seconds)
            .await?;
        if plan.shortened && !allow_shorter {
            return Err(SchemaError::InvalidData(format!(
                "the manifest TTL of {schema_name} would be shortened to {ttl_seconds}s, and \
                 records between the two TTLs would expire. Confirm it explicitly"
            )));
        }
        let Some(next) = plan.next.clone() else {
            let effective = plan.current.clone().ok_or_else(|| {
                SchemaError::InvalidData("manifest retention left no policy in force".to_string())
            })?;
            return Ok(ManifestRetentionOutcome {
                anchor: plan.anchor,
                status: plan.status,
                previous: plan.current,
                effective,
                shortened: plan.shortened,
            });
        };
        self.format
            .put_state(StateChange::Policy {
                schema: schema_name,
                policy: &next,
            })
            .await?;
        self.format.flush_states().await?;
        Ok(ManifestRetentionOutcome {
            anchor: plan.anchor,
            status: plan.status,
            previous: plan.current,
            effective: next,
            shortened: plan.shortened,
        })
    }

    pub(super) async fn restore_shadowed_manifest_policy(
        &self,
        schema_name: &str,
    ) -> Result<bool, SchemaError> {
        let key = retention_policy_key(schema_name);
        let Some(current) = self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&key)
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
        Some(policy) if policy.source == RetentionSource::Manifest => {
            if policy.ttl_seconds == ttl_seconds {
                return (None, ManifestRetentionStatus::Unchanged, false);
            }
            let next = SchemaRetentionPolicy {
                ttl_seconds,
                ..policy.clone()
            };
            (
                Some(next),
                ManifestRetentionStatus::Updated,
                ttl_seconds < policy.ttl_seconds,
            )
        }
        Some(policy) => {
            let status = ManifestRetentionStatus::OperatorOverride;
            if policy.overridden_manifest_ttl_seconds == Some(ttl_seconds) {
                return (None, status, false);
            }
            let next = SchemaRetentionPolicy {
                overridden_manifest_ttl_seconds: Some(ttl_seconds),
                ..policy.clone()
            };
            (Some(next), status, false)
        }
    }
}
