//! Retention anchors and the manifest policy layer of [`SchemaStore`].
//!
//! An app schema has two installed names that share records: the manifest name
//! (`app/Name`) and the catalog schema, named by its identity hash. Retention
//! state is keyed by one name, the anchor. Declaring a manifest `retention`
//! records the manifest name as an alias of the catalog schema, so a write
//! through either name reads and writes the same policy, age index and
//! HashRange partition registry. A name with no alias is its own anchor.

use super::catalog_keys::*;
use super::SchemaStore;
use crate::schema::manifest_retention::{ManifestRetentionOutcome, ManifestRetentionStatus};
use crate::schema::{RetentionSource, SchemaError, SchemaRetentionPolicy};

impl SchemaStore {
    /// The schema that holds the retention state for `schema_name`: the alias
    /// target when a manifest declare recorded one, otherwise the name itself.
    pub async fn retention_anchor(&self, schema_name: &str) -> Result<String, SchemaError> {
        let row = self
            .schema_states_store
            .get_item::<SchemaRetentionAnchor>(&retention_anchor_key(schema_name))
            .await?;
        Ok(row.map_or_else(|| schema_name.to_string(), |row| row.anchor))
    }

    /// Apply the TTL of an app manifest `retention` key to one declared schema.
    ///
    /// `schema_name` is the manifest name and `anchor` the catalog schema it is
    /// bound to. Both must be installed. The call records the alias, then sets
    /// the manifest policy under the anchor unless an operator policy is in
    /// force: the operator policy wins, and the manifest TTL is kept beside it.
    /// It is idempotent, and it never reads, changes or deletes product data.
    pub async fn apply_manifest_retention(
        &self,
        schema_name: &str,
        anchor: &str,
        ttl_seconds: u64,
    ) -> Result<ManifestRetentionOutcome, SchemaError> {
        self.require_installed_schema(schema_name).await?;
        self.require_installed_schema(anchor).await?;
        if ttl_seconds == 0 {
            return Err(SchemaError::InvalidData(
                "manifest retention ttl_seconds must be greater than zero".to_string(),
            ));
        }
        if schema_name != anchor {
            self.record_retention_anchor(schema_name, anchor).await?;
        }
        let key = retention_policy_key(anchor);
        let current = self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&key)
            .await?;
        let (next, status) = match current {
            None => (
                SchemaRetentionPolicy::manifest(ttl_seconds),
                ManifestRetentionStatus::Applied,
            ),
            Some(policy) if policy.source == RetentionSource::Manifest => {
                if policy.ttl_seconds == ttl_seconds {
                    return Ok(outcome(anchor, ManifestRetentionStatus::Unchanged, policy));
                }
                let next = SchemaRetentionPolicy {
                    ttl_seconds,
                    ..policy
                };
                (next, ManifestRetentionStatus::Updated)
            }
            Some(policy) => {
                let status = ManifestRetentionStatus::OperatorOverride;
                if policy.overridden_manifest_ttl_seconds == Some(ttl_seconds) {
                    return Ok(outcome(anchor, status, policy));
                }
                let next = SchemaRetentionPolicy {
                    overridden_manifest_ttl_seconds: Some(ttl_seconds),
                    ..policy
                };
                (next, status)
            }
        };
        self.schema_states_store.put_item(&key, &next).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(outcome(anchor, status, next))
    }

    /// Put a manifest TTL back in force when the operator policy over it is
    /// cleared. Returns `false` when the policy in force shadows nothing, so
    /// the caller clears the policy and its tracking rows as before.
    pub(super) async fn restore_shadowed_manifest_policy(
        &self,
        anchor: &str,
    ) -> Result<bool, SchemaError> {
        let key = retention_policy_key(anchor);
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
        self.schema_states_store.put_item(&key, &restored).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(true)
    }

    /// Record `anchor` as the retention anchor of `schema_name`.
    ///
    /// Refuses when the manifest name already has a policy of its own: the
    /// alias would hide it, and its age index and partition registry, from the
    /// sweep. The operator clears it first.
    async fn record_retention_anchor(
        &self,
        schema_name: &str,
        anchor: &str,
    ) -> Result<(), SchemaError> {
        let key = retention_anchor_key(schema_name);
        let row = SchemaRetentionAnchor {
            anchor: anchor.to_string(),
        };
        let existing = self
            .schema_states_store
            .get_item::<SchemaRetentionAnchor>(&key)
            .await?;
        if existing.as_ref() == Some(&row) {
            return Ok(());
        }
        if existing.is_none()
            && self
                .schema_states_store
                .get_item::<SchemaRetentionPolicy>(&retention_policy_key(schema_name))
                .await?
                .is_some()
        {
            return Err(SchemaError::InvalidData(format!(
                "'{schema_name}' already has a retention policy of its own. Clear it with \
                 `lastdb schema-retention clear --schema {schema_name}`, then declare the manifest again"
            )));
        }
        self.schema_states_store.put_item(&key, &row).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }
}

fn outcome(
    anchor: &str,
    status: ManifestRetentionStatus,
    effective: SchemaRetentionPolicy,
) -> ManifestRetentionOutcome {
    ManifestRetentionOutcome {
        anchor: anchor.to_string(),
        status,
        effective,
    }
}
