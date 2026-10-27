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
use crate::schema::manifest_retention::{
    ManifestRetentionOutcome, ManifestRetentionPlan, ManifestRetentionStatus,
};
use crate::schema::{RetentionSource, SchemaError, SchemaRetentionPolicy};
use crate::storage::StorageError;

impl SchemaStore {
    /// The schema that holds the retention state for `schema_name`: the alias
    /// target when a manifest declare recorded one, otherwise the name itself.
    ///
    /// An alias row that cannot be decoded reads as no alias, with an error in
    /// the log. This runs in the persist lane for every write: a failed read
    /// there is retried and then quarantines the write, so one bad row must not
    /// block the durability of a schema's writes. Reading no alias deletes
    /// nothing: the schema simply stops being tracked.
    pub async fn retention_anchor(&self, schema_name: &str) -> Result<String, SchemaError> {
        let key = retention_anchor_key(schema_name);
        match self
            .schema_states_store
            .get_item::<SchemaRetentionAnchor>(&key)
            .await
        {
            Ok(row) => Ok(row.map_or_else(|| schema_name.to_string(), |row| row.anchor)),
            Err(StorageError::SerializationError(error)) => {
                tracing::error!(
                    schema = %schema_name,
                    error = %error,
                    "unreadable schema retention anchor row; treating the schema as its own anchor"
                );
                Ok(schema_name.to_string())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Read what a manifest TTL of `ttl_seconds` would do to `schema_name`
    /// anchored on `anchor`. Writes nothing.
    pub async fn plan_manifest_retention(
        &self,
        schema_name: &str,
        anchor: &str,
        ttl_seconds: u64,
    ) -> Result<ManifestRetentionPlan, SchemaError> {
        let alias = self.retention_anchor(schema_name).await?;
        let moved_from = (alias != schema_name && alias != anchor).then(|| alias.clone());
        if alias == schema_name && schema_name != anchor {
            self.refuse_policy_on_manifest_name(schema_name).await?;
        }
        let source = moved_from.as_deref().unwrap_or(anchor);
        let mut current = self.read_retention_policy(source).await?;
        if current.is_none() && moved_from.is_some() {
            // A move that was interrupted after the old policy was deleted:
            // the policy is already on the new anchor.
            current = self.read_retention_policy(anchor).await?;
        }
        if let (Some(from), Some(carried)) = (&moved_from, &current) {
            let held = self.read_retention_policy(anchor).await?;
            if held.as_ref().is_some_and(|held| held != carried) {
                return Err(SchemaError::InvalidData(format!(
                    "{from} and {anchor} hold different retention policies. Clear one with \
                     `lastdb schema-retention clear`, then declare the manifest again"
                )));
            }
        }
        let (next, status, shortened) = next_manifest_policy(current.as_ref(), ttl_seconds);
        Ok(ManifestRetentionPlan {
            anchor: anchor.to_string(),
            moved_from,
            current,
            next,
            status,
            shortened,
        })
    }

    /// Apply the TTL of an app manifest `retention` key to one declared schema.
    ///
    /// `schema_name` is the manifest name and `anchor` the catalog schema it is
    /// bound to. Both must be installed. The call refuses when another app owns
    /// the catalog schema, or when another schema is bound to it: the TTL sweep
    /// would delete their records too. It refuses a TTL shorter than the
    /// manifest TTL running unless `allow_shorter`. It records the alias, moves
    /// the policy and its tracking when the manifest name pointed at another
    /// anchor, then sets the manifest policy unless an operator policy is in
    /// force: the operator policy wins, and the manifest TTL is kept beside it.
    /// It is idempotent, and it never reads, changes or deletes product data.
    pub async fn apply_manifest_retention(
        &self,
        schema_name: &str,
        anchor: &str,
        ttl_seconds: u64,
        owner_app: &str,
        allow_shorter: bool,
    ) -> Result<ManifestRetentionOutcome, SchemaError> {
        self.require_installed_schema(schema_name).await?;
        self.require_installed_schema(anchor).await?;
        self.require_retention_format(anchor).await?;
        if ttl_seconds == 0 {
            return Err(SchemaError::InvalidData(
                "manifest retention ttl_seconds must be greater than zero".to_string(),
            ));
        }
        let sharing = self
            .retention_sharing(schema_name, anchor, owner_app, &[])
            .await?;
        if let Some(reason) = sharing.refusal(schema_name, anchor, owner_app) {
            return Err(SchemaError::InvalidData(reason));
        }
        let plan = self
            .plan_manifest_retention(schema_name, anchor, ttl_seconds)
            .await?;
        if plan.shortened && !allow_shorter {
            return Err(SchemaError::InvalidData(format!(
                "the manifest TTL of {schema_name} would be shortened to {ttl_seconds}s, and \
                 records between the two TTLs would expire. Confirm it explicitly"
            )));
        }
        let new_alias = plan.moved_from.is_none()
            && schema_name != anchor
            && self.retention_anchor(schema_name).await? == schema_name;
        if let Some(from) = &plan.moved_from {
            self.move_retention_anchor(schema_name, from, anchor)
                .await?;
        } else if schema_name != anchor {
            self.record_retention_anchor(schema_name, anchor).await?;
        }
        let key = retention_policy_key(anchor);
        let (next, status, shortened) = (plan.next.clone(), plan.status, plan.shortened);
        let Some(next) = next else {
            let effective = plan.current.clone().ok_or_else(|| {
                SchemaError::InvalidData("manifest retention left no policy in force".to_string())
            })?;
            return Ok(outcome(&plan, status, shortened, effective));
        };
        let written = async {
            self.schema_states_store.put_item(&key, &next).await?;
            self.schema_states_store.inner().flush().await
        }
        .await;
        if let Err(error) = written {
            if new_alias {
                // No alias without its policy: the state stays as it was.
                let _ = self
                    .schema_states_store
                    .delete_item(&retention_anchor_key(schema_name))
                    .await;
            }
            return Err(error.into());
        }
        Ok(outcome(&plan, status, shortened, next))
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
    pub(super) async fn record_retention_anchor(
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
        if existing.is_none() {
            self.refuse_policy_on_manifest_name(schema_name).await?;
        }
        self.schema_states_store.put_item(&key, &row).await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    pub(super) async fn read_retention_policy(
        &self,
        anchor: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        Ok(self
            .schema_states_store
            .get_item::<SchemaRetentionPolicy>(&retention_policy_key(anchor))
            .await?)
    }

    async fn refuse_policy_on_manifest_name(&self, schema_name: &str) -> Result<(), SchemaError> {
        if self.read_retention_policy(schema_name).await?.is_some() {
            return Err(SchemaError::InvalidData(format!(
                "'{schema_name}' already has a retention policy of its own. Clear it with \
                 `lastdb schema-retention clear --schema {schema_name}`, then declare the manifest again"
            )));
        }
        Ok(())
    }
}

/// The manifest policy that follows `current` for a manifest TTL of
/// `ttl_seconds`: `None` when nothing changes, the status, and whether the
/// manifest TTL got shorter.
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

fn outcome(
    plan: &ManifestRetentionPlan,
    status: ManifestRetentionStatus,
    shortened: bool,
    effective: SchemaRetentionPolicy,
) -> ManifestRetentionOutcome {
    ManifestRetentionOutcome {
        anchor: plan.anchor.clone(),
        status,
        previous: plan.current.clone(),
        effective,
        moved_from: plan.moved_from.clone(),
        shortened,
    }
}
