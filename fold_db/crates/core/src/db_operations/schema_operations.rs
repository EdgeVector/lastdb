//! Logical schema operations and manifest retention application.

use super::core::DbOperations;
use super::schema_store::SchemaStore;
use crate::schema::manifest_retention::{
    ManifestRetentionOutcome, ManifestRetentionPlan, ManifestRetentionStatus,
};
use crate::schema::types::{
    RetentionSource, Schema, SchemaError, SchemaRetentionPolicy, SchemaState,
};
use crate::storage::format::schemas::{keys::retention_policy_key, StateChange};
use futures::{future::BoxFuture, FutureExt};
use std::collections::HashMap;

impl DbOperations {
    pub async fn get_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        self.schemas().get_schema(schema_name).await
    }

    pub async fn get_schema_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaState>, SchemaError> {
        self.schemas().get_schema_state(schema_name).await
    }

    pub fn store_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a Schema,
    ) -> BoxFuture<'a, Result<(), SchemaError>> {
        crate::storage::disk_pack::transaction::scope(
            self.native_runtime(),
            self.store_schema_in_scope(schema_name, schema),
        )
        .boxed()
    }

    async fn store_schema_in_scope(
        &self,
        schema_name: &str,
        schema: &Schema,
    ) -> Result<(), SchemaError> {
        schema
            .validate_definition()
            .map_err(SchemaError::InvalidData)?;
        let next = schema_field_key_profiles(schema)?;
        if let Some(old) = self.schemas().get_schema(schema_name).await? {
            let previous: HashMap<_, _> = schema_field_key_profiles(&old)?.into_iter().collect();
            for (field_key, profile) in &next {
                if previous
                    .get(field_key)
                    .is_some_and(|stored| stored != profile)
                {
                    return Err(SchemaError::InvalidData(
                        "the schema range type differs from the stored field key profile".into(),
                    ));
                }
            }
        }
        self.schemas().store_schema(schema_name, schema).await
    }

    pub async fn store_schema_state(
        &self,
        schema_name: &str,
        state: &SchemaState,
    ) -> Result<(), SchemaError> {
        self.schemas().store_schema_state(schema_name, state).await
    }

    pub async fn get_schema_retention_policy(
        &self,
        schema_name: &str,
    ) -> Result<Option<SchemaRetentionPolicy>, SchemaError> {
        self.schemas()
            .get_schema_retention_policy(schema_name)
            .await
    }

    pub async fn list_schema_retention_policies(
        &self,
    ) -> Result<Vec<(String, SchemaRetentionPolicy)>, SchemaError> {
        self.schemas().list_schema_retention_policies().await
    }

    pub async fn get_all_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        self.schemas().get_all_schemas().await
    }

    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        self.schemas().get_all_schema_states().await
    }

    pub fn drop_schema<'a>(
        &'a self,
        schema_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, SchemaError>> {
        async move {
            if let Some(schema) = self.block_schema_for_drop(schema_name).await? {
                let released = self.records().release_schema_rows(&schema).await?;
                tracing::info!(
                    target: "fold_node::database",
                    schema = schema_name,
                    records = released.records,
                    alternates = released.alternates,
                    "schema drop released the stored rows of the schema"
                );
            }
            self.schemas().drop_schema(schema_name).await
        }
        .boxed()
    }

    pub async fn block_schema_for_drop(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        crate::storage::disk_pack::transaction::scope(self.native_runtime(), async {
            if self.metadata().shared_schema_bound(schema_name).await? {
                return Err(SchemaError::InvalidData(
                    "a shared schema requires coordinated retirement".into(),
                ));
            }
            let Some(schema) = self.schemas().get_schema(schema_name).await? else {
                return Ok(None);
            };
            if self.schemas().get_schema_state(schema_name).await? != Some(SchemaState::Blocked) {
                self.store_schema_state(schema_name, &SchemaState::Blocked)
                    .await?;
            }
            Ok(Some(schema))
        })
        .await
    }
}

impl SchemaStore {
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

pub(crate) fn schema_field_key_profiles(
    schema: &Schema,
) -> Result<Vec<(String, schema_types::field_range_profile::FieldRangeProfile)>, SchemaError> {
    let definitions = schema.require_record_keys()?;
    let primary = definitions
        .primary
        .range_profile()
        .map_err(SchemaError::InvalidData)?;
    let mut result = Vec::with_capacity(definitions.alternates.len() + 1);
    for (name, key) in &definitions.alternates {
        let profile = key.range_profile().map_err(SchemaError::InvalidData)?;
        let id = lastdb_model::field_key_id::alternate_field_key_id(schema, name);
        if !lastdb_model::field_key_id::is_canonical_field_key_id(&id) {
            return Err(SchemaError::InvalidData(
                "invalid alternate field key identity".into(),
            ));
        }
        result.push((id, profile));
    }
    let space = lastdb_model::record_space::record_space_uuid(&schema.name);
    if !lastdb_model::field_key_id::is_canonical_field_key_id(&space) {
        return Err(SchemaError::InvalidData(
            "invalid record space identity".into(),
        ));
    }
    result.push((space, primary));
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.dedup();
    Ok(result)
}
