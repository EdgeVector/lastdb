//! Schema registry, retention policies, and core behaviors over `lastdb_model::schema`.

pub mod manifest_retention;
pub mod schema_types;
pub mod types;

use crate::storage::disk_pack::transaction::publication;
use crate::storage::StorageError;
use ::schema_types::IdentityRecompute;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub use lastdb_model::schema::logical_rules::{require_logical_filter, require_logical_layout};
pub use manifest_retention::{
    min_manifest_ttl_seconds, ManifestRetention, ManifestRetentionOutcome, ManifestRetentionPlan,
    ManifestRetentionStatus, MAX_MANIFEST_TTL_SECONDS, MIN_MANIFEST_TTL_ENV,
    MIN_MANIFEST_TTL_SECONDS,
};
pub use schema_types::{
    RetentionSource, SchemaListEntry, SchemaRetentionPolicy, SchemaState, SchemaWithState,
};
pub use types::{Schema, SchemaError};

pub mod mutation_author {
    use crate::schema::types::author_clock::mutation_author_canonical_bytes;
    use crate::schema::types::Mutation;
    use crate::security::{Ed25519KeyPair, KeyUtils};

    #[must_use]
    pub fn sign_mutation_author_clock(mutation: &Mutation, keypair: &Ed25519KeyPair) -> String {
        KeyUtils::signature_to_base64(&keypair.sign(&mutation_author_canonical_bytes(mutation)))
    }
}

#[derive(Clone, Default)]
struct SchemaCatalog {
    schemas: HashMap<String, Schema>,
    states: HashMap<String, SchemaState>,
}

struct CachedSchema {
    schema: Schema,
    state: SchemaState,
}

/// Core schema management system combining validation, copy-on-write catalog, and persistence.
pub struct SchemaCore {
    catalog: Arc<RwLock<Arc<SchemaCatalog>>>,
    db_ops: Arc<crate::db_operations::DbOperations>,
}

impl SchemaCore {
    pub async fn new(db_ops: Arc<crate::db_operations::DbOperations>) -> Result<Self, SchemaError> {
        let schemas = db_ops.schemas().get_all_schemas().await?;
        for schema in schemas.values() {
            require_explicit_record_keys(schema)?;
        }
        let states = db_ops.schemas().get_all_schema_states().await?;
        Ok(Self {
            catalog: Arc::new(RwLock::new(Arc::new(SchemaCatalog { schemas, states }))),
            db_ops,
        })
    }

    fn catalog_snapshot(&self) -> Result<Arc<SchemaCatalog>, SchemaError> {
        self.catalog
            .read()
            .map(|guard| Arc::clone(&guard))
            .map_err(|e| SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}")))
    }

    fn update_catalog(&self, f: impl FnOnce(&mut SchemaCatalog)) -> Result<(), SchemaError> {
        let mut guard = self
            .catalog
            .write()
            .map_err(|e| SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}")))?;
        f(Arc::make_mut(&mut *guard));
        Ok(())
    }

    fn staged_schema(&self, name: &str) -> Result<Option<Arc<CachedSchema>>, SchemaError> {
        Ok(publication::get(
            &self.db_ops.native_runtime(),
            Arc::as_ptr(&self.catalog) as usize,
            name,
        )?)
    }

    fn staged_schemas(&self) -> Result<Vec<(String, Arc<CachedSchema>)>, SchemaError> {
        Ok(publication::entries(
            &self.db_ops.native_runtime(),
            Arc::as_ptr(&self.catalog) as usize,
        )?)
    }

    fn publish_schema_cache(
        &self,
        name: &str,
        schema: Schema,
        state: SchemaState,
    ) -> Result<(), SchemaError> {
        let catalog = Arc::clone(&self.catalog);
        let name = name.to_string();
        let staged = CachedSchema {
            schema: schema.clone(),
            state,
        };
        publication::publish(
            &self.db_ops.native_runtime(),
            Arc::as_ptr(&self.catalog) as usize,
            name.clone(),
            staged,
            move || {
                let mut guard = catalog.write().map_err(|e| {
                    StorageError::BackendError(format!("schema catalog lock poisoned: {e}"))
                })?;
                let inner = Arc::make_mut(&mut *guard);
                inner.schemas.insert(name.clone(), schema);
                inner.states.insert(name, state);
                Ok(())
            },
        )?;
        Ok(())
    }

    pub fn get_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        let mut schemas = self.catalog_snapshot()?.schemas.clone();
        for (name, staged) in self.staged_schemas()? {
            schemas.insert(name, staged.schema.clone());
        }
        Ok(schemas)
    }

    pub fn get_schemas_with_states(&self) -> Result<Vec<SchemaWithState>, SchemaError> {
        let catalog = self.catalog_snapshot()?;
        let (mut schemas, mut states) = (catalog.schemas.clone(), catalog.states.clone());
        for (name, staged) in self.staged_schemas()? {
            schemas.insert(name.clone(), staged.schema.clone());
            states.insert(name, staged.state);
        }
        Ok(schemas
            .into_iter()
            .map(|(name, schema)| {
                SchemaWithState::new(schema, states.get(&name).copied().unwrap_or_default())
            })
            .collect())
    }

    pub fn get_active_schema_list_entries_with_states(
        &self,
    ) -> Result<Vec<SchemaListEntry>, SchemaError> {
        let catalog = self.catalog_snapshot()?;
        Ok(catalog
            .schemas
            .values()
            .filter_map(|schema| {
                let state = catalog
                    .states
                    .get(&schema.name)
                    .copied()
                    .unwrap_or_default();
                (state != SchemaState::Blocked).then(|| SchemaListEntry::from_schema(schema, state))
            })
            .collect())
    }

    pub fn get_schema_metadata(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        Ok(self.get_schema_and_state(schema_name)?.0)
    }

    pub(crate) fn get_schema_and_state(
        &self,
        schema_name: &str,
    ) -> Result<(Option<Schema>, SchemaState), SchemaError> {
        if let Some(staged) = self.staged_schema(schema_name)? {
            return Ok((Some(staged.schema.clone()), staged.state));
        }
        let catalog = self.catalog_snapshot()?;
        Ok((
            catalog.schemas.get(schema_name).cloned(),
            catalog.states.get(schema_name).copied().unwrap_or_default(),
        ))
    }

    pub(crate) fn require_schema_metadata(&self, schema_name: &str) -> Result<Schema, SchemaError> {
        self.get_schema_metadata(schema_name)?
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))
    }

    pub fn schemas_with_newer_identity_algo(&self) -> Result<Vec<String>, SchemaError> {
        let mut names: Vec<String> = self
            .catalog_snapshot()?
            .schemas
            .iter()
            .filter(|(_, schema)| schema.identity_is_newer_than_binary())
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        Ok(names)
    }

    pub fn get_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        let mut states = self.catalog_snapshot()?.states.clone();
        for (name, staged) in self.staged_schemas()? {
            states.insert(name, staged.state);
        }
        Ok(states)
    }

    pub(crate) fn require_queryable_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        if self.get_schema_and_state(schema_name)?.1 == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be queried"
            )));
        }
        Ok(())
    }

    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        if self
            .db_ops
            .block_schema_for_drop(schema_name)
            .await?
            .is_some()
        {
            self.update_catalog(|c| {
                c.states
                    .insert(schema_name.to_string(), SchemaState::Blocked);
            })?;
        }
        let existed = self.db_ops.drop_schema(schema_name).await?;
        self.update_catalog(|c| {
            c.schemas.remove(schema_name);
            c.states.remove(schema_name);
        })?;
        Ok(existed)
    }

    pub fn schema_names_owned_by(&self, owner_app: &str) -> Result<Vec<String>, SchemaError> {
        Ok(self
            .catalog_snapshot()?
            .schemas
            .iter()
            .filter(|(_, schema)| schema.owner_app_id.as_deref() == Some(owner_app))
            .map(|(name, _)| name.clone())
            .collect())
    }

    pub async fn resolve_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(schema) = self.get_schema_metadata(schema_name)? {
            return Ok(Some(schema));
        }
        let Some(schema) = self
            .db_ops
            .schemas()
            .get_schema(schema_name)
            .await
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?
        else {
            return Ok(None);
        };
        let cached_name = schema.name.clone();
        self.load_schema_internal(schema.clone()).await?;
        tracing::info!("Refreshed schema '{cached_name}' from database (stale cache)");
        Ok(self.get_schema_metadata(&cached_name)?.or(Some(schema)))
    }

    pub async fn load_schema_internal(&self, mut schema: Schema) -> Result<(), SchemaError> {
        require_explicit_record_keys(&schema)?;
        schema.validate()?;
        schema.ensure_field_hashes();

        let existing = self.db_ops.schemas().get_schema(&schema.name).await?;
        stamp_identity_for_load(&mut schema, existing.as_ref());

        let refresh = existing.is_some();
        if let Some(stored) = existing {
            if schema.record_keys != stored.record_keys
                || schema.alternate_keys != stored.alternate_keys
            {
                return Err(SchemaError::InvalidData(
                    "explicit primary/alternate key declarations are immutable; declare a new schema and copy explicitly"
                        .into(),
                ));
            }
            schema.validate()?;
        }
        let name = schema.name.clone();
        self.db_ops.store_schema(&name, &schema).await?;

        let state = if refresh {
            if let Some(state) = self.db_ops.schemas().get_schema_state(&name).await? {
                state
            } else {
                self.get_schema_and_state(&name)?.1
            }
        } else {
            self.db_ops
                .schemas()
                .store_schema_state(&name, &SchemaState::Available)
                .await?;
            SchemaState::Available
        };
        self.publish_schema_cache(&name, schema, state)
    }
}

fn require_explicit_record_keys(schema: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys.is_none() {
        return Err(SchemaError::InvalidData(
            "this home requires explicit record_keys; field-derived keys are unsupported".into(),
        ));
    }
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}

fn stamp_identity_for_load(schema: &mut Schema, existing: Option<&Schema>) {
    if let IdentityRecompute::RefusedDowngrade { stored, binary } =
        schema.recompute_identity_hash_unless_newer()
    {
        tracing::warn!(
            schema = %schema.name,
            stored_algo_version = stored,
            binary_algo_version = binary,
            identity_hash = %schema.identity_hash.as_deref().unwrap_or("<none>"),
            "refusing to downgrade schema identity minted by a newer algorithm",
        );
    }
    if let Some(stored_schema) = existing {
        if let Some(stored) = ::schema_types::refuses_identity_downgrade(
            stored_schema.identity_hash.as_deref(),
            stored_schema.identity_hash_algo_version,
        ) {
            schema.identity_hash = stored_schema.identity_hash.clone();
            schema.identity_hash_algo_version = stored_schema.identity_hash_algo_version;
            tracing::warn!(
                schema = %schema.name,
                stored_algo_version = stored,
                binary_algo_version = ::schema_types::IDENTITY_HASH_ALGO_VERSION,
                "refusing to overwrite a stored schema identity minted by a newer algorithm",
            );
        }
    }
}
