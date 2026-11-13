//! Schema registry, retention policies, and core behaviors over `lastdb_model::schema`.

pub mod types;

use crate::storage::disk_pack::transaction::{self, publication};
use crate::storage::format::metadata::SchemaReplayDrop;
use crate::storage::StorageError;
use ::schema_types::IdentityRecompute;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub use crate::error::manifest_retention::{
    self, ManifestRetention, ManifestRetentionOutcome, ManifestRetentionPlan,
    ManifestRetentionStatus,
};
pub use crate::schema::types::schema_types::{
    self, RetentionSource, SchemaListEntry, SchemaRetentionPolicy, SchemaState, SchemaWithState,
};
pub use crate::schema::types::{Schema, SchemaError};
pub use lastdb_model::schema::logical_rules::{require_logical_filter, require_logical_layout};

pub mod mutation_author {
    use crate::crypto::security::{Ed25519KeyPair, KeyUtils};
    use crate::schema::types::{author_clock::mutation_author_canonical_bytes, Mutation};

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

#[derive(Clone)]
struct CachedSchema {
    schema: Option<Schema>,
    state: SchemaState,
}

pub struct SchemaCore {
    catalog: Arc<RwLock<Arc<SchemaCatalog>>>,
    db_ops: Arc<crate::db_operations::DbOperations>,
    drops: tokio::sync::Mutex<()>,
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
            drops: tokio::sync::Mutex::new(()),
        })
    }

    fn snapshot(&self) -> Result<Arc<SchemaCatalog>, SchemaError> {
        self.catalog
            .read()
            .map(|g| Arc::clone(&g))
            .map_err(|e| SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}")))
    }

    fn catalog_id(&self) -> usize {
        Arc::as_ptr(&self.catalog) as usize
    }

    fn merged(&self) -> Result<SchemaCatalog, SchemaError> {
        let mut c = (*self.snapshot()?).clone();
        for (n, s) in
            publication::entries::<CachedSchema>(&self.db_ops.native_runtime(), self.catalog_id())?
        {
            s.update(&mut c, n);
        }
        Ok(c)
    }

    pub fn get_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        Ok(self.merged()?.schemas)
    }

    pub fn get_schemas_with_states(&self) -> Result<Vec<SchemaWithState>, SchemaError> {
        let c = self.merged()?;
        Ok(c.schemas
            .into_iter()
            .map(|(n, s)| SchemaWithState::new(s, c.states.get(&n).copied().unwrap_or_default()))
            .collect())
    }

    pub fn get_active_schema_list_entries_with_states(
        &self,
    ) -> Result<Vec<SchemaListEntry>, SchemaError> {
        let c = self.snapshot()?;
        Ok(c.schemas
            .values()
            .filter_map(|s| {
                let st = c.states.get(&s.name).copied().unwrap_or_default();
                (st != SchemaState::Blocked).then(|| SchemaListEntry::from_schema(s, st))
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
        if let Some(s) = publication::get::<CachedSchema>(
            &self.db_ops.native_runtime(),
            self.catalog_id(),
            schema_name,
        )? {
            return Ok((s.schema.clone(), s.state));
        }
        let c = self.snapshot()?;
        Ok((
            c.schemas.get(schema_name).cloned(),
            c.states.get(schema_name).copied().unwrap_or_default(),
        ))
    }

    pub(crate) fn require_schema_metadata(&self, schema_name: &str) -> Result<Schema, SchemaError> {
        self.get_schema_metadata(schema_name)?
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))
    }

    pub fn schemas_with_newer_identity_algo(&self) -> Result<Vec<String>, SchemaError> {
        let mut names: Vec<String> = self
            .snapshot()?
            .schemas
            .iter()
            .filter(|(_, s)| s.identity_is_newer_than_binary())
            .map(|(n, _)| n.clone())
            .collect();
        names.sort();
        Ok(names)
    }

    pub fn get_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        Ok(self.merged()?.states)
    }

    pub(crate) fn require_queryable_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        (self.get_schema_and_state(schema_name)?.1 != SchemaState::Blocked)
            .then_some(())
            .ok_or_else(|| {
                SchemaError::Blocked(format!(
                    "Schema '{schema_name}' is blocked and cannot be queried"
                ))
            })
    }

    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.drop_with_receipt(schema_name, None).await
    }

    /// Apply a signed schema removal once, using bounded native cleanup transactions.
    pub async fn drop_replay_schema(
        &self,
        schema_name: &str,
        receipt_id: &[u8; 32],
        digest: &[u8; 32],
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime
            .replay_accepted(self.drop_with_receipt(
                schema_name,
                Some(&SchemaReplayDrop {
                    id: receipt_id,
                    digest,
                }),
            ))
            .await?;
        Ok(())
    }

    async fn drop_with_receipt(
        &self,
        schema_name: &str,
        replay: Option<&SchemaReplayDrop<'_>>,
    ) -> Result<bool, SchemaError> {
        use crate::db_operations::schema_store::SchemaDrop;
        let _drop = self.drops.lock().await;
        let SchemaDrop::Ready(schema) = self
            .db_ops
            .schemas()
            .block_for_drop(schema_name, replay, |s| {
                self.publish_catalog(schema_name.to_owned(), s.cloned(), SchemaState::Blocked)
            })
            .await?
        else {
            return Ok(false);
        };
        if let Some(schema) = schema {
            let r = self.db_ops.records().release_schema_rows(&schema).await?;
            tracing::info!(
                target: "fold_node::database",
                schema = schema_name, records = r.records, alternates = r.alternates,
                "schema drop released the stored rows of the schema"
            );
        }
        let existed = self
            .db_ops
            .schemas()
            .drop_reserved_schema(schema_name, replay, || {
                self.publish_catalog(schema_name.to_owned(), None, SchemaState::Available)
            })
            .await?;
        Ok(existed)
    }

    pub fn schema_names_owned_by(&self, owner_app: &str) -> Result<Vec<String>, SchemaError> {
        Ok(self
            .snapshot()?
            .schemas
            .iter()
            .filter(|(_, s)| s.owner_app_id.as_deref() == Some(owner_app))
            .map(|(n, _)| n.clone())
            .collect())
    }

    pub async fn resolve_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(s) = self.get_schema_metadata(schema_name)? {
            return Ok(Some(s));
        }
        if self
            .db_ops
            .schemas()
            .get_schema(schema_name)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        transaction::scope(self.db_ops.native_runtime(), async {
            let Some(schema) = self.db_ops.schemas().get_schema(schema_name).await? else {
                return Ok(None);
            };
            let state = self
                .db_ops
                .schemas()
                .get_schema_state(schema_name)
                .await?
                .unwrap_or_default();
            require_explicit_record_keys(&schema)?;
            self.publish_catalog(schema_name.to_owned(), Some(schema.clone()), state)?;
            Ok(Some(schema))
        })
        .await
    }

    pub async fn load_schema_internal(&self, mut schema: Schema) -> Result<(), SchemaError> {
        require_explicit_record_keys(&schema)?;
        schema.validate()?;
        schema.ensure_field_hashes();
        transaction::scope(self.db_ops.native_runtime(), self.save_schema(schema)).await
    }

    async fn save_schema(&self, mut schema: Schema) -> Result<(), SchemaError> {
        let existing = self.db_ops.schemas().get_schema(&schema.name).await?;
        stamp_identity_for_load(&mut schema, existing.as_ref());
        let refresh = existing.is_some();
        if let Some(stored) = existing {
            if schema.record_keys != stored.record_keys
                || schema.alternate_keys != stored.alternate_keys
            {
                return Err(SchemaError::InvalidData(
                    "explicit primary/alternate key declarations are immutable; declare a new schema and copy explicitly".into(),
                ));
            }
            schema.validate()?;
        }
        let name = schema.name.clone();
        self.db_ops.schemas().store_schema(&name, &schema).await?;
        let state = if refresh {
            match self.db_ops.schemas().get_schema_state(&name).await? {
                Some(st) => st,
                None => self.get_schema_and_state(&name)?.1,
            }
        } else {
            self.db_ops
                .schemas()
                .store_schema_state(&name, &SchemaState::Available)
                .await?;
            SchemaState::Available
        };
        self.publish_catalog(name, Some(schema), state)
    }

    fn publish_catalog(
        &self,
        key: String,
        schema: Option<Schema>,
        state: SchemaState,
    ) -> Result<(), SchemaError> {
        let catalog = Arc::clone(&self.catalog);
        let staged = CachedSchema { schema, state };
        publication::publish(
            &self.db_ops.native_runtime(),
            self.catalog_id(),
            key.clone(),
            staged.clone(),
            move || {
                let mut g = catalog.write().map_err(|e| {
                    StorageError::BackendError(format!("schema catalog lock poisoned: {e}"))
                })?;
                staged.update(Arc::make_mut(&mut *g), key);
                Ok(())
            },
        )?;
        Ok(())
    }
}

impl CachedSchema {
    fn update(&self, catalog: &mut SchemaCatalog, name: String) {
        if let Some(schema) = &self.schema {
            catalog.schemas.insert(name.clone(), schema.clone());
        } else {
            catalog.schemas.remove(&name);
        }
        if self.schema.is_some() || self.state == SchemaState::Blocked {
            catalog.states.insert(name, self.state);
        } else {
            catalog.states.remove(&name);
        }
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
            schema = %schema.name, stored_algo_version = stored, binary_algo_version = binary,
            identity_hash = %schema.identity_hash.as_deref().unwrap_or("<none>"),
            "refusing to downgrade schema identity minted by a newer algorithm",
        );
    }
    if let Some((s, stored)) = existing.and_then(|s| {
        ::schema_types::refuses_identity_downgrade(
            s.identity_hash.as_deref(),
            s.identity_hash_algo_version,
        )
        .map(|v| (s, v))
    }) {
        schema.identity_hash.clone_from(&s.identity_hash);
        schema.identity_hash_algo_version = s.identity_hash_algo_version;
        tracing::warn!(
            schema = %schema.name, stored_algo_version = stored,
            binary_algo_version = ::schema_types::IDENTITY_HASH_ALGO_VERSION,
            "refusing to overwrite a stored schema identity minted by a newer algorithm",
        );
    }
}
