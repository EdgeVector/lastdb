//! Schema registry service, RCU catalog snapshots, and core lookup behaviors.

mod lifecycle;
pub mod types;

use crate::storage::disk_pack::transaction::publication;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub use crate::limits::manifest_retention::{
    self, ManifestRetention, ManifestRetentionOutcome, ManifestRetentionPlan,
    ManifestRetentionStatus,
};
pub use crate::schema::types::catalog_types as schema_types;
pub use crate::schema::types::catalog_types::{
    RetentionSource, SchemaListEntry, SchemaRetentionPolicy, SchemaState, SchemaWithState,
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
pub(super) struct SchemaCatalog {
    pub(super) schemas: HashMap<String, Schema>,
    pub(super) states: HashMap<String, SchemaState>,
}

#[derive(Clone)]
pub(super) struct CachedSchema {
    pub(super) schema: Option<Schema>,
    pub(super) state: SchemaState,
}

pub struct SchemaCore {
    pub(super) catalog: Arc<RwLock<Arc<SchemaCatalog>>>,
    pub(super) db_ops: Arc<crate::db_operations::DbOperations>,
    pub(super) drops: tokio::sync::Mutex<()>,
}

impl SchemaCore {
    pub async fn new(db_ops: Arc<crate::db_operations::DbOperations>) -> Result<Self, SchemaError> {
        let schemas = db_ops.schemas().get_all_schemas().await?;
        for schema in schemas.values() {
            lifecycle::require_explicit_record_keys(schema)?;
        }
        let states = db_ops.schemas().get_all_schema_states().await?;
        Ok(Self {
            catalog: Arc::new(RwLock::new(Arc::new(SchemaCatalog { schemas, states }))),
            db_ops,
            drops: tokio::sync::Mutex::new(()),
        })
    }

    pub(super) fn snapshot(&self) -> Result<Arc<SchemaCatalog>, SchemaError> {
        self.catalog
            .read()
            .map(|g| Arc::clone(&g))
            .map_err(|e| SchemaError::InvalidData(format!("schema catalog lock poisoned: {e}")))
    }

    pub(super) fn catalog_id(&self) -> usize {
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

    pub fn is_schema_blocked(&self, schema_name: &str) -> Result<bool, SchemaError> {
        if let Some(s) = publication::get::<CachedSchema>(
            &self.db_ops.native_runtime(),
            self.catalog_id(),
            schema_name,
        )? {
            return Ok(s.state == SchemaState::Blocked);
        }
        Ok(self
            .snapshot()?
            .states
            .get(schema_name)
            .copied()
            .unwrap_or_default()
            == SchemaState::Blocked)
    }

    pub fn schema_and_state(
        &self,
        schema_name: &str,
    ) -> Result<Option<(Schema, SchemaState)>, SchemaError> {
        let (schema, state) = self.get_schema_and_state(schema_name)?;
        Ok(schema.map(|s| (s, state)))
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
            return Ok((s.schema, s.state));
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
        (!self.is_schema_blocked(schema_name)?)
            .then_some(())
            .ok_or_else(|| {
                SchemaError::Blocked(format!(
                    "Schema '{schema_name}' is blocked and cannot be queried"
                ))
            })
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
}

impl CachedSchema {
    pub(super) fn update(&self, catalog: &mut SchemaCatalog, name: String) {
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
