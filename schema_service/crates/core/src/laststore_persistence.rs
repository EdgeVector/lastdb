//! Local registry backend: **Last Store** instead of sled.
//!
//! Schema service does not need FoldDB product conventions — only
//! put/get/list documents by collection. Collections are namespaced with
//! an `ss_` prefix so a store home is not confused with Mini's
//! `schemas`/`atoms`/`tips` layout if someone points both at one tree.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use laststore::LastStore;

use crate::declared_fields::DeclaredFieldRecord;
use crate::external_persistence::ExternalSchemaPersistence;
use crate::near_miss::NearMissRecord;
use crate::snapshot::AppRecord;
use crate::types::CanonicalField;
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};

/// Collection names for the schema-service registry inside a Last Store home.
pub mod collections {
    /// Content-addressed schemas (`schema.name` / identity hash).
    pub const SCHEMAS: &str = "ss_schemas";
    /// Canonical field registry.
    pub const CANONICAL_FIELDS: &str = "ss_canonical_fields";
    /// Phase C near-miss audit log.
    pub const NEAR_MISSES: &str = "ss_near_misses";
    /// App identity registry.
    pub const APPS: &str = "ss_apps";
    /// Published app releases — release id -> record (`/v2` registry).
    pub const APP_RELEASES: &str = "ss_app_releases";
    /// Release channels — `<app_id>\x1f<channel>` -> record (`/v2` registry).
    pub const APP_CHANNELS: &str = "ss_app_channels";
    /// Declared fields — declaration id -> record
    /// (brain `design-lastdb-declared-fields`).
    pub const DECLARED_FIELDS: &str = "ss_declared_fields";
    /// Descriptive-name embeddings (key = schema identity hash).
    pub const DESC_EMBEDDINGS: &str = "ss_desc_name_embeddings";
    /// Canonical field embeddings (key = field name).
    pub const CF_EMBEDDINGS: &str = "ss_canonical_field_embeddings";
}

/// Last Store implementation of [`ExternalSchemaPersistence`].
pub struct LastStoreSchemaPersistence {
    store: Arc<LastStore>,
}

impl LastStoreSchemaPersistence {
    /// Open (or create) a Last Store home at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> FoldDbResult<Self> {
        let store = LastStore::open(path.as_ref()).map_err(|e| map_ls_err(&e))?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    fn put_json<T: serde::Serialize>(
        &self,
        collection: &str,
        id: &str,
        value: &T,
    ) -> FoldDbResult<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| FoldDbError::Serialization(format!("serialize {collection}/{id}: {e}")))?;
        self.store
            .put(collection, id, &bytes)
            .map_err(|e| map_ls_err(&e))?;
        self.store.flush().map_err(|e| map_ls_err(&e))?;
        Ok(())
    }

    fn load_all_json<T: serde::de::DeserializeOwned>(
        &self,
        collection: &str,
    ) -> FoldDbResult<HashMap<String, T>> {
        let rows = self
            .store
            .list_prefix(collection, "")
            .map_err(|e| map_ls_err(&e))?;
        let mut out = HashMap::with_capacity(rows.len());
        for (id, bytes) in rows {
            let value: T = serde_json::from_slice(&bytes)
                .map_err(|e| FoldDbError::Config(format!("parse {collection}/{id}: {e}")))?;
            out.insert(id, value);
        }
        Ok(out)
    }

    fn put_f32_le(&self, collection: &str, id: &str, embedding: &[f32]) -> FoldDbResult<()> {
        let mut bytes = Vec::with_capacity(embedding.len() * 4);
        for f in embedding {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        self.store
            .put(collection, id, &bytes)
            .map_err(|e| map_ls_err(&e))?;
        self.store.flush().map_err(|e| map_ls_err(&e))?;
        Ok(())
    }

    fn load_all_f32_le(&self, collection: &str) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        let rows = self
            .store
            .list_prefix(collection, "")
            .map_err(|e| map_ls_err(&e))?;
        let mut out = HashMap::with_capacity(rows.len());
        for (id, bytes) in rows {
            if bytes.len() % 4 != 0 {
                return Err(FoldDbError::Config(format!(
                    "embedding {collection}/{id}: length {} not multiple of 4",
                    bytes.len()
                )));
            }
            let mut vec = Vec::with_capacity(bytes.len() / 4);
            for chunk in bytes.chunks_exact(4) {
                vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            out.insert(id, vec);
        }
        Ok(out)
    }

    /// Delete every document in a collection (dev reset / snapshot import).
    pub fn clear_collection(&self, collection: &str) -> FoldDbResult<()> {
        let keys = self
            .store
            .list_prefix_keys(collection, "")
            .map_err(|e| map_ls_err(&e))?;
        for id in keys {
            self.store
                .delete(collection, &id)
                .map_err(|e| map_ls_err(&e))?;
        }
        self.store.flush().map_err(|e| map_ls_err(&e))?;
        Ok(())
    }

    /// Clear schemas only (matches historical sled reset endpoint).
    pub fn clear_schemas(&self) -> FoldDbResult<()> {
        self.clear_collection(collections::SCHEMAS)
    }

    /// Clear schemas + canonical fields + apps (snapshot import wipe).
    pub fn clear_registry_core(&self) -> FoldDbResult<()> {
        self.clear_collection(collections::SCHEMAS)?;
        self.clear_collection(collections::CANONICAL_FIELDS)?;
        self.clear_collection(collections::APPS)?;
        Ok(())
    }
}

fn map_ls_err(e: &laststore::Error) -> FoldDbError {
    FoldDbError::Config(format!("laststore: {e}"))
}

#[async_trait]
impl ExternalSchemaPersistence for LastStoreSchemaPersistence {
    async fn save_schema(&self, schema: &Schema) -> FoldDbResult<()> {
        self.put_json(collections::SCHEMAS, &schema.name, schema)
    }

    async fn save_schemas(&self, schemas: &[Schema]) -> FoldDbResult<()> {
        for schema in schemas {
            let bytes = serde_json::to_vec(schema).map_err(|e| {
                FoldDbError::Serialization(format!("serialize schema '{}': {e}", schema.name))
            })?;
            self.store
                .put(collections::SCHEMAS, &schema.name, &bytes)
                .map_err(|e| map_ls_err(&e))?;
        }
        self.store.flush().map_err(|e| map_ls_err(&e))?;
        Ok(())
    }

    async fn load_all_schemas(&self) -> FoldDbResult<HashMap<String, Schema>> {
        self.load_all_json(collections::SCHEMAS)
    }

    async fn save_canonical_field(&self, name: &str, field: &CanonicalField) -> FoldDbResult<()> {
        self.put_json(collections::CANONICAL_FIELDS, name, field)
    }

    async fn save_canonical_fields(&self, fields: &[(String, CanonicalField)]) -> FoldDbResult<()> {
        for (name, field) in fields {
            let bytes = serde_json::to_vec(field).map_err(|e| {
                FoldDbError::Serialization(format!("serialize canonical field '{name}': {e}"))
            })?;
            self.store
                .put(collections::CANONICAL_FIELDS, name, &bytes)
                .map_err(|e| map_ls_err(&e))?;
        }
        self.store.flush().map_err(|e| map_ls_err(&e))?;
        Ok(())
    }

    async fn load_all_canonical_fields(&self) -> FoldDbResult<HashMap<String, CanonicalField>> {
        self.load_all_json(collections::CANONICAL_FIELDS)
    }

    async fn save_descriptive_name_embedding(
        &self,
        schema_hash: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()> {
        self.put_f32_le(collections::DESC_EMBEDDINGS, schema_hash, embedding)
    }

    async fn load_descriptive_name_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        self.load_all_f32_le(collections::DESC_EMBEDDINGS)
    }

    async fn save_canonical_field_embedding(
        &self,
        field_name: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()> {
        self.put_f32_le(collections::CF_EMBEDDINGS, field_name, embedding)
    }

    async fn load_canonical_field_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        self.load_all_f32_le(collections::CF_EMBEDDINGS)
    }

    async fn append_near_miss(&self, record: &NearMissRecord) -> FoldDbResult<()> {
        // NearMissRecord has registration_id
        let id = record.registration_id.clone();
        if self
            .store
            .exists(collections::NEAR_MISSES, &id)
            .map_err(|e| map_ls_err(&e))?
        {
            return Ok(());
        }
        self.put_json(collections::NEAR_MISSES, &id, record)
    }

    async fn load_all_near_misses(&self) -> FoldDbResult<Vec<NearMissRecord>> {
        let map: HashMap<String, NearMissRecord> = self.load_all_json(collections::NEAR_MISSES)?;
        Ok(map.into_values().collect())
    }

    async fn save_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        if self
            .store
            .exists(collections::APPS, &app.app_id)
            .map_err(|e| map_ls_err(&e))?
        {
            // first-write-wins
            return Ok(());
        }
        self.put_json(collections::APPS, &app.app_id, app)
    }

    async fn update_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        self.put_json(collections::APPS, &app.app_id, app)
    }

    async fn load_all_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        self.load_all_json(collections::APPS)
    }

    async fn save_release(&self, record: &crate::app_release::ReleaseRecord) -> FoldDbResult<()> {
        // Upsert: the manifest is immutable and the id is its digest, so the
        // only byte that can differ on a re-save is the revocation stamp.
        self.put_json(collections::APP_RELEASES, &record.release_id, record)
    }

    async fn load_all_releases(
        &self,
    ) -> FoldDbResult<HashMap<String, crate::app_release::ReleaseRecord>> {
        self.load_all_json(collections::APP_RELEASES)
    }

    async fn save_channel(&self, record: &crate::app_release::ChannelRecord) -> FoldDbResult<()> {
        self.put_json(
            collections::APP_CHANNELS,
            &crate::app_release::channel_key(&record.app_id, &record.channel),
            record,
        )
    }

    async fn load_all_channels(
        &self,
    ) -> FoldDbResult<HashMap<String, crate::app_release::ChannelRecord>> {
        self.load_all_json(collections::APP_CHANNELS)
    }

    async fn save_declared_field(&self, record: &DeclaredFieldRecord) -> FoldDbResult<()> {
        // Upsert, not insert-if-absent: a re-declare legitimately adds fields
        // or grants to an existing declaration, and the id is immutable, so
        // the row can only ever grow.
        self.put_json(collections::DECLARED_FIELDS, &record.declaration_id, record)
    }

    async fn load_all_declared_fields(&self) -> FoldDbResult<Vec<DeclaredFieldRecord>> {
        let map: HashMap<String, DeclaredFieldRecord> =
            self.load_all_json(collections::DECLARED_FIELDS)?;
        Ok(map.into_values().collect())
    }

    async fn clear_all_schemas(&self) -> FoldDbResult<()> {
        self.clear_schemas()
    }

    async fn clear_registry_for_snapshot_import(&self) -> FoldDbResult<()> {
        self.clear_registry_core()
    }
}
