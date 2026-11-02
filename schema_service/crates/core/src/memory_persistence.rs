//! In-memory registry backend for the dev server, the eval tool and the
//! offline seed generator.
//!
//! Every record lives in a `HashMap` behind one lock, so nothing survives
//! a restart. Production uses the S3 backend (`schema_service_s3`). This
//! backend replaced the LastStore-backed local registry so local tools no
//! longer depend on a storage engine.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;

use crate::app_release::{channel_key, ChannelRecord, ReleaseRecord};
use crate::declared_fields::DeclaredFieldRecord;
use crate::external_persistence::ExternalSchemaPersistence;
use crate::lock_helpers::{read_lock, write_lock};
use crate::near_miss::NearMissRecord;
use crate::snapshot::AppRecord;
use crate::types::CanonicalField;
use crate::CanonicalVocabularyWrite;
use schema_types::{FoldDbError, FoldDbResult, Schema};

#[derive(Default)]
struct Registry {
    schemas: HashMap<String, Schema>,
    canonical_fields: HashMap<String, CanonicalField>,
    descriptive_name_embeddings: HashMap<String, Vec<f32>>,
    canonical_field_embeddings: HashMap<String, Vec<f32>>,
    near_misses: HashMap<String, NearMissRecord>,
    apps: HashMap<String, AppRecord>,
    releases: HashMap<String, ReleaseRecord>,
    channels: HashMap<String, ChannelRecord>,
    declared_fields: HashMap<String, DeclaredFieldRecord>,
}

/// In-memory implementation of [`ExternalSchemaPersistence`].
///
/// Keys match the S3 backend: schemas by `name`, canonical fields by name,
/// apps by `app_id`, releases by `release_id`, channels by
/// [`channel_key`], declarations by `declaration_id`.
#[derive(Default)]
pub struct InMemorySchemaPersistence {
    registry: RwLock<Registry>,
}

impl InMemorySchemaPersistence {
    fn read(&self) -> FoldDbResult<RwLockReadGuard<'_, Registry>> {
        read_lock(&self.registry, "in-memory registry")
    }

    fn write(&self) -> FoldDbResult<RwLockWriteGuard<'_, Registry>> {
        write_lock(&self.registry, "in-memory registry")
    }

    fn commit_schema_definitions(
        &self,
        schemas: &[Schema],
        fields: &[(String, CanonicalField)],
        vocabulary: Option<&CanonicalVocabularyWrite>,
    ) -> FoldDbResult<()> {
        let mut registry = self.write()?;
        if let Some(vocabulary) = vocabulary {
            vocabulary.verify(&registry.canonical_fields)?;
        }
        crate::schema_expansion_commit::validate_expansion_pairs(&registry.schemas, schemas)?;
        let canonical_fields = crate::field_name_policy::canonical_fields_after_insert(
            &registry.canonical_fields,
            fields,
        )?;
        registry.canonical_fields = canonical_fields;
        for schema in schemas {
            registry.schemas.insert(schema.name.clone(), schema.clone());
        }
        Ok(())
    }

    fn commit_declaration_definitions(
        &self,
        record: &DeclaredFieldRecord,
        fields: &[(String, CanonicalField)],
        expected: Option<&DeclaredFieldRecord>,
        vocabulary: Option<&CanonicalVocabularyWrite>,
    ) -> FoldDbResult<()> {
        let mut registry = self.write()?;
        if let Some(vocabulary) = vocabulary {
            vocabulary.verify(&registry.canonical_fields)?;
        }
        crate::declared_fields::validate_declaration_commit(
            &registry.declared_fields,
            record,
            expected,
        )?;
        let canonical_fields = crate::field_name_policy::canonical_fields_after_insert(
            &registry.canonical_fields,
            fields,
        )?;
        registry.canonical_fields = canonical_fields;
        registry
            .declared_fields
            .insert(record.declaration_id.clone(), record.clone());
        Ok(())
    }
}

#[async_trait]
impl ExternalSchemaPersistence for InMemorySchemaPersistence {
    async fn save_schema_definitions(
        &self,
        schemas: &[Schema],
        fields: &[(String, CanonicalField)],
    ) -> FoldDbResult<()> {
        self.commit_schema_definitions(schemas, fields, None)
    }

    async fn save_schema_definitions_if_vocabulary(
        &self,
        schemas: &[Schema],
        vocabulary: &CanonicalVocabularyWrite,
    ) -> FoldDbResult<()> {
        self.commit_schema_definitions(schemas, &vocabulary.fields, Some(vocabulary))
    }

    async fn load_all_schemas(&self) -> FoldDbResult<HashMap<String, Schema>> {
        Ok(self.read()?.schemas.clone())
    }

    async fn load_all_canonical_fields(&self) -> FoldDbResult<HashMap<String, CanonicalField>> {
        Ok(self.read()?.canonical_fields.clone())
    }

    async fn save_descriptive_name_embedding(
        &self,
        schema_hash: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()> {
        self.write()?
            .descriptive_name_embeddings
            .insert(schema_hash.to_string(), embedding.to_vec());
        Ok(())
    }

    async fn load_descriptive_name_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        Ok(self.read()?.descriptive_name_embeddings.clone())
    }

    async fn save_canonical_field_embedding(
        &self,
        field_name: &str,
        embedding: &[f32],
    ) -> FoldDbResult<()> {
        self.write()?
            .canonical_field_embeddings
            .insert(field_name.to_string(), embedding.to_vec());
        Ok(())
    }

    async fn load_canonical_field_embeddings(&self) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        Ok(self.read()?.canonical_field_embeddings.clone())
    }

    async fn append_near_miss(&self, record: &NearMissRecord) -> FoldDbResult<()> {
        // A repeated `registration_id` is a no-op (trait contract).
        self.write()?
            .near_misses
            .entry(record.registration_id.clone())
            .or_insert_with(|| record.clone());
        Ok(())
    }

    async fn load_all_near_misses(&self) -> FoldDbResult<Vec<NearMissRecord>> {
        Ok(self.read()?.near_misses.values().cloned().collect())
    }

    async fn save_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        // First-write-wins: never replace an existing `app_id`.
        self.write()?
            .apps
            .entry(app.app_id.clone())
            .or_insert_with(|| app.clone());
        Ok(())
    }

    async fn update_app(&self, app: &AppRecord) -> FoldDbResult<()> {
        self.write()?.apps.insert(app.app_id.clone(), app.clone());
        Ok(())
    }

    async fn load_all_apps(&self) -> FoldDbResult<HashMap<String, AppRecord>> {
        Ok(self.read()?.apps.clone())
    }

    async fn save_release(&self, record: &ReleaseRecord) -> FoldDbResult<()> {
        self.write()?
            .releases
            .insert(record.release_id.clone(), record.clone());
        Ok(())
    }

    async fn load_all_releases(&self) -> FoldDbResult<HashMap<String, ReleaseRecord>> {
        Ok(self.read()?.releases.clone())
    }

    async fn load_release(&self, release_id: &str) -> FoldDbResult<Option<ReleaseRecord>> {
        Ok(self.read()?.releases.get(release_id).cloned())
    }

    /// Same generation rule as the S3 backend's conditional write: a new
    /// record must be one past the stored generation.
    async fn save_channel(&self, record: &ChannelRecord) -> FoldDbResult<()> {
        let key = channel_key(&record.app_id, &record.channel);
        let mut registry = self.write()?;
        let stored = registry.channels.get(&key).map_or(0, |c| c.generation);
        if record.generation != stored + 1 {
            return Err(FoldDbError::GenerationConflict {
                expected: stored,
                supplied: record.generation,
            });
        }
        registry.channels.insert(key, record.clone());
        Ok(())
    }

    async fn load_all_channels(&self) -> FoldDbResult<HashMap<String, ChannelRecord>> {
        Ok(self.read()?.channels.clone())
    }

    async fn load_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> FoldDbResult<Option<ChannelRecord>> {
        Ok(self
            .read()?
            .channels
            .get(&channel_key(app_id, channel))
            .cloned())
    }

    async fn save_declared_field_definitions_if_vocabulary(
        &self,
        record: &DeclaredFieldRecord,
        vocabulary: &CanonicalVocabularyWrite,
        expected: Option<&DeclaredFieldRecord>,
    ) -> FoldDbResult<()> {
        self.commit_declaration_definitions(record, &vocabulary.fields, expected, Some(vocabulary))
    }

    async fn load_all_declared_fields(&self) -> FoldDbResult<Vec<DeclaredFieldRecord>> {
        Ok(self.read()?.declared_fields.values().cloned().collect())
    }

    /// `POST /v1/system/reset`: clear schemas only.
    async fn clear_all_schemas(&self) -> FoldDbResult<()> {
        self.write()?.schemas.clear();
        Ok(())
    }

    /// `POST /v1/snapshot/import`: clear schemas, canonical fields and apps
    /// before the import writes the snapshot records.
    async fn clear_registry_for_snapshot_import(&self) -> FoldDbResult<()> {
        let mut registry = self.write()?;
        registry.schemas.clear();
        registry.canonical_fields.clear();
        registry.apps.clear();
        Ok(())
    }
}
