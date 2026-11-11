//! Portable checkpoint capture, schema binding, and shared definition installation.

use std::collections::{BTreeMap, BTreeSet};

use cloud_backup_protocol::shared::{LogHead, StreamCover, WriterStream};
use cloud_backup_protocol::Id;
use lastdb_model::portable_mutation::{
    MutationEntry, MutationOperation, SchemaIdentity, StreamId, StreamProgress,
};
use schema_types::{DeclarativeSchemaDefinition, PortableSchemaDefinition, PortableSchemaId};

use super::portable_journal::schema_fingerprint;
use super::validation::validate_field_value;
use super::MutationManager;
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::byte_backup::SharedRunCapture;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::{CheckpointDirectory, DefinitionSet};

pub(super) struct PortableReplaySchema<'a> {
    id: &'a PortableSchemaId,
    local: &'a Schema,
    declared: Option<&'a DeclarativeSchemaDefinition>,
}

impl<'a> PortableReplaySchema<'a> {
    pub(super) fn bind(
        definition: &'a PortableSchemaDefinition,
        expected: &PortableSchemaId,
        local: &'a Schema,
    ) -> Result<Self, SchemaError> {
        definition
            .bind_local_alias(expected, &local.to_wire())
            .map_err(SchemaError::InvalidData)?;
        Ok(Self {
            id: definition.id(),
            local,
            declared: None,
        })
    }

    pub(super) fn bind_version(
        definition: &'a PortableSchemaDefinition,
        expected: &PortableSchemaId,
        local: &'a Schema,
    ) -> Result<Self, SchemaError> {
        let declared = definition
            .bind_local_version(expected, &local.to_wire())
            .map_err(SchemaError::InvalidData)?;
        Ok(Self {
            id: definition.id(),
            local,
            declared: Some(declared),
        })
    }

    pub(super) fn validate_entry(&self, entry: &MutationEntry) -> Result<(), SchemaError> {
        if !matches!(&entry.schema, SchemaIdentity::Shared(id) if id == self.id.as_str()) {
            return Err(invalid(
                "portable mutation targets a different schema revision",
            ));
        }
        entry.validate().map_err(detail)?;
        entry
            .validate_keys(self.local.require_record_keys()?)
            .map_err(detail)?;
        if let MutationOperation::ReplaceRecord { data, .. } = &entry.operation {
            for (field, value) in &data.fields {
                let declared = self.declared.is_none_or(|definition| {
                    definition.fields.iter().flatten().any(|f| f == field)
                });
                if !declared
                    || validate_field_value(&self.local.name, self.local, field, value).is_err()
                {
                    return Err(invalid(
                        "portable mutation has an undeclared or wrongly typed field",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Logical metadata of the immutable native generation.
pub struct PortableCheckpoint {
    directory: CheckpointDirectory,
    definitions: BTreeMap<String, PortableSchemaDefinition>,
}

impl MutationManager {
    pub async fn try_freeze_portable_checkpoint(
        &self,
        snapshot: uuid::Uuid,
        key: &[u8; 32],
    ) -> Result<Option<(PortableCheckpoint, SharedRunCapture)>, SchemaError> {
        self.db_ops
            .native_runtime()
            .freeze_shared_checkpoint(snapshot, key, self.try_capture_portable_checkpoint())
            .await
    }

    async fn try_capture_portable_checkpoint(
        &self,
    ) -> Result<Option<PortableCheckpoint>, SchemaError> {
        let root = self.db_ops.read_scope().await?;
        root.run_result(async {
            let metadata = self.db_ops.metadata();
            let Some(directory) = metadata.portable_checkpoint_directory().await? else {
                return Ok(None);
            };
            let definitions = metadata.portable_definitions().await?;
            self.db_ops
                .native_runtime()
                .require_shared_schema_set(definitions.keys().map(String::as_str))?;
            futures::future::try_join_all(definitions.iter().map(|(name, definition)| {
                let binding = directory.schemas.get(name);
                async move {
                    let schema = self.load_schema_for_delta(name).await?;
                    if let Some(binding) = binding {
                        if definition.id().as_str() != binding.id
                            || schema_fingerprint(&schema)? != binding.local_sha256
                        {
                            return Err(invalid("checkpoint schema differs from its journal"));
                        }
                    }
                    PortableReplaySchema::bind(definition, definition.id(), &schema)?;
                    if self
                        .db_ops
                        .schemas()
                        .get_schema_retention_policy(name)
                        .await?
                        .is_some()
                    {
                        return Err(invalid("shared checkpoint retention policy is deferred"));
                    }
                    Ok(())
                }
            }))
            .await?;
            if directory
                .schemas
                .keys()
                .any(|name| !definitions.contains_key(name))
            {
                return Err(invalid("checkpoint schema definition is missing"));
            }
            Ok(Some(PortableCheckpoint {
                directory,
                definitions,
            }))
        })
        .await
    }

    pub async fn portable_accepted_definitions(
        &self,
    ) -> Result<BTreeMap<String, Vec<PortableSchemaDefinition>>, SchemaError> {
        let sets = self
            .db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().portable_definition_sets())
            .await?;
        Ok(sets
            .into_iter()
            .map(|(alias, set)| (alias, set.accepted.into_values().collect()))
            .collect())
    }

    pub async fn record_portable_definitions(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(
            self.db_ops.native_runtime(),
            self.stage_recorded_definitions(definitions),
        )
        .await
    }

    pub(super) async fn stage_recorded_definitions(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        runtime.require_shared_schema_subset(definitions.keys().map(String::as_str))?;
        futures::future::try_join_all(definitions.iter().map(|(alias, definition)| async move {
            let schema = self.load_schema_for_delta(alias).await?;
            PortableReplaySchema::bind(definition, definition.id(), &schema)?;
            Ok::<_, SchemaError>(())
        }))
        .await?;
        let metadata = self.db_ops.metadata();
        let recorded = metadata.portable_definitions().await?;
        if runtime
            .shared_schema_aliases()
            .iter()
            .any(|alias| !recorded.contains_key(alias) && !definitions.contains_key(alias))
        {
            return Err(invalid("every shared alias requires its schema definition"));
        }
        Ok(metadata.stage_portable_definitions(definitions).await?)
    }

    pub async fn install_portable_definitions(
        &self,
        definitions: &[PortableSchemaDefinition],
    ) -> Result<(), SchemaError> {
        if !definitions.is_empty() {
            transaction::request_scope(
                self.db_ops.native_runtime(),
                self.stage_log_definitions(definitions),
            )
            .await?;
        }
        self.follow_portable_definitions().await
    }

    async fn stage_log_definitions(
        &self,
        definitions: &[PortableSchemaDefinition],
    ) -> Result<(), SchemaError> {
        let metadata = self.db_ops.metadata();
        let mut sets = metadata.portable_definition_sets().await?;
        let mut changed = BTreeSet::new();
        let mut added = BTreeMap::<String, Vec<PortableSchemaDefinition>>::new();
        for definition in definitions {
            definition.verify(definition.id()).map_err(detail)?;
            if sets
                .values()
                .any(|set| set.accepted.contains_key(definition.id()))
            {
                continue;
            }
            let name = definition.canonical_name();
            let mut holders = sets
                .iter_mut()
                .filter(|(_, set)| set.current.canonical_name() == name);
            match (holders.next(), holders.next()) {
                (Some((alias, set)), None) => {
                    set.accepted
                        .insert(definition.id().clone(), definition.clone());
                    changed.insert(alias.clone());
                }
                (None, _) => {
                    let versions = added.entry(name.to_owned()).or_default();
                    if versions.iter().all(|known| known.id() != definition.id()) {
                        versions.push(definition.clone());
                    }
                }
                (Some(_), Some(_)) => {
                    return Err(invalid("two shared aliases hold one shared schema name"));
                }
            }
        }
        let mut staged = BTreeMap::new();
        let mut schemas = Vec::new();
        for alias in changed {
            let mut set = sets
                .remove(&alias)
                .ok_or_else(|| invalid("a changed shared alias has no definitions"))?;
            let accepted: Vec<_> = set.accepted.values().cloned().collect();
            let current = PortableSchemaDefinition::current_of(&accepted).map_err(detail)?;
            if current.id() != set.current.id() {
                schemas.push(local_schema(&alias, &current)?);
                set = DefinitionSet::new(current, accepted);
            }
            staged.insert(alias, set);
        }
        for (name, versions) in &added {
            if sets.contains_key(name) || self.db_ops.schemas().get_schema(name).await?.is_some() {
                return Err(invalid(
                    "a shared schema definition names a schema already in this home",
                ));
            }
            let current = PortableSchemaDefinition::current_of(versions).map_err(detail)?;
            schemas.push(local_schema(name, &current)?);
            staged.insert(name.clone(), DefinitionSet::new(current, versions.clone()));
        }
        if staged.is_empty() {
            return Ok(());
        }
        metadata
            .stage_shared_aliases(added.keys().map(String::as_str))
            .await?;
        metadata.stage_definition_sets(&staged).await?;
        self.schema_manager.load_schema_batch(schemas).await
    }
}

impl PortableCheckpoint {
    pub fn definitions(&self) -> &BTreeMap<String, PortableSchemaDefinition> {
        &self.definitions
    }

    pub fn writer(&self) -> &WriterStream {
        &self.directory.writer
    }

    pub fn progress(&self) -> &BTreeMap<StreamId, StreamProgress> {
        &self.directory.progress
    }

    pub fn delete_reclaimed_through_ms(&self) -> Option<u64> {
        self.directory.delete_reclaimed_through_ms
    }

    pub fn covers(
        &self,
        accepted: &BTreeMap<StreamId, LogHead>,
    ) -> Result<Vec<StreamCover>, SchemaError> {
        let covers = self
            .progress()
            .iter()
            .filter(|(_, progress)| progress.applied_through != 0)
            .map(|(stream, progress)| {
                let head = accepted
                    .get(stream)
                    .ok_or_else(|| invalid("checkpoint accepted end is missing"))?;
                if head.sequence.get() != progress.applied_through
                    || (stream.as_str() == self.writer().source_stream.as_str()
                        && self.directory.local_head.as_ref() != Some(head))
                {
                    return Err(invalid("accepted end differs from checkpoint position"));
                }
                head.validate().map_err(detail)?;
                Ok(StreamCover {
                    source_stream: Id::try_from(stream.as_str().to_owned()).map_err(detail)?,
                    through: head.clone(),
                })
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        if covers.len() != accepted.len() {
            return Err(invalid("checkpoint accepted ends contain extra streams"));
        }
        Ok(covers)
    }
}

fn local_schema(alias: &str, current: &PortableSchemaDefinition) -> Result<Schema, SchemaError> {
    let mut local = current.verify(current.id()).map_err(detail)?.clone();
    local.name = alias.to_owned();
    Ok(Schema::from(local))
}

fn invalid(reason: &str) -> SchemaError {
    SchemaError::InvalidData(reason.into())
}

fn detail(reason: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(reason.to_string())
}
