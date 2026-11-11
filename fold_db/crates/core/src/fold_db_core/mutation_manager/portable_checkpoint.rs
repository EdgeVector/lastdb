//! Applied writer positions bound to one frozen native generation, plus portable schema binding.

use std::collections::BTreeMap;

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
use crate::storage::format::metadata::CheckpointDirectory;

/// Resolve once per schema and batch. The local schema carries the exact
/// runtime address to use for records and alternate pointers.
pub(super) struct PortableReplaySchema<'a> {
    id: &'a PortableSchemaId,
    local: &'a Schema,
    /// The record's definition when the local schema may hold fields that
    /// later definitions added. Its records use only its own fields.
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

    /// Bind a record's definition to the schema of its name, which may hold
    /// fields that later accepted definitions added.
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

    /// A mismatched identity, key, or field cannot advance replay progress.
    pub(super) fn validate_entry(&self, entry: &MutationEntry) -> Result<(), SchemaError> {
        if !matches!(&entry.schema, SchemaIdentity::Shared(id) if id == self.id.as_str()) {
            return Err(SchemaError::InvalidData(
                "portable mutation targets a different schema revision".into(),
            ));
        }
        let invalid_entry = |error: lastdb_model::portable_mutation::PortableMutationError| {
            SchemaError::InvalidData(error.to_string())
        };
        entry.validate().map_err(invalid_entry)?;
        entry
            .validate_keys(self.local.require_record_keys()?)
            .map_err(invalid_entry)?;
        if let MutationOperation::ReplaceRecord { data, .. } = &entry.operation {
            for (field, value) in &data.fields {
                let declared = self.declared.is_none_or(|definition| {
                    definition.fields.iter().flatten().any(|f| f == field)
                });
                if !declared
                    || validate_field_value(&self.local.name, self.local, field, value).is_err()
                {
                    return Err(SchemaError::InvalidData(
                        "portable mutation has an undeclared or wrongly typed field".into(),
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
    /// Freeze original native files and read the matching applied positions.
    /// The checkpoint carries the recorded definition of every shared alias.
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

    /// Wait for ordinary sync before selecting a cut with unaccepted writes.
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
            let mut lookups = Vec::with_capacity(definitions.len());
            for (name, definition) in &definitions {
                let binding = directory.schemas.get(name);
                lookups.push(async move {
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
                });
            }
            futures::future::try_join_all(lookups).await?;
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

    /// Bind all nonempty applied positions to exact authenticated accepted
    /// batch ends. Callers can fetch these ends after capturing the root.
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

fn invalid(reason: &str) -> SchemaError {
    SchemaError::InvalidData(reason.into())
}

fn detail(reason: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(reason.to_string())
}
