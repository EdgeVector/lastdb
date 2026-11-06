//! A bounded, read-only export of one committed shared database root.

use super::MutationManager;
use super::{portable_journal::schema_fingerprint, portable_schema::PortableReplaySchema};
use crate::db_operations::{record_store::RangeWalk, DbOperations};
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::PackReadScope;
use crate::storage::format::metadata::CheckpointDirectory;
use crate::storage::format::records::{delete_record_key, portable_record_data, DELETE_AGE_FAMILY};
use cloud_backup_protocol::shared::{LogHead, StreamCover, WriterStream};
use cloud_backup_protocol::Id;
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::portable_mutation::{
    MutationEntry, MutationOperation, Position, RecordKey, SchemaIdentity, Stamp, StreamId,
    StreamProgress,
};
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::{FileMarker, StoredRecord};
use schema_types::PortableSchemaDefinition;
use std::collections::BTreeMap;
use std::sync::Arc;

const PAGE_ROWS: usize = 32;

pub struct CheckpointRow {
    local_schema: String,
    entry: MutationEntry,
}

impl CheckpointRow {
    pub fn local_schema(&self) -> &str {
        &self.local_schema
    }

    pub fn entry(&self) -> &MutationEntry {
        &self.entry
    }
}

/// Holds the captured native files until every selected page and file is read.
/// It never pins a write gate, and local-only state never enters its pages.
pub struct PortableCheckpoint {
    db_ops: Arc<DbOperations>,
    root: PackReadScope,
    directory: CheckpointDirectory,
    schemas: BTreeMap<String, Schema>,
    definitions: BTreeMap<String, PortableSchemaDefinition>,
    spaces: BTreeMap<String, String>,
    deletes: RangeWalk,
    records: Vec<(String, RangeWalk)>,
}

impl MutationManager {
    /// Capture one immutable root with an already accepted local journal prefix.
    pub async fn capture_portable_checkpoint(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<PortableCheckpoint, SchemaError> {
        self.try_capture_portable_checkpoint(definitions)
            .await?
            .ok_or_else(|| invalid("checkpoint has an unaccepted local prefix"))
    }

    /// Wait for ordinary sync before selecting a cut with unaccepted writes.
    pub async fn try_capture_portable_checkpoint(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<Option<PortableCheckpoint>, SchemaError> {
        let root = self.db_ops.read_scope().await?;
        let captured = root
            .run_result(async {
                let Some(directory) = self
                    .db_ops
                    .metadata()
                    .portable_checkpoint_directory()
                    .await?
                else {
                    return Ok(None);
                };
                if definitions.len() != directory.schemas.len() {
                    return Err(invalid("checkpoint schema set differs from its journal"));
                }
                let schemas = futures::future::try_join_all(directory.schemas.iter().map(
                    |(name, binding)| async move {
                        let definition = definitions
                            .get(name)
                            .ok_or_else(|| invalid("checkpoint schema definition is missing"))?;
                        let schema = self.load_schema_for_delta(name).await?;
                        if definition.id().as_str() != binding.id
                            || schema_fingerprint(&schema)? != binding.local_sha256
                        {
                            return Err(invalid("checkpoint schema differs from its journal"));
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
                        Ok((name.clone(), schema))
                    },
                ))
                .await?
                .into_iter()
                .collect::<BTreeMap<_, _>>();
                Ok(Some((directory, schemas)))
            })
            .await?;
        let Some((directory, schemas)) = captured else {
            return Ok(None);
        };
        let spaces: BTreeMap<_, _> = schemas
            .keys()
            .map(|name| (record_space_uuid(name), name.clone()))
            .collect();
        let records = spaces
            .iter()
            .rev()
            .map(|(space, name)| (name.clone(), walk(&format!("{RECORD_PREFIX}{space}:"))))
            .collect();
        Ok(Some(PortableCheckpoint {
            db_ops: Arc::clone(&self.db_ops),
            root,
            directory,
            schemas,
            definitions: definitions.clone(),
            spaces,
            deletes: walk(&format!("{DELETE_AGE_FAMILY}:")),
            records,
        }))
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

    /// Bounded pages visit retained deletes first so a staged writer can
    /// preserve a delete marker alongside a newer live replacement.
    pub async fn next_page(&mut self) -> Result<Option<Vec<CheckpointRow>>, SchemaError> {
        let records = self.db_ops.records();
        let runtime = self.db_ops.native_runtime();
        let page = self.root.checkpoint_page(&runtime)?;
        if !self.deletes.done() {
            let mut walk = self.deletes.clone();
            let rows = page.run_shared(records.walk_step(&mut walk)).await?.rows;
            let entries = self
                .root
                .checkpoint_page(&runtime)?
                .run_result(self.delete_rows(rows))
                .await?;
            self.deletes = walk;
            return Ok(Some(entries));
        }
        while self.records.last().is_some_and(|(_, walk)| walk.done()) {
            self.records.pop();
        }
        let Some((name, current)) = self.records.last() else {
            return Ok(None);
        };
        let mut walk = current.clone();
        let rows = page.run_shared(records.walk_step(&mut walk)).await?.rows;
        let entries = rows
            .into_iter()
            .map(|(key, value)| {
                let record = StoredRecord::decode(&value).map_err(detail)?;
                let address = records.record_address(&self.schemas[name], &record.key)?;
                if address.as_str().as_bytes() != key {
                    return Err(invalid("checkpoint record has the wrong storage address"));
                }
                let data = portable_record_data(&record)?;
                if let Some(file) = &data.file {
                    self.require_position(&file.version)?;
                }
                self.row(
                    name,
                    record.stamp,
                    MutationOperation::ReplaceRecord {
                        primary_key: RecordKey::try_from(record.key).map_err(detail)?,
                        data,
                    },
                )
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        if let Some((_, current)) = self.records.last_mut() {
            *current = walk;
        }
        Ok(Some(entries))
    }

    /// Read the authenticated sealed file version from that same root.
    pub async fn file(&self, row: &CheckpointRow) -> Result<Vec<u8>, SchemaError> {
        let MutationOperation::ReplaceRecord { primary_key, data } = &row.entry.operation else {
            return Err(invalid("checkpoint delete has no file"));
        };
        let file = data
            .file
            .as_ref()
            .ok_or_else(|| invalid("checkpoint record has no file"))?;
        self.require_position(&file.version)?;
        let schema = self
            .schemas
            .get(&row.local_schema)
            .ok_or_else(|| invalid("file schema is outside the checkpoint"))?;
        let records = self.db_ops.records();
        let address = records.record_address(schema, &primary_key.clone().into())?;
        let marker = FileMarker {
            name: file.name.clone(),
            media_type: file.media_type.clone(),
            size_bytes: file.size_bytes,
            sha256: file.sha256.clone(),
        };
        self.root
            .checkpoint_page(&self.db_ops.native_runtime())?
            .run_result(records.portable_file(&address, &file.version, &marker))
            .await
    }

    async fn delete_rows(
        &self,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<CheckpointRow>, SchemaError> {
        let records = self.db_ops.records();
        let active = records.current_delete_ages(rows).await?;
        active
            .into_iter()
            .map(|age| {
                let physical = delete_record_key(&age.marker)
                    .ok_or_else(|| invalid("checkpoint marker has no record address"))?;
                let (space, _) = physical
                    .strip_prefix(RECORD_PREFIX)
                    .and_then(|rest| rest.split_once(':'))
                    .ok_or_else(|| invalid("checkpoint delete has an invalid space"))?;
                let name = self
                    .spaces
                    .get(space)
                    .ok_or_else(|| invalid("shared delete schema is outside the checkpoint"))?;
                let primary = age.primary.ok_or_else(|| {
                    invalid("an active legacy shared delete has no exportable primary coordinates")
                })?;
                let address =
                    records.record_address(&self.schemas[name], &primary.clone().into())?;
                if address.as_str() != physical {
                    return Err(invalid("checkpoint delete has the wrong storage address"));
                }
                self.row(
                    name,
                    age.stamp,
                    MutationOperation::DeleteRecord {
                        primary_key: primary,
                    },
                )
            })
            .collect()
    }

    fn row(
        &self,
        name: &str,
        stamp: Stamp,
        operation: MutationOperation,
    ) -> Result<CheckpointRow, SchemaError> {
        let progress = self.require_position(&stamp.position)?;
        if progress
            .applied_through_ms
            .is_none_or(|through| stamp.authored_at_ms > through)
        {
            return Err(invalid("record stamp exceeds the checkpoint timestamp"));
        }
        let entry = MutationEntry {
            stamp,
            schema: SchemaIdentity::Shared(self.directory.schemas[name].id.clone()),
            operation,
        };
        entry.validate().map_err(detail)?;
        entry
            .validate_keys(self.schemas[name].require_record_keys()?)
            .map_err(detail)?;
        Ok(CheckpointRow {
            local_schema: name.to_owned(),
            entry,
        })
    }

    fn require_position(&self, position: &Position) -> Result<&StreamProgress, SchemaError> {
        self.progress()
            .get(&position.source_stream)
            .filter(|progress| {
                position.sequence > 0 && position.sequence <= progress.applied_through
            })
            .ok_or_else(|| invalid("record version is outside the checkpoint prefixes"))
    }
}

fn walk(prefix: &str) -> RangeWalk {
    let (start, end) = exact_prefix_bounds(prefix);
    RangeWalk::new(start.as_bytes(), end.as_bytes(), PAGE_ROWS)
}

fn invalid(reason: &str) -> SchemaError {
    SchemaError::InvalidData(reason.into())
}

fn detail(reason: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(reason.to_string())
}
