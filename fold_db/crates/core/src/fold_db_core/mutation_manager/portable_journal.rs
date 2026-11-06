//! Owner-facing local journal ports for shared database replication.

use super::{portable_schema::PortableReplaySchema, MutationManager};
use crate::db_operations::metadata_store::OutgoingPortablePage;
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::JournalSchema;
use cloud_backup_protocol::shared::{BatchReceipt, LogBatch, WriterStream};
use lastdb_model::canonical::CanonicalValue;
use lastdb_model::portable_mutation::{MutationEntry, MutationOperation, StreamProgress};
use lastdb_model::record_value::FileMarker;
use schema_types::PortableSchemaDefinition;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

impl MutationManager {
    /// Bind a new, explicitly registered writer to exact local schemas.
    pub async fn bind_portable_journal(
        &self,
        stream: WriterStream,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<(), SchemaError> {
        if definitions.is_empty() || definitions.len() > 64 {
            return Err(invalid("invalid shared journal schema count"));
        }
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let schemas =
                futures::future::try_join_all(definitions.iter().map(|(name, definition)| async {
                    let schema = self.load_schema_for_delta(name).await?;
                    PortableReplaySchema::bind(definition, definition.id(), &schema)?;
                    if self
                        .db_ops
                        .schemas()
                        .get_schema_retention_policy(name)
                        .await?
                        .is_some()
                    {
                        return Err(invalid("shared expiry policy is deferred"));
                    }
                    Ok((
                        schema.name.clone(),
                        JournalSchema {
                            id: definition.id().as_str().to_owned(),
                            local_sha256: schema_fingerprint(&schema)?,
                        },
                    ))
                }))
                .await?;
            let bound: BTreeMap<_, _> = schemas.into_iter().collect();
            if bound.len() != definitions.len() {
                return Err(invalid(
                    "shared schema aliases resolve to a duplicate local schema",
                ));
            }
            self.db_ops
                .metadata()
                .bind_portable_journal(stream, bound)
                .await
        })
        .await
    }

    /// The next bounded immutable prefix after this writer's accepted head.
    pub async fn portable_outgoing(&self) -> Result<OutgoingPortablePage, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().portable_outgoing())
            .await
    }

    /// The bound writer's durable local and cloud positions.
    pub async fn portable_writer_progress(
        &self,
    ) -> Result<(WriterStream, StreamProgress), SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().portable_writer_progress())
            .await
    }

    /// Map each bound immutable service identity to its local schema alias.
    pub async fn portable_writer_schemas(&self) -> Result<BTreeMap<String, String>, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(self.db_ops.metadata().portable_bound_schemas())
            .await
    }

    /// Read the exact sealed file version named by an outgoing entry.
    pub async fn portable_outgoing_file(
        &self,
        entry: &MutationEntry,
    ) -> Result<Vec<u8>, SchemaError> {
        let MutationOperation::ReplaceRecord { primary_key, data } = &entry.operation else {
            return Err(invalid("a delete has no portable file"));
        };
        let file = data
            .file
            .as_ref()
            .ok_or_else(|| invalid("outgoing entry has no file"))?;
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let name = self
                    .db_ops
                    .metadata()
                    .outgoing_schema_name(&entry.schema)
                    .await?;
                let schema = self.load_schema_for_delta(&name).await?;
                let records = self.db_ops.records();
                let address = records.record_address(&schema, &primary_key.clone().into())?;
                let marker = FileMarker {
                    name: file.name.clone(),
                    media_type: file.media_type.clone(),
                    size_bytes: file.size_bytes,
                    sha256: file.sha256.clone(),
                };
                records
                    .portable_file(&address, &file.version, &marker)
                    .await
            })
            .await
    }

    /// Advance after the authenticated service accepts this exact saved batch.
    pub async fn confirm_portable_upload(
        &self,
        batch: &LogBatch,
        receipt: &BatchReceipt,
    ) -> Result<(), SchemaError> {
        transaction::request_scope(
            self.db_ops.native_runtime(),
            self.db_ops
                .metadata()
                .stage_outgoing_receipt(batch, receipt),
        )
        .await
    }
}

pub(super) fn schema_fingerprint(schema: &Schema) -> Result<String, SchemaError> {
    let schema = serde_json::to_value(schema.to_wire())
        .map_err(|_| invalid("cannot encode the local shared schema"))?;
    let bytes = serde_json::to_vec(&CanonicalValue(&schema))
        .map_err(|_| invalid("cannot canonicalize the local shared schema"))?;
    Ok(lastdb_model::hex::hex_lower(Sha256::digest(bytes)))
}

fn invalid(detail: &str) -> SchemaError {
    SchemaError::InvalidData(detail.into())
}
