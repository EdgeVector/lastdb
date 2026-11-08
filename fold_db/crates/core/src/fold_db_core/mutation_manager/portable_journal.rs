//! Owner-facing local journal ports for shared database replication.

use super::{portable_schema::PortableReplaySchema, MutationManager};
use crate::db_operations::metadata_store::OutgoingPortablePage;
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::JournalSchema;
use cloud_backup_protocol::shared::{BatchReceipt, LogBatch, WriterStream};
use cloud_backup_protocol::Id;
use lastdb_model::canonical::CanonicalValue;
use lastdb_model::portable_mutation::{MutationEntry, MutationOperation, StreamProgress};
use lastdb_model::record_value::FileMarker;
use schema_types::PortableSchemaDefinition;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod retention;

impl MutationManager {
    /// Bind an enrolled restored writer, or the authenticated first publisher.
    /// Each alias must already hold its schema in the shared scope.
    pub async fn bind_portable_journal(
        &self,
        stream: WriterStream,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
        device: &Id,
        initialize_publisher: bool,
    ) -> Result<(), SchemaError> {
        if definitions.is_empty() || definitions.len() > 64 {
            return Err(invalid("invalid shared journal schema count"));
        }
        transaction::request_scope(self.db_ops.native_runtime(), async {
            self.stage_recorded_definitions(definitions).await?;
            let bound = self.journal_schemas(definitions).await?;
            self.db_ops
                .metadata()
                .bind_portable_journal(stream, bound, device, initialize_publisher)
                .await
        })
        .await
    }

    /// Admit more shared schemas to a bound writer. A schema this home lacks
    /// installs under its canonical name. A later definition of an admitted
    /// name adds its fields to that name. The writer binds each name's current
    /// definition, and its stream defines that before the name's next record.
    pub async fn admit_portable_schemas(
        &self,
        definitions: &[PortableSchemaDefinition],
    ) -> Result<(), SchemaError> {
        if definitions.is_empty() || definitions.len() > 64 {
            return Err(invalid("invalid admitted shared schema count"));
        }
        self.install_portable_definitions(definitions).await?;
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let metadata = self.db_ops.metadata();
            let sets = metadata.portable_definition_sets().await?;
            let mut admitted = BTreeMap::new();
            for definition in definitions {
                let (alias, set) = sets
                    .iter()
                    .find(|(_, set)| set.accepted.contains_key(definition.id()))
                    .ok_or_else(|| invalid("an admitted shared schema is not installed"))?;
                admitted.insert(alias.clone(), set.current.clone());
            }
            let bound = self.journal_schemas(&admitted).await?;
            metadata.admit_portable_schemas(bound).await
        })
        .await
    }

    /// Bind each of the writer's aliases to its name's current definition.
    /// A record then names the definition that declares its fields, and the
    /// stream defines that definition before the record.
    pub(super) async fn follow_portable_definitions(&self) -> Result<(), SchemaError> {
        let stale = self
            .db_ops
            .read_scope()
            .await?
            .run_result(self.stale_writer_bindings())
            .await?;
        if stale.is_empty() {
            return Ok(());
        }
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let stale = self.stale_writer_bindings().await?;
            if stale.is_empty() {
                return Ok(());
            }
            let bound = self.journal_schemas(&stale).await?;
            self.db_ops.metadata().rebind_portable_schemas(bound).await
        })
        .await
    }

    /// The current definition of each bound alias whose binding is older.
    async fn stale_writer_bindings(
        &self,
    ) -> Result<BTreeMap<String, PortableSchemaDefinition>, SchemaError> {
        let metadata = self.db_ops.metadata();
        let Some(bound) = metadata.portable_writer_bindings().await? else {
            return Ok(BTreeMap::new());
        };
        let mut current = metadata.portable_definitions().await?;
        current.retain(|alias, definition| {
            bound
                .get(alias)
                .is_some_and(|binding| binding.id != definition.id().as_str())
        });
        Ok(current)
    }

    /// Fingerprint each installed alias after its definition binds to it.
    async fn journal_schemas(
        &self,
        definitions: &BTreeMap<String, PortableSchemaDefinition>,
    ) -> Result<BTreeMap<String, JournalSchema>, SchemaError> {
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
        if bound.len() != definitions.len() || !bound.keys().eq(definitions.keys()) {
            return Err(invalid(
                "shared schema aliases resolve to a duplicate local schema",
            ));
        }
        Ok(bound)
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
        transaction::request_scope(self.db_ops.native_runtime(), async {
            let retired = self
                .db_ops
                .metadata()
                .stage_outgoing_receipt(batch, receipt)
                .await?;
            self.release_uploaded_files(&retired).await
        })
        .await
    }

    /// Advance the one-time cleanup of homes written before retention tracking.
    pub async fn reclaim_legacy_shared_retention(&self) -> Result<(), SchemaError> {
        let records = self.db_ops.records();
        if records.file_retention_complete().await? {
            return Ok(());
        }
        // Each step releases the native write gate and saves its continuation.
        // A large old home can finish on subsequent shared sync invocations.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        for _ in 0..64 {
            let more = transaction::background_scope(
                self.db_ops.native_runtime(),
                records.stage_file_retention_step(),
            )
            .await?;
            if !more || std::time::Instant::now() >= deadline {
                break;
            }
        }
        Ok(())
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
