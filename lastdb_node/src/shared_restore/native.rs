//! Native writer adapter for an authenticated staged shared restore.

use super::recovery::{
    HeldFileVersions, PreparedSharedRestore, SharedRecoverySource, SnapshotSchema,
};
use super::RestoreCut;
use crate::byte_cloud_restore::{RecoveryKeys, RestoreError, Result};
use cloud_backup_protocol::Id;
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::{
    factory::{create_fold_db_with_options, FoldDbOptions},
    mutation_manager::portable_replay::ReplayCandidate,
};
use fold_db::portable_mutation::{MutationEntry, MutationOperation, Position, StreamId};
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use fold_db::FoldDB;
use schema_service_client::SchemaServiceClient;
use schema_types::PortableSchemaId;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod rejoin;
mod schemas;
pub use rejoin::PreparedRejoin;
pub(super) use rejoin::{prepare_rejoin, RejoinSource};
pub(crate) use schemas::{resolve_schemas, schema_id, SchemaTarget};

const FILE_INPUT_LIMIT: u64 = lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
const BATCH_FILE_BYTES: u64 = FILE_INPUT_LIMIT / 2;
const BATCH_RECORDS: usize = 128;

pub async fn restore_from_source<S: SharedRecoverySource>(
    source: &S,
    database_id: &Id,
    device: &Id,
    destination: &Path,
    keys: &RecoveryKeys,
    service: &SchemaServiceClient,
) -> Result<PathBuf> {
    let prepared =
        super::recovery::prepare(source, database_id, keys.content_key(), destination).await?;
    replay_and_activate(prepared, device, keys, service).await
}

async fn replay_and_activate<S: SharedRecoverySource>(
    prepared: PreparedSharedRestore<'_, S>,
    device: &Id,
    keys: &RecoveryKeys,
    service: &SchemaServiceClient,
) -> Result<PathBuf> {
    let PreparedSharedRestore {
        mut session,
        schemas,
        tail,
    } = prepared;
    let mut writer = match NativeWriter::open(
        session.stage(),
        device,
        &session.cut,
        schemas,
        keys,
        service,
    )
    .await
    {
        Ok(writer) => writer,
        Err(error) => {
            session.discard_unsealed()?;
            return Err(error.shared_stage("writer-open"));
        }
    };
    if let Err(error) = tail
        .replay(keys.content_key(), &mut session, &mut writer)
        .await
    {
        let _ = writer.db.shutdown().await;
        drop(writer);
        session.discard_unsealed()?;
        return Err(error.shared_stage("tail-read"));
    }
    let path = session.workspace().to_path_buf();
    session
        .finish(writer, tail.database_id())
        .await
        .map_err(|cause| RestoreError::SharedWorkspace {
            path,
            cause: Box::new(cause.shared_stage("replay-activate")),
        })
}

pub(super) struct NativeWriter<'a> {
    pub(super) stage: PathBuf,
    device: Id,
    db: Arc<FoldDB>,
    schemas: BTreeMap<PortableSchemaId, SnapshotSchema>,
    keys: &'a RecoveryKeys,
    service: &'a SchemaServiceClient,
}

impl<'a> NativeWriter<'a> {
    async fn open(
        stage: &Path,
        device: &Id,
        checkpoint: &RestoreCut,
        schemas: Vec<SnapshotSchema>,
        keys: &'a RecoveryKeys,
        service: &'a SchemaServiceClient,
    ) -> Result<Self> {
        let e2e = E2eKeys::from_ed25519_seed(keys.account_seed())
            .map_err(|_| RestoreError::Invalid("shared account key"))?;
        let signer = Ed25519KeyPair::from_secret_key(keys.account_seed())
            .map_err(|_| RestoreError::Invalid("shared account signer"))?;
        let db = create_fold_db_with_options(
            &DatabaseConfig::local(stage.join("data")),
            &e2e,
            Arc::new(signer),
            FoldDbOptions {
                offline_maintenance: true,
                search_outbox_inbox: None,
            },
        )
        .await
        .map_err(|_| RestoreError::Invalid("staged native writer open"))?;
        db.db_ops()
            .native_runtime()
            .require_shared_schema_set(schemas.iter().map(|schema| schema.local_schema.as_str()))
            .map_err(|_| {
                RestoreError::Invalid("snapshot disk scope differs from its schema bindings")
            })?;
        db.mutation_manager()
            .validate_portable_restore_checkpoint(
                &checkpoint.progress,
                checkpoint.delete_reclaimed_through_ms,
            )
            .await
            .map_err(|_| RestoreError::Invalid("staged replay checkpoint"))?;

        let mut admitted = BTreeMap::new();
        let manager = db.schema_manager();
        for schema in schemas {
            let local = manager
                .get_schema_metadata(&schema.local_schema)
                .map_err(|_| RestoreError::Invalid("checkpoint schema read"))?
                .ok_or(RestoreError::Invalid("missing checkpoint schema"))?;
            schema
                .definition
                .bind_local_alias(schema.definition.id(), &local.to_wire())
                .map_err(|_| RestoreError::Invalid("checkpoint schema binding"))?;
            admitted.insert(schema.definition.id().clone(), schema);
        }

        Ok(Self {
            stage: stage.to_path_buf(),
            device: device.clone(),
            db,
            schemas: admitted,
            keys,
            service,
        })
    }

    async fn apply(
        &self,
        candidates: &[ReplayCandidate<'_>],
        needed: Vec<usize>,
        dependencies: &HeldFileVersions<'_, impl SharedRecoverySource>,
    ) -> Result<()> {
        let mut group = Vec::new();
        let mut files = BTreeMap::new();
        let mut bytes = 0;
        for index in needed {
            let candidate = candidates
                .get(index)
                .ok_or(RestoreError::Invalid("native replay candidate index"))?;
            let version = file_version(candidate.entry);
            let length = version
                .map(|version| dependencies.bytes(version))
                .transpose()?
                .unwrap_or(0);
            if length > FILE_INPUT_LIMIT {
                return Err(RestoreError::Limit("native replay file bytes"));
            }
            if !group.is_empty()
                && (group.len() == BATCH_RECORDS || bytes + length > BATCH_FILE_BYTES)
            {
                self.commit_group(&group, std::mem::take(&mut files))
                    .await?;
                group.clear();
                bytes = 0;
            }
            if let Some(version) = version {
                let sealed = dependencies.fetch(version, FILE_INPUT_LIMIT).await?;
                bytes += length;
                files.insert(version.clone(), sealed);
            }
            group.push(ReplayCandidate {
                entry: candidate.entry,
                definition: candidate.definition,
                local_schema: candidate.local_schema,
            });
        }
        if !group.is_empty() {
            self.commit_group(&group, files).await?;
        }
        Ok(())
    }

    async fn commit_group(
        &self,
        group: &[ReplayCandidate<'_>],
        files: BTreeMap<Position, Vec<u8>>,
    ) -> Result<()> {
        self.db
            .mutation_manager()
            .apply_portable_candidates(group, files)
            .await
            .map_err(|_| RestoreError::Invalid("commit native replay candidates"))
    }
    pub(super) async fn apply_candidates(
        &mut self,
        entries: &[&MutationEntry],
        files: &HeldFileVersions<'_, impl SharedRecoverySource>,
    ) -> Result<()> {
        resolve_schemas(
            &self.db,
            self.service,
            &mut self.schemas,
            entries,
            SchemaTarget::StagedRestore,
        )
        .await?;
        let candidates = entries
            .iter()
            .map(|entry| {
                let binding = self
                    .schemas
                    .get(&schema_id(&entry.schema)?)
                    .ok_or(RestoreError::Invalid("portable schema binding"))?;
                Ok(ReplayCandidate {
                    entry,
                    definition: &binding.definition,
                    local_schema: &binding.local_schema,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let needed = self
            .db
            .mutation_manager()
            .portable_replay_needed(&candidates)
            .await
            .map_err(|_| RestoreError::Invalid("select native replay candidates"))?;
        self.apply(&candidates, needed, files).await
    }

    pub(super) async fn finish(
        self,
        cut: &RestoreCut,
        local_stream: &StreamId,
        database_id: &Id,
    ) -> Result<()> {
        let mut progress = cut.progress.clone();
        // Only streams with applied mutations belong to the sealed progress.
        // An enrolled idle writer can still bind this home as its first home.
        progress.retain(|_, cursor| cursor.applied_through != 0);
        self.db
            .mutation_manager()
            .finish_portable_restore(
                &progress,
                cut.delete_reclaimed_through_ms,
                local_stream,
                database_id,
                &self.device,
            )
            .await
            .map_err(|_| RestoreError::Invalid("seal portable restore progress"))?;
        let stage = self.stage.clone();
        let seed = self.keys.account_seed();
        self.close_verified().await?;
        lastdb_identity::write_seed(&stage, seed)
            .map_err(|_| RestoreError::Invalid("restored account identity"))?;
        Ok(())
    }
}

fn file_version(entry: &MutationEntry) -> Option<&Position> {
    match &entry.operation {
        MutationOperation::ReplaceRecord { data, .. } => {
            data.file.as_ref().map(|marker| &marker.version)
        }
        MutationOperation::DeleteRecord { .. } => None,
    }
}
