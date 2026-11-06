use super::super::{
    recovery::{self, PreparedSharedRestore, SnapshotSchema},
    rejoin_disk, require_complete, SharedRestore,
};
use super::{NativeWriter, RecoveryKeys, RestoreError, Result, SharedRecoverySource};
use cloud_backup_protocol::shared::{LogHead, WriterStream};
use cloud_backup_protocol::Id;
use fold_db::storage::disk_pack::run_restore;
use fold_db::FoldDB;
use schema_service_client::SchemaServiceClient;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(in crate::shared_restore) struct RejoinSource<'a> {
    pub device: &'a Id,
    pub stream: &'a WriterStream,
    pub confirmed_head: Option<&'a LogHead>,
    pub source_data: &'a Path,
    pub source: &'a FoldDB,
}

/// A verified replacement that has not yet taken the live data path.
pub struct PreparedRejoin {
    _owner: File,
    runtime: std::sync::Weak<fold_db::storage::disk_pack::PackRuntime>,
    source_data: PathBuf,
    staged_data: PathBuf,
}

impl PreparedRejoin {
    /// Retain the original storage owner until both sides of the swap are durable.
    pub async fn activate(self, source: Arc<FoldDB>) -> Result<PathBuf> {
        let runtime = source.db_ops().native_runtime();
        if !self.runtime.ptr_eq(&Arc::downgrade(&runtime)) {
            return Err(RestoreError::Invalid("returning writer source changed"));
        }
        source
            .shutdown()
            .await
            .map_err(RestoreError::native_proof)?;
        let _source_owner = runtime
            .retain_rejoin_source_owner()
            .map_err(RestoreError::native_proof)?;
        drop(source);
        let runtime = Arc::into_inner(runtime).ok_or(RestoreError::Invalid(
            "returning writer storage is still open",
        ))?;
        // Retired files still carry paths beneath the original data directory.
        // Complete their cleanup while that directory is still in place.
        drop(runtime);
        rejoin_disk::activate(&self.source_data, &self.staged_data, &_source_owner)
    }
}

pub(in crate::shared_restore) async fn prepare_rejoin(
    remote: &impl SharedRecoverySource,
    keys: &RecoveryKeys,
    schemas: &SchemaServiceClient,
    original: RejoinSource<'_>,
) -> Result<PreparedRejoin> {
    crate::offline_home::refuse_primary(original.source_data)
        .map_err(|_| RestoreError::Invalid("rejoin source home"))?;
    let source_data = original.source_data.canonicalize()?;
    let unused_destination =
        source_data.with_file_name(format!(".rejoin-{}", uuid::Uuid::new_v4()));
    let PreparedSharedRestore {
        mut session,
        schemas: checkpoint_schemas,
        tail,
    } = recovery::prepare(
        remote,
        &original.stream.database_id,
        keys.content_key(),
        &unused_destination,
    )
    .await?;
    let staged_data = session.stage().join("data");
    let runtime = original.source.db_ops().native_runtime();
    let result = async {
        require_original_bindings(original.source, &checkpoint_schemas).await?;
        rejoin_disk::stage_private_run(&runtime, &staged_data).await?;
        let writer = NativeWriter::open(
            session.stage(),
            original.device,
            &session.cut,
            checkpoint_schemas,
            keys,
            schemas,
        )
        .await?;
        writer
            .db
            .mutation_manager()
            .reset_portable_rejoin_file_references()
            .await
            .map_err(|_| RestoreError::Invalid("reset returning file references"))?;
        Ok::<_, RestoreError>(writer)
    }
    .await;
    let mut writer = match result {
        Ok(writer) => writer,
        Err(error) => {
            session.discard_unsealed()?;
            return Err(error);
        }
    };
    let ready = async {
        tail.replay(keys.content_key(), &mut session, &mut writer)
            .await?;
        require_complete(&session.cut)?;
        writer
            .db
            .mutation_manager()
            .finish_portable_rejoin(
                &session.cut.progress,
                session.cut.delete_reclaimed_through_ms,
                original.stream,
                original.device,
                original.confirmed_head,
            )
            .await
            .map_err(|_| RestoreError::Invalid("seal returning writer progress"))
    }
    .await;
    if let Err(error) = ready {
        let _ = writer.db.shutdown().await;
        drop(writer);
        session.discard_unsealed()?;
        return Err(error);
    }
    if let Err(error) = writer.close_verified().await {
        session.discard_unsealed()?;
        return Err(error);
    }
    let SharedRestore { _owner, .. } = session;
    Ok(PreparedRejoin {
        _owner,
        runtime: Arc::downgrade(&runtime),
        source_data,
        staged_data,
    })
}

async fn require_original_bindings(source: &FoldDB, schemas: &[SnapshotSchema]) -> Result<()> {
    let bindings = source
        .mutation_manager()
        .portable_writer_schemas()
        .await
        .map_err(|_| RestoreError::Invalid("read returning writer schema bindings"))?;
    if schemas.len() != bindings.len()
        || schemas.iter().any(|schema| {
            bindings.get(schema.definition.id().as_str()) != Some(&schema.local_schema)
        })
    {
        return Err(RestoreError::Invalid(
            "returning writer schema bindings differ",
        ));
    }
    Ok(())
}

impl NativeWriter<'_> {
    pub(super) async fn close_verified(self) -> Result<()> {
        self.db
            .shutdown()
            .await
            .map_err(RestoreError::native_proof)?;
        drop(self.db);
        run_restore::validate_replayed_shared_node(
            &self.stage.join("data"),
            self.keys.content_key(),
        )
        .map_err(RestoreError::native_proof)
    }
}
