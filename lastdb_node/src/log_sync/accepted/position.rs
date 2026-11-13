//! The device allocates upload numbers from its own durable confirmation.

use std::sync::Arc;

use fold_db::storage::disk_backup::accepted::{UploadIdentity, UploadPosition};
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::{LogError, MutationLog};

use super::CloudLog;

pub(super) struct LocalUpload {
    identity: Arc<UploadIdentity>,
    pub(super) position: UploadPosition,
}

impl LocalUpload {
    pub(super) async fn open(
        runtime: &Arc<PackRuntime>,
        log: &CloudLog,
        after: u64,
    ) -> Result<Option<Self>, &'static str> {
        let identity = Arc::new(UploadIdentity {
            scope: log.scope.clone(),
            environment: log.environment().to_owned(),
            database_id: log.database_id().to_owned(),
            device_id: log.device().as_str().to_owned(),
            writer_key: log.principal().as_str().to_owned(),
        });
        let reader = Arc::clone(runtime);
        let read_identity = Arc::clone(&identity);
        let saved =
            tokio::task::spawn_blocking(move || reader.accepted_upload_position(&read_identity))
                .await
                .map_err(|_| "local upload reader stopped")?
                .map_err(|_| {
                    "local upload position is unavailable for this device and cloud scope"
                })?;
        let Some(position) = saved else {
            let upload = Self {
                identity,
                position: UploadPosition {
                    writer_seq: contents_of_writer(log).await?,
                    spool_through: after,
                },
            };
            // Save the migration baseline before assigning a number or sealing a payload.
            upload.confirm(runtime, upload.position).await?;
            return Ok(Some(upload));
        };
        if position.spool_through != after {
            // A previously confirmed page was retired while opening the local position.
            return Ok(None);
        }
        Ok(Some(Self { identity, position }))
    }

    pub(super) async fn confirm(
        &self,
        runtime: &Arc<PackRuntime>,
        saved: UploadPosition,
    ) -> Result<(), &'static str> {
        let runtime = Arc::clone(runtime);
        let identity = Arc::clone(&self.identity);
        let after = self.position.spool_through;
        tokio::task::spawn_blocking(move || {
            runtime.confirm_accepted_upload(&identity, after, saved)
        })
        .await
        .map_err(|_| "local upload writer stopped")?
        .map_err(|_| "upload confirmation could not be recorded locally")
    }
}

/// Existing homes have an unbound spool acknowledgement. Discover the
/// highest stored writer entry once and leave all existing seals unchanged.
async fn contents_of_writer(log: &CloudLog) -> Result<u64, &'static str> {
    let log = log.clone();
    let result = tokio::task::spawn_blocking(move || {
        MutationLog::writer_tip(&log, log.principal(), &log.reader())
    })
    .await
    .map_err(|_| "upload history recovery stopped")?;
    result.map_err(|error| match error {
        LogError::Conflict => "cloud entries disagree with the saved upload identity",
        _ => "could not read stored writes to initialize the local upload position",
    })
}
