//! Authenticated native restore source. No route, worker or activation is registered here.
use crate::native_cloud_restore::{ObjectStream, RestoreError, RestoreSource, Result};
use cloud_backup_protocol::access::{ReadGrant, ReadGrantRequest, ReadGrantResponse};
use cloud_backup_protocol::publication::{AcceptedBackup, CommitResponse, CommitStatusRequest};
use cloud_backup_protocol::{BackupJob, Version};
use fold_db::sync::auth::native_backup::NativeBackupClient;

#[derive(Clone)]
pub struct NativeCloudRestoreSource {
    client: NativeBackupClient,
}
impl NativeCloudRestoreSource {
    pub fn new(client: NativeBackupClient) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl RestoreSource for NativeCloudRestoreSource {
    async fn accepted(&self, job: &BackupJob) -> Result<AcceptedBackup> {
        let request = CommitStatusRequest {
            version: Version,
            job: job.clone(),
        };
        match self
            .client
            .commit_status(&request)
            .await
            .map_err(|_| RestoreError::Source("accepted native commit"))?
        {
            CommitResponse::Accepted { backup } => Ok(*backup),
            _ => Err(RestoreError::Source(
                "selected native commit is not accepted",
            )),
        }
    }
    async fn grants(&self, request: &ReadGrantRequest) -> Result<ReadGrantResponse> {
        self.client
            .read_grants(request)
            .await
            .map_err(|_| RestoreError::Source("native object grants"))
    }
    async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream> {
        self.client
            .open(grant)
            .await
            .map_err(|_| RestoreError::Source("native object transport"))
    }
}
