//! The cloud source of record files that are not on this device (design
//! `design-lastdb-blob-files-on-demand-20261107`).
//!
//! `fetch-blob` asks this source for the sealed row of a record file version
//! when its row is not local, for example after a restore. The source asks the
//! service for a GET grant at the record's blob path
//! (`POST cloud/backups/v2/blobs/read`) and downloads exactly the granted
//! length. Core checks the bytes against the record's marker before it uses
//! or installs them.
use async_trait::async_trait;
use cloud_backup_client::byte_backup::ByteBackupClient;
use cloud_backup_protocol::v2::blobs::{BlobPath, BlobReadItem, BlobReadRequest};
use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::{Destination, Version};
use cloud_backup_protocol::{AccountId, CloudScope};
use fold_db::fold_db_core::{BlobFetch, RecordBlobSource, SIGN_IN_TO_FETCH_FILE};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

/// Register the source a personal home uses for a record file that is not
/// local. Saved credentials install the cloud source. A personal home with
/// no saved credentials installs a source that tells the owner to sign in.
/// A dedicated shared home, and a home whose credential file cannot be read,
/// keep no source.
pub fn register_blob_source(host: &crate::host::Host) {
    let runtime = host.db.db_ops().native_runtime();
    if runtime.is_dedicated_shared_home() {
        return;
    }
    let source: Arc<dyn RecordBlobSource> = match blob_source(host) {
        Ok(source) => Arc::new(source),
        Err(reason) => {
            if credentials_absent(&host.home) {
                Arc::new(SignInRequired)
            } else {
                tracing::debug!(
                    target: "lastdbd::blob_source",
                    %reason,
                    "record files download on request is off"
                );
                return;
            }
        }
    };
    host.db.set_record_blob_source(Some(source));
}

fn credentials_absent(home: &std::path::Path) -> bool {
    matches!(crate::cloud::load_saved_credentials(home), Ok(None))
}

/// A personal home that has not signed in. The fetch does not call the cloud.
struct SignInRequired;

#[async_trait]
impl RecordBlobSource for SignInRequired {
    async fn fetch(&self, _path: &BlobPath, _sealed_bytes: u64) -> BlobFetch {
        BlobFetch::Unavailable(SIGN_IN_TO_FETCH_FILE.to_owned())
    }
}

fn blob_source(host: &crate::host::Host) -> Result<CloudBlobSource, String> {
    let client = super::credentials::client_for_recovery(&host.home)?;
    let account = AccountId::try_from(host.user_hash.clone())
        .map_err(|_| "the node account id is not a cloud account id".to_owned())?;
    Ok(CloudBlobSource {
        client,
        account,
        scope: tokio::sync::Mutex::new(None),
    })
}

struct CloudBlobSource {
    client: ByteBackupClient,
    account: AccountId,
    /// The account scope from the last context read; cleared after a failed
    /// read so a new account generation is picked up.
    scope: tokio::sync::Mutex<Option<CloudScope>>,
}

#[async_trait]
impl RecordBlobSource for CloudBlobSource {
    async fn fetch(&self, path: &BlobPath, sealed_bytes: u64) -> BlobFetch {
        match self.read(path, sealed_bytes).await {
            Ok(fetch) => fetch,
            Err(reason) => BlobFetch::Unavailable(reason),
        }
    }
}

impl CloudBlobSource {
    async fn scope(&self) -> Result<CloudScope, String> {
        let mut cached = self.scope.lock().await;
        if let Some(scope) = cached.as_ref() {
            return Ok(scope.clone());
        }
        let context = self
            .client
            .context(&ContextReadRequest {
                version: Version::V2,
                account_id: self.account.clone(),
                required_destinations: vec![Destination::B2],
            })
            .await
            .map_err(|error| format!("cloud account context: {error}"))?;
        *cached = Some(context.scope.clone());
        Ok(context.scope)
    }

    async fn read(&self, path: &BlobPath, sealed_bytes: u64) -> Result<BlobFetch, String> {
        let request = BlobReadRequest {
            version: Version::V2,
            scope: self.scope().await?,
            paths: vec![path.clone()],
        };
        let response = match self.client.blob_read(&request).await {
            Ok(response) => response,
            Err(error) => {
                *self.scope.lock().await = None;
                return Err(format!("cloud blob read: {error}"));
            }
        };
        let item = response
            .items
            .into_iter()
            .next()
            .ok_or_else(|| "cloud blob read returned no result".to_owned())?;
        match &item {
            BlobReadItem::Absent { .. } => return Ok(BlobFetch::Absent),
            BlobReadItem::Grant { bytes, .. } if bytes.get() != sealed_bytes => {
                return Err(format!(
                    "the cloud object has {} bytes; the record's file needs {sealed_bytes}",
                    bytes.get()
                ));
            }
            BlobReadItem::Grant { .. } => {}
        }
        let mut stream = self
            .client
            .open_blob(&item)
            .await
            .map_err(|error| format!("cloud blob download: {error}"))?;
        let mut row = Vec::with_capacity(usize::try_from(sealed_bytes).unwrap_or(0));
        stream
            .read_to_end(&mut row)
            .await
            .map_err(|error| format!("cloud blob download: {error}"))?;
        Ok(BlobFetch::Bytes(row))
    }
}
