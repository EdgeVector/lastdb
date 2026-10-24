use super::{paths, BackupError, Options, Result};
use crate::byte_cloud_backup::{model::Saved, retry, source::Source};
use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use fold_db::sync::auth::byte_backup::ByteBackupClient;
use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use std::sync::atomic::AtomicBool;

pub(super) fn endpoint(env: Environment) -> Result<String> {
    let mut url = reqwest::Url::parse(exemem_api_url_for(env)).map_err(|_| BackupError::Invalid)?;
    let base = url.path().trim_end_matches('/');
    let api = if base.ends_with("/api") {
        base.to_owned()
    } else {
        format!("{base}/api")
    };
    url.set_path(&format!("{api}/cloud/backups/v2/"));
    Ok(url.to_string())
}

pub(super) fn client(options: &Options, source: &Source) -> Result<ByteBackupClient> {
    if !options.credentials_home.is_absolute() {
        return Err(BackupError::Identity);
    }
    paths::private_directory(&options.credentials_home)?;
    let home = options.credentials_home.canonicalize()?;
    let identity = super::identity::Identity::open(&home)?;
    if identity.account() != &source.account || identity.recovery() != &source.recovery {
        return Err(BackupError::Identity);
    }
    // A standalone uploader never activates legacy sync or selects an active key.
    if home.join("cloud_sync.json").try_exists()? {
        return Err(BackupError::Identity);
    }
    let client = crate::byte_cloud_backup::credentials::client_for_recovery(&home)
        .map_err(|_| BackupError::Identity)?;
    if client.canonical_endpoint() != endpoint(options.environment)? {
        return Err(BackupError::Identity);
    }
    identity.check()?;
    Ok(client)
}

pub(super) async fn revalidate(
    source: &Source,
    saved: &Saved,
    client: &ByteBackupClient,
    pause: &AtomicBool,
) -> Result<()> {
    if let Some(begin) = &saved.begin {
        let request = ContextReadRequest {
            version: Version::V2,
            account_id: source.account.clone(),
        };
        let context = retry::control("standalone_context", pause, || async {
            Ok(client.context(&request).await?)
        })
        .await?;
        if !context.new_work_allowed || context.scope != begin.job.stream.scope {
            return Err(BackupError::Authority);
        }
    }
    source.validate(saved)
}
