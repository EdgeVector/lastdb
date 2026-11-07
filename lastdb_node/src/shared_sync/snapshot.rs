use super::config::{self, Config};
use crate::byte_cloud_backup::{BackupError, Result};
use crate::byte_cloud_restore::local;
use cloud_backup_protocol::Id;
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::sync::Arc;
use std::time::Duration;

pub(super) async fn publish(host: &crate::Host, config: &mut Config) -> Result<()> {
    let root = host.home.join("shared-cloud-snapshots");
    match fs::DirBuilder::new().mode(0o700).create(&root) {
        Ok(()) => File::open(&host.home)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    local::directory(&root).map_err(|_| BackupError::Identity)?;
    if let Some(completed) = &config.completed_snapshot {
        let path = root.join(completed.as_str());
        if path.try_exists()? {
            local::directory(&path).map_err(|_| BackupError::Identity)?;
            fs::remove_dir_all(&path)?;
            File::open(&root)?.sync_all()?;
        }
        config.completed_snapshot = None;
        config::save(&host.home, config)?;
    }
    if config.pending_snapshot.is_none() {
        config.pending_snapshot =
            Some(Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(|_| BackupError::Invalid)?);
        config::save(&host.home, config)?;
    }
    let pending = config
        .pending_snapshot
        .as_ref()
        .ok_or(BackupError::Missing)?;
    let auth = cloud_backup_client::AuthClient::new(
        config.credentials.api_url.clone(),
        config.credentials.api_key.as_str().to_owned(),
    );
    let result = crate::shared_snapshot::sequence::run_shared_snapshot_locked(
        host,
        &root.join(pending.as_str()),
        Duration::from_secs(90),
        auth,
        config.device.clone(),
        Arc::new(config.signer()?),
    )
    .await
    .map_err(|_| BackupError::Source)?;
    config.last_snapshot_id = Some(result.snapshot_id);
    config.completed_snapshot = config.pending_snapshot.take();
    config.last_snapshot_at = Some(fold_db::clock::unix_secs());
    config.next_snapshot_at =
        fold_db::clock::unix_secs().saturating_add(config.snapshot_interval_seconds);
    config::save(&host.home, config)
}
