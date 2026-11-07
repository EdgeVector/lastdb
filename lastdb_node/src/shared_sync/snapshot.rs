use super::config::{self, Config};
use crate::byte_cloud_backup::{BackupError, Result};
use crate::byte_cloud_restore::local;
use crate::shared_snapshot::SignedSnapshotTransport;
use cloud_backup_protocol::Id;
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::sync::Arc;
use std::time::Duration;

pub(super) async fn publish(host: &crate::Host, config: &mut Config) -> Result<()> {
    let auth = cloud_backup_client::AuthClient::new(
        config.credentials.api_url.clone(),
        config.credentials.api_key.as_str().to_owned(),
    );
    let signer = Arc::new(config.signer()?);
    if config.last_snapshot_id.is_none() && config.pending_snapshot.is_none() {
        let transport = SignedSnapshotTransport::from_auth(
            auth.clone(),
            config.account.clone(),
            config.device.clone(),
            Arc::clone(&signer),
        )
        .map_err(|_| BackupError::Source)?;
        if let Some(receipt) = transport
            .current_snapshot(&config.stream.database_id)
            .await
            .map_err(|_| BackupError::Source)?
        {
            // An existing manual publication already protects the starting cut.
            schedule_next(config, receipt.snapshot_id);
            return config::save(&host.home, config);
        }
    }
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
    let result = crate::shared_snapshot::sequence::run_shared_snapshot_locked(
        host,
        &root.join(pending.as_str()),
        Duration::from_secs(90),
        auth,
        config.device.clone(),
        signer,
    )
    .await;
    let result = match result {
        Ok(result) => result,
        Err(crate::byte_cloud_restore::RestoreError::Invalid(
            crate::shared_snapshot::native::ABANDONED,
        )) => {
            // The publication is terminal and its hold has been released.
            // Retire this workspace before the next automatic capture.
            config.completed_snapshot = config.pending_snapshot.take();
            config::save(&host.home, config)?;
            return Err(BackupError::Waiting);
        }
        Err(_) => return Err(BackupError::Source),
    };
    config.completed_snapshot = config.pending_snapshot.take();
    schedule_next(config, result.snapshot_id);
    config::save(&host.home, config)
}

fn schedule_next(config: &mut Config, snapshot: Id) {
    let now = fold_db::clock::unix_secs();
    config.last_snapshot_id = Some(snapshot);
    config.last_snapshot_at = Some(now);
    config.next_snapshot_at = now.saturating_add(config.snapshot_interval_seconds);
}
