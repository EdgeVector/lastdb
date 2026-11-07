use super::{config, snapshot};
use crate::byte_cloud_backup::{BackupError, Result};
use crate::shared_peer::{PeerIdentity, PeerOutcome, PeerWorker};
use crate::shared_upload::{upload_next, ServiceObjects, UploadIdentity, UploadOutcome};
use cloud_backup_client::{
    shared_log::SharedLogClient, shared_objects::SharedObjectClient, shared_peer::SharedPeerClient,
    AuthClient,
};
use folddb_profile::endpoints::{schema_service_url_for, Environment};
use schema_service_client::SchemaServiceClient;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use zeroize::Zeroizing;

const PASS_SECONDS: u64 = 30;
const RANGES_PER_PASS: usize = 32;

pub(super) async fn run(host: Arc<crate::Host>, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Acquire) && !host.byte_backup.snapshot().shutting_down {
        if tick(&host).await.is_err() {
            host.shared_sync.update(|state| {
                state.running = false;
                state.blocked_reason = Some("shared_sync_retry_pending");
                state.next_sync_at = fold_db::clock::unix_secs().saturating_add(5);
            });
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn tick(host: &crate::Host) -> Result<()> {
    let Ok(_activity) = host.shared_sync.gate.try_lock() else {
        return Ok(());
    };
    let Some(mut config) = config::load(&host.home)? else {
        return Ok(());
    };
    let now = fold_db::clock::unix_secs();
    let status = super::status(host)?.ok_or(BackupError::Missing)?;
    if !config.enabled || now < status.progress.next_sync_at {
        return Ok(());
    }
    host.shared_sync.update(|state| state.running = true);
    sync(host, &config).await?;
    if config.publisher && (config.pending_snapshot.is_some() || now >= config.next_snapshot_at) {
        snapshot::publish(host, &mut config).await?;
    }
    host.shared_sync.update(|state| {
        state.running = false;
        state.last_sync_at = Some(fold_db::clock::unix_secs());
        state.next_sync_at = fold_db::clock::unix_secs().saturating_add(config.interval_seconds);
        state.blocked_reason = None;
    });
    Ok(())
}

async fn sync(host: &crate::Host, config: &config::Config) -> Result<()> {
    host.db
        .db_ops()
        .native_runtime()
        .require_direct_shared_home()?;
    let native = host.db.mutation_manager();
    let (stream, progress) = native
        .portable_writer_progress()
        .await
        .map_err(|_| BackupError::Identity)?;
    if stream != config.stream || config.account.as_str() != host.user_hash {
        return Err(BackupError::Identity);
    }
    host.shared_sync
        .update(|state| state.uploaded_through = progress.uploaded_through.unwrap_or(0));
    let signer = config.signer()?;
    let key = Zeroizing::new(host.keypair.secret_key_bytes());
    let e2e =
        fold_db::crypto::E2eKeys::from_ed25519_seed(&key).map_err(|_| BackupError::Identity)?;
    let content_key = Zeroizing::new(e2e.encryption_key());
    let auth = AuthClient::new(
        config.credentials.api_url.clone(),
        config.credentials.api_key.as_str().to_owned(),
    );
    let logs = SharedLogClient::from_auth(auth.clone()).map_err(|_| BackupError::Source)?;
    let objects =
        SharedObjectClient::from_auth(auth.clone(), config.account.clone(), config.device.clone())
            .map_err(|_| BackupError::Source)?;
    let uploader = ServiceObjects {
        client: &objects,
        device_signer: &signer,
        deadline_ms: fold_db::clock::unix_millis().saturating_add(PASS_SECONDS * 1_000),
    };
    let identity = UploadIdentity {
        account: &config.account,
        device: &config.device,
        device_signer: &signer,
        content_key: &content_key,
    };
    // Each direction gets its own time budget. A failed upload must not
    // prevent downloading other writers' accepted mutations.
    let (upload, download) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(PASS_SECONDS),
            upload(host, &identity, &logs, &uploader)
        ),
        tokio::time::timeout(
            Duration::from_secs(PASS_SECONDS),
            download(host, config, auth, &signer, &content_key)
        ),
    );
    upload.map_err(|_| BackupError::Waiting)??;
    download.map_err(|_| BackupError::Waiting)?
}

async fn upload(
    host: &crate::Host,
    identity: &UploadIdentity<'_>,
    logs: &SharedLogClient,
    uploader: &ServiceObjects<'_>,
) -> Result<()> {
    for _ in 0..RANGES_PER_PASS {
        match upload_next(
            &host.data_dir,
            host.db.mutation_manager(),
            identity,
            logs,
            uploader,
        )
        .await
        .map_err(|_| BackupError::Source)?
        {
            UploadOutcome::Idle => return Ok(()),
            UploadOutcome::Accepted(head) => host.shared_sync.update(|state| {
                state.uploaded_through = head.sequence.get();
                state.last_upload_at = Some(fold_db::clock::unix_secs());
            }),
            UploadOutcome::Reconcile(_) => return Err(BackupError::Identity),
        }
    }
    Ok(())
}

async fn download(
    host: &crate::Host,
    config: &config::Config,
    auth: AuthClient,
    signer: &fold_db::security::Ed25519KeyPair,
    content_key: &[u8; 32],
) -> Result<()> {
    let peers = SharedPeerClient::from_auth(auth, config.account.clone(), config.device.clone())
        .map_err(|_| BackupError::Source)?;
    let service = SchemaServiceClient::new(schema_service_url_for(Environment::Dev));
    let mut worker = PeerWorker::new(
        &host.db,
        &peers,
        &service,
        PeerIdentity {
            device_signer: signer,
            content_key,
        },
        BTreeMap::new(),
    );
    for _ in 0..RANGES_PER_PASS {
        match worker.step().await.map_err(|_| BackupError::Source)? {
            (PeerOutcome::Idle, _) => {
                host.shared_sync.update(|state| state.needs_restore = None);
                return Ok(());
            }
            (PeerOutcome::Applied { .. }, _) => {
                host.shared_sync.update(|state| state.applied_ranges += 1);
            }
            (PeerOutcome::NeedsCheckpoint { stream }, _) => {
                host.shared_sync
                    .update(|state| state.needs_restore = Some(stream));
                return Err(BackupError::Source);
            }
        }
    }
    Ok(())
}
