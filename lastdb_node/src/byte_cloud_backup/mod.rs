//! Explicit V2 byte backup and personal record blob reads.
pub(crate) mod automatic_publication;
pub(crate) mod connection;
mod log_capture;
mod source;
mod store;
mod transfer;
mod work;
mod worker;

pub use crate::backup_runtime::{BackupError, BackupRuntime, Phase};
use crate::backup_runtime::{InvocationOutcome, PauseState};
use crate::cloud_backup_client::byte_backup::ByteBackupClient;
use crate::host::Host;
use async_trait::async_trait;
use cloud_backup_protocol::v2::{
    blobs::{BlobPath, BlobReadItem, BlobReadRequest},
    context::ContextReadRequest,
    AcceptedBackup, Destination, Version,
};
use cloud_backup_protocol::{AccountId, CloudScope, Id};
pub use connection::{
    connect_existing, share_write_gate, ManualConnectionStatus, SensitiveConnectionRequest,
};
use fold_db::fold_db_core::{BlobFetch, RecordBlobSource, SIGN_IN_TO_FETCH_FILE};
use serde::Deserialize;
pub(crate) use source::PublicationSource;
use source::{Saved, Source};
use std::path::Path;
use std::sync::{atomic::AtomicBool, Arc};
use store::Store;
use tokio::io::AsyncReadExt;

pub type Result<T> = std::result::Result<T, BackupError>;
pub const PUBLICATION_MARKER: &str = "share-publication.json";
type BlobInner = (
    ByteBackupClient,
    AccountId,
    tokio::sync::Mutex<Option<CloudScope>>,
);

#[derive(Clone, Debug)]
pub struct AutomaticBackupStatus {
    pub paused: Option<PauseState>,
}

#[derive(Clone, Debug)]
pub struct ByteBackupStatus {
    pub automatic_backup_enabled: bool,
    pub automatic_backup: Option<AutomaticBackupStatus>,
    pub running: bool,
    pub phase: Option<Phase>,
    pub blocked_reason: Option<String>,
}

#[derive(Deserialize)]
struct AutoFile {
    account: AccountId,
    source_home: Id,
    state: AutoState,
}

#[derive(Deserialize)]
struct AutoState {
    enabled: bool,
    paused: Option<PauseState>,
}

pub fn client_from_home(home: &Path) -> std::result::Result<ByteBackupClient, String> {
    ByteBackupClient::from_auth(crate::cloud::auth_for_recovery(home)?).map_err(|e| e.to_string())
}

pub(crate) fn is_publication_home(home: &Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(home.join(PUBLICATION_MARKER)) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn require_enabled() -> Result<()> {
    fold_db::env_flag::var_truthy("LASTDB_BYTE_BACKUP_V2_ENABLED")
        .then_some(())
        .ok_or(BackupError::Disabled)
}

fn spawn_job(
    host: &Host,
    source: Source,
    store: Store,
    saved: Saved,
    client: Option<ByteBackupClient>,
    permit: std::fs::File,
    select: bool,
) -> Result<()> {
    let id = saved.request_id.clone();
    if select {
        store.select(&id)?;
    }
    host.byte_backup.start(id, move |pause| {
        execute(&source, &store, saved, client.as_ref(), permit, &pause)
    })
}

fn execute(
    source: &Source,
    store: &Store,
    mut saved: Saved,
    client: Option<&ByteBackupClient>,
    _permit: std::fs::File,
    pause: &AtomicBool,
) -> InvocationOutcome {
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| BackupError::Source)?;
        if let Some(c) = client {
            rt.block_on(worker::run(source, store, &mut saved, c, pause))
        } else {
            work::check_pause(pause)?;
            rt.block_on(worker::reconcile(source, store, &mut saved))
        }
    }))
    .unwrap_or(Err(BackupError::WorkerPanic));
    let Err(error) = res else {
        return InvocationOutcome::Complete;
    };
    let pause_state = error.pause_state();
    saved.blocked_reason = Some(error.to_string());
    let persisted = store.reopen().and_then(|fresh| {
        fresh.write("pause.json", &pause_state)?;
        fresh.save(&saved)
    });
    InvocationOutcome::Paused(persisted.map_or_else(|e| e.pause_state(), |()| pause_state))
}

pub(crate) fn status(host: &Host) -> Result<ByteBackupStatus> {
    let base = Store::base(&host.home);
    let id = Store::active(&base)?;
    let saved = id
        .as_ref()
        .map(|id| store::state(&base, id))
        .transpose()?
        .flatten();
    let (account, source_home) = connection::identity(host)?;
    if saved
        .as_ref()
        .is_some_and(|s| s.account != account || s.source_home != source_home)
    {
        return Err(BackupError::Identity);
    }
    let accepted: Option<AcceptedBackup> = store::read(&base.join("accepted.json"))?;
    if let Some(acc) = &accepted {
        acc.validate()?;
        if acc.job().stream.scope.account_id != account {
            return Err(BackupError::Identity);
        }
    }
    let auto = match store::read::<AutoFile>(&base.join(store::AUTOMATIC_FILE))? {
        Some(a) if a.account != account || a.source_home != source_home => {
            return Err(BackupError::Identity);
        }
        Some(a) => Some((
            a.state.enabled,
            AutomaticBackupStatus {
                paused: a.state.paused,
            },
        )),
        None => None,
    };
    let running = host
        .byte_backup
        .snapshot()
        .active
        .is_some_and(|a| id.as_ref() == Some(&a));
    Ok(ByteBackupStatus {
        automatic_backup_enabled: !crate::log_sync::legacy_resume_forbidden(host)
            && auto.as_ref().is_some_and(|(en, _)| *en),
        automatic_backup: auto.map(|(_, st)| st),
        running,
        phase: saved.as_ref().map(|s| s.phase),
        blocked_reason: saved.and_then(|s| s.blocked_reason),
    })
}

/// Register the cloud source a personal home uses for non-local record blobs.
pub fn register_blob_source(host: &Host) {
    let has_saved = !matches!(crate::cloud::load_saved_credentials(&host.home), Ok(None));
    let acct = AccountId::try_from(host.user_hash.clone())
        .map_err(|_| "the node account id is not a cloud account id".to_owned());
    let inner = match client_from_home(&host.home).and_then(|c| Ok((c, acct?))) {
        Ok((client, account)) => Some((client, account, tokio::sync::Mutex::new(None))),
        Err(reason) if has_saved => {
            tracing::debug!(target: "lastdbd::blob_source", %reason, "record files download on request is off");
            return;
        }
        Err(_) => None,
    };
    host.db
        .set_record_blob_source(Some(Arc::new(CloudBlobSource(inner))));
}

struct CloudBlobSource(Option<BlobInner>);

#[async_trait]
impl RecordBlobSource for CloudBlobSource {
    async fn fetch(&self, path: &BlobPath, sealed_bytes: u64) -> BlobFetch {
        let Some((client, account, scope)) = &self.0 else {
            return BlobFetch::Unavailable(SIGN_IN_TO_FETCH_FILE.to_owned());
        };
        read_blob(client, account, scope, path, sealed_bytes)
            .await
            .unwrap_or_else(BlobFetch::Unavailable)
    }
}

async fn read_blob(
    client: &ByteBackupClient,
    account: &AccountId,
    cached: &tokio::sync::Mutex<Option<CloudScope>>,
    path: &BlobPath,
    sealed_bytes: u64,
) -> std::result::Result<BlobFetch, String> {
    let mut guard = cached.lock().await;
    let scope = if let Some(s) = guard.clone() {
        s
    } else {
        let req = ContextReadRequest {
            version: Version::V2,
            account_id: account.clone(),
            required_destinations: vec![Destination::B2],
        };
        let s = client
            .context(&req)
            .await
            .map_err(|e| format!("cloud account context: {e}"))?
            .scope;
        *guard = Some(s.clone());
        s
    };
    drop(guard);
    let req = BlobReadRequest {
        version: Version::V2,
        scope,
        paths: vec![path.clone()],
    };
    let resp = match client.blob_read(&req).await {
        Ok(r) => r,
        Err(e) => {
            *cached.lock().await = None;
            return Err(format!("cloud blob read: {e}"));
        }
    };
    let item = resp
        .items
        .into_iter()
        .next()
        .ok_or("cloud blob read returned no result")?;
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
    let mut stream = client
        .open_blob(&item)
        .await
        .map_err(|e| format!("cloud blob download: {e}"))?;
    let mut row = Vec::with_capacity(usize::try_from(sealed_bytes).unwrap_or(0));
    stream
        .read_to_end(&mut row)
        .await
        .map_err(|e| format!("cloud blob download: {e}"))?;
    Ok(BlobFetch::Bytes(row))
}
