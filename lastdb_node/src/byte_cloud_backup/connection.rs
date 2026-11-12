//! Explicit owner consent for an existing identity, stream binding, and share write gate.
use super::{
    source::{Saved, Source},
    store::{self, Store},
    BackupError, Result,
};
use crate::cloud::{SavedConnection, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use crate::cloud_backup_client::{byte_backup::ByteBackupClient, AuthClient};
use crate::host::Host;
use crate::offline_home::Directory;
use crate::profile::endpoints::{exemem_api_url_for, Environment};
use cloud_backup_protocol::{
    v2::{context::ContextReadRequest, Destination, Stream, Version},
    AccountId, CloudScope, Id,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use zeroize::Zeroize;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamRecord {
    source_home: Id,
    stream: Stream,
}

impl StreamRecord {
    fn matches(&self, source: &Source, scope: &CloudScope) -> bool {
        self.source_home == source.source_home
            && self.stream.scope == *scope
            && self.stream.stream_id == source.publication_stream
    }
}

pub(super) fn context_request(account_id: AccountId) -> ContextReadRequest {
    ContextReadRequest {
        version: Version::V2,
        account_id,
        required_destinations: vec![Destination::R2, Destination::B2],
    }
}

/// A dedicated owner request. The API key never enters the ordinary backup command enum.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SensitiveConnectionRequest {
    pub environment: String,
    pub expected_user_hash: Option<AccountId>,
    pub api_key: String,
}

impl Drop for SensitiveConnectionRequest {
    fn drop(&mut self) {
        self.api_key.zeroize();
    }
}

/// Public connection state contains no credentials and starts no work.
#[derive(Clone, Debug, Serialize)]
pub struct ManualConnectionStatus {
    pub account_id: AccountId,
    pub source_home: Id,
    pub environment: String,
    pub manual_backup_enabled: bool,
    pub automatic_backup_enabled: bool,
    pub registration_attempted: bool,
}

impl ManualConnectionStatus {
    fn of(r: SavedConnection, automatic_backup_enabled: bool) -> Self {
        Self {
            account_id: r.account_id,
            source_home: r.source_home,
            environment: r.environment,
            manual_backup_enabled: true,
            automatic_backup_enabled,
            registration_attempted: false,
        }
    }
}

pub async fn connect_existing(
    host: &Host,
    req: SensitiveConnectionRequest,
) -> Result<ManualConnectionStatus> {
    if super::is_publication_home(&host.home)? {
        return Err(BackupError::Identity);
    }
    let id_before = identity(host)?;
    let env = Environment::try_from_str(&req.environment).map_err(|_| BackupError::Invalid)?;
    let key = req.api_key.as_str();
    if req
        .expected_user_hash
        .as_ref()
        .is_some_and(|u| u != &id_before.0)
        || key.is_empty()
        || key.len() > 4096
        || key.trim() != key
        || key.chars().any(char::is_control)
    {
        return Err(BackupError::Identity);
    }
    let _ = (host.byte_backup.transition()?, preflight(host)?);
    let api_url = exemem_api_url_for(env);
    let client = ByteBackupClient::from_auth(AuthClient::new(api_url.into(), key.into()))?;
    if !client
        .context(&context_request(id_before.0.clone()))
        .await?
        .new_work_allowed
    {
        return Err(BackupError::Authority);
    }
    let _ = (host.byte_backup.transition()?, preflight(host)?);
    if identity(host)? != id_before {
        return Err(BackupError::Identity);
    }
    let (_permit, dir) = (
        Store::lock(&Store::base(&host.home))?,
        Directory::open(&host.home)?,
    );
    let prev = dir.read(CLOUD_CREDENTIALS_FILE)?;
    let mut creds = match &prev {
        Some(bytes) => {
            let saved = SavedCredentials::parse(bytes).map_err(|_| BackupError::Invalid)?;
            (saved.api_url == api_url && saved.api_key.as_str() == key)
                .then_some(saved)
                .ok_or(BackupError::Identity)?
        }
        None => SavedCredentials::new(api_url, key.to_owned()),
    };
    let conn = SavedConnection {
        account_id: id_before.0,
        source_home: id_before.1,
        environment: req.environment.clone(),
        endpoint: client.canonical_endpoint(),
    };
    if creds.connection.as_ref().is_some_and(|s| s != &conn) {
        return Err(BackupError::Identity);
    }
    creds.connection = Some(conn);
    let encoded = creds.to_json().map_err(|_| BackupError::Invalid)?;
    dir.publish(
        CLOUD_CREDENTIALS_FILE,
        prev.as_deref().map(Vec::as_slice),
        &encoded,
    )?;
    let saved = load(host)?.ok_or(BackupError::Identity)?;
    super::register_blob_source(host);
    Ok(ManualConnectionStatus::of(saved, false))
}

fn preflight(host: &Host) -> Result<()> {
    host.byte_backup.check_admission()?;
    (!Store::has_active(&Store::base(&host.home))? && host.byte_backup.snapshot().active.is_none())
        .then_some(())
        .ok_or(BackupError::Busy)
}

fn load(host: &Host) -> Result<Option<SavedConnection>> {
    let (account, source) = identity(host)?;
    let dir = Directory::open(&host.home)?;
    let Some(bytes) = dir.read(CLOUD_CREDENTIALS_FILE)? else {
        return Ok(None);
    };
    let mut creds = SavedCredentials::parse(&bytes).map_err(|_| BackupError::Invalid)?;
    let Some(conn) = creds.connection.take() else {
        return Ok(None);
    };
    let env = Environment::try_from_str(&conn.environment).map_err(|_| BackupError::Invalid)?;
    if conn.account_id != account
        || conn.source_home != source
        || creds.api_url != exemem_api_url_for(env)
        || creds.api_key.is_empty()
    {
        return Err(BackupError::Identity);
    }
    dir.check()?;
    let auth = AuthClient::new(
        exemem_api_url_for(env).into(),
        creds.api_key.as_str().into(),
    );
    let client = ByteBackupClient::from_auth(auth)?;
    (client.canonical_endpoint() == conn.endpoint)
        .then_some(Some(conn))
        .ok_or(BackupError::Identity)
}

pub(super) fn identity(host: &Host) -> Result<(AccountId, Id)> {
    let home = host.db.db_ops().native_runtime().native_home_id();
    Ok((
        AccountId::try_from(host.user_hash.clone())?,
        Id::try_from(home.to_string())?,
    ))
}

pub(super) fn validate_saved_stream(source: &Source, store: &Store, saved: &Saved) -> Result<()> {
    let rec: StreamRecord = store
        .read_base("stream.json")?
        .ok_or(BackupError::Identity)?;
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    (rec.matches(source, &begin.job.stream.scope)
        && rec.stream == begin.job.stream
        && rec.stream.scope.account_id == source.account)
        .then_some(())
        .ok_or(BackupError::Identity)
}

pub(super) fn select_stream(source: &Source, store: &Store, scope: CloudScope) -> Result<Stream> {
    let stream = match store.read_base::<StreamRecord>("stream.json")? {
        Some(r) if r.matches(source, &scope) => r.stream,
        Some(_) => return Err(BackupError::Identity),
        None => Stream {
            scope,
            stream_id: source.publication_stream.clone(),
        },
    };
    stream.validate()?;
    let rec = StreamRecord {
        source_home: source.source_home.clone(),
        stream: stream.clone(),
    };
    store.write_base("stream.json", &rec).map(|()| stream)
}

pub(super) async fn publication_stream(
    base: &Path,
    source: &Source,
    stream_id: &Id,
    client: &ByteBackupClient,
) -> Result<Stream> {
    let stream = match store::read::<StreamRecord>(&base.join("stream.json"))? {
        Some(r) if r.source_home == source.source_home => r.stream,
        Some(_) => return Err(BackupError::Identity),
        None => Stream {
            scope: client
                .context(&context_request(source.account.clone()))
                .await?
                .scope,
            stream_id: stream_id.clone(),
        },
    };
    stream.validate()?;
    (stream.stream_id == *stream_id && stream.scope.account_id == source.account)
        .then_some(stream)
        .ok_or(BackupError::Identity)
}

pub(crate) mod files {
    pub(crate) use crate::offline_home::Directory;
}

pub mod share_write_gate {
    use super::files::Directory;
    use rustix::fs::{flock, FlockOperation};
    use std::{fs::File, io, path::Path};

    const LOCK: &str = "share-edit.lock";
    const HOLD: &str = "share-edit.pending";
    const MARKER: &[u8] = b"1\n";

    pub struct ShareEdit {
        directory: Directory,
        _lock: File,
    }

    fn open_gate(home: &Path) -> Result<(Directory, File), String> {
        let dir = Directory::open(home).map_err(|e| e.to_string())?;
        let lock = dir.open_lock(LOCK).map_err(|e| e.to_string())?;
        Ok((dir, lock))
    }

    pub fn begin_edit(home: &Path) -> Result<ShareEdit, String> {
        let (directory, _lock) = open_gate(home)?;
        flock(&_lock, FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| "a share edit or snapshot capture is active; retry the file import")?;
        match directory.read(HOLD).map_err(|e| e.to_string())? {
            Some(cur) if cur.as_slice() == MARKER => {}
            Some(_) => return Err("invalid pending share edit".into()),
            None => directory
                .publish(HOLD, None, MARKER)
                .map_err(|e| e.to_string())?,
        }
        Ok(ShareEdit { directory, _lock })
    }

    impl ShareEdit {
        pub fn finish(self) -> Result<(), String> {
            self.directory
                .remove_exact(HOLD, MARKER)
                .map_err(|e| e.to_string())
        }
    }

    pub(crate) fn try_capture(home: &Path) -> Result<Option<File>, String> {
        let (directory, lock) = open_gate(home)?;
        match flock(&lock, FlockOperation::NonBlockingLockShared) {
            Err(e) if io::Error::from(e).kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(_) => return Err("cannot lock share publication capture".into()),
            Ok(()) => {}
        }
        Ok(directory
            .read(HOLD)
            .map_err(|e| e.to_string())?
            .is_none()
            .then_some(lock))
    }
}
