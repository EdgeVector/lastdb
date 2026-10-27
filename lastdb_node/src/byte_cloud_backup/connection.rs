//! Explicit owner consent for an existing identity. No enrollment or automatic work.
use super::{
    store::{self, Store},
    BackupError, Result,
};
use cloud_backup_protocol::{
    v2::{context::ContextReadRequest, Destination, Version},
    AccountId, Digest, Id,
};
use fold_db::sync::auth::{byte_backup::ByteBackupClient, AuthClient};
use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

mod files;
mod model;
use model::{Credentials, ManualConnectionRecord};
pub use model::{ManualConnectionStatus, SensitiveConnectionRequest};

const RECORD: &str = "cloud-byte-backup-v2-manual.json";

pub async fn connect_existing(
    host: &crate::host::Host,
    request: SensitiveConnectionRequest,
) -> Result<ManualConnectionStatus> {
    let identity_before = identity(host)?;
    let environment = environment(&request.environment)?;
    if request
        .expected_user_hash
        .as_ref()
        .is_some_and(|id| id != &identity_before.0)
        || request.api_key.is_empty()
        || request.api_key.len() > 4096
        || request.api_key.trim() != request.api_key
        || request.api_key.chars().any(char::is_control)
    {
        return Err(BackupError::Identity);
    }
    {
        let _transition = host.byte_backup.transition()?;
        preflight(host)?;
    }
    let client = client(environment, &request.api_key)?;
    let context = client
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: identity_before.0.clone(),
            required_destinations: vec![Destination::R2, Destination::B2],
        })
        .await?;
    if !context.new_work_allowed {
        return Err(BackupError::Authority);
    }
    let _transition = host.byte_backup.transition()?;
    preflight(host)?;
    if identity(host)? != identity_before {
        return Err(BackupError::Identity);
    }
    let _permit = Store::lock(&host.home)?;
    let directory = files::Directory::open(&host.home)?;
    let credentials = Credentials {
        api_url: exemem_api_url_for(environment).to_owned(),
        api_key: request.api_key.clone(),
    };
    let bytes = credential_bytes(&directory, &credentials)?;
    let record = ManualConnectionRecord {
        version: 1,
        account_id: identity_before.0,
        source_home: identity_before.1,
        environment: request.environment.clone(),
        endpoint: client.canonical_endpoint(),
        credential_sha256: digest(&bytes)?,
        origin: "imported".to_owned(),
        registration_attempted: false,
        automatic_backup_enabled: false,
    };
    let encoded = serde_json::to_vec(&record).map_err(|_| BackupError::Invalid)?;
    // The record is the commit marker. A crash after credentials grants no manual consent.
    if let Some(previous) = directory.read(RECORD)? {
        if previous.as_slice() != encoded {
            return Err(BackupError::Identity);
        }
    }
    directory.publish(crate::cloud::CLOUD_SYNC_PAUSED_FILE, &bytes)?;
    directory.publish(RECORD, &encoded)?;
    let (saved, _) = load(host)?.ok_or(BackupError::Identity)?;
    Ok(saved.into())
}

fn preflight(host: &crate::host::Host) -> Result<()> {
    host.byte_backup.check_admission()?;
    if host.byte_backup.snapshot().active.is_some() {
        return Err(BackupError::Busy);
    }
    if let Some(id) = Store::active(&host.home)? {
        if !store::state(&host.home, &id)?
            .ok_or(BackupError::Invalid)?
            .terminal()
        {
            return Err(BackupError::Busy);
        }
    }
    Ok(())
}

fn credential_bytes(
    directory: &files::Directory,
    supplied: &Credentials,
) -> Result<Zeroizing<Vec<u8>>> {
    if let Some(bytes) = directory.read(crate::cloud::CLOUD_SYNC_PAUSED_FILE)? {
        let saved: Credentials =
            serde_json::from_slice(&bytes).map_err(|_| BackupError::Invalid)?;
        if saved.api_url != supplied.api_url || saved.api_key != supplied.api_key {
            return Err(BackupError::Identity);
        }
        return Ok(bytes);
    }
    Ok(Zeroizing::new(
        serde_json::to_vec(supplied).map_err(|_| BackupError::Invalid)?,
    ))
}

pub(super) fn status(host: &crate::host::Host) -> Result<Option<ManualConnectionStatus>> {
    let mut status: Option<ManualConnectionStatus> = load(host)?.map(|(record, _)| record.into());
    if let Some(status) = &mut status {
        status.automatic_backup_enabled =
            super::automatic::status(host)?.is_some_and(|state| state.enabled);
    }
    Ok(status)
}

pub(super) fn automatic_binding(
    host: &crate::host::Host,
) -> Result<(AccountId, Id, String, Digest)> {
    let (record, _) = load(host)?.ok_or(BackupError::Disabled)?;
    Ok((
        record.account_id,
        record.source_home,
        record.endpoint,
        record.credential_sha256,
    ))
}

pub(super) fn require_admission(host: &crate::host::Host) -> Result<()> {
    if super::require_enabled().is_ok() || load(host)?.is_some() {
        Ok(())
    } else {
        Err(BackupError::Disabled)
    }
}

pub(super) fn client_for_host(host: &crate::host::Host) -> Result<ByteBackupClient> {
    if let Some((_, client)) = load(host)? {
        return Ok(client);
    }
    super::require_enabled()?;
    super::credentials::client_for_recovery(&host.home).map_err(|_| BackupError::Identity)
}

fn load(host: &crate::host::Host) -> Result<Option<(ManualConnectionRecord, ByteBackupClient)>> {
    let directory = files::Directory::open(&host.home)?;
    let Some(bytes) = directory.read(RECORD)? else {
        return Ok(None);
    };
    let record: ManualConnectionRecord =
        serde_json::from_slice(&bytes).map_err(|_| BackupError::Invalid)?;
    record.validate(host)?;
    let bytes = directory
        .read(crate::cloud::CLOUD_SYNC_PAUSED_FILE)?
        .ok_or(BackupError::Identity)?;
    if digest(&bytes)? != record.credential_sha256 {
        return Err(BackupError::Identity);
    }
    let credentials: Credentials =
        serde_json::from_slice(&bytes).map_err(|_| BackupError::Invalid)?;
    let environment = environment(&record.environment)?;
    if credentials.api_url != exemem_api_url_for(environment) || credentials.api_key.is_empty() {
        return Err(BackupError::Identity);
    }
    let client = client(environment, &credentials.api_key)?;
    if client.canonical_endpoint() != record.endpoint {
        return Err(BackupError::Identity);
    }
    directory.check()?;
    Ok(Some((record, client)))
}

fn identity(host: &crate::host::Host) -> Result<(AccountId, Id)> {
    let runtime = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or(BackupError::Source)?;
    Ok((
        AccountId::try_from(host.user_hash.clone())?,
        Id::try_from(runtime.native_home_id().to_string())?,
    ))
}

fn environment(value: &str) -> Result<Environment> {
    match value {
        "dev" => Ok(Environment::Dev),
        "prod" => Ok(Environment::Prod),
        _ => Err(BackupError::Invalid),
    }
}

fn client(environment: Environment, key: &str) -> Result<ByteBackupClient> {
    Ok(ByteBackupClient::from_auth(AuthClient::new(
        exemem_api_url_for(environment).to_owned(),
        key.to_owned(),
    ))?)
}

fn digest(bytes: &[u8]) -> Result<Digest> {
    Ok(Digest::try_from(fold_db::hex::hex_lower(Sha256::digest(
        bytes,
    )))?)
}
