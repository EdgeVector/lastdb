//! Explicit owner consent for an existing identity. No enrollment or automatic work.
use super::{
    store::{self, Store},
    BackupError, Result,
};
use crate::cloud::{SavedConnection, SavedCredentials, CLOUD_CREDENTIALS_FILE};
use crate::cloud_backup_client::{byte_backup::ByteBackupClient, AuthClient};
use crate::profile::endpoints::{exemem_api_url_for, Environment};
use cloud_backup_protocol::{
    bytes_sha256,
    v2::{context::ContextReadRequest, Destination, Version},
    AccountId, Digest, Id,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

pub(crate) mod files;

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

impl From<SavedConnection> for ManualConnectionStatus {
    fn from(record: SavedConnection) -> Self {
        Self {
            account_id: record.account_id,
            source_home: record.source_home,
            environment: record.environment,
            manual_backup_enabled: true,
            automatic_backup_enabled: false,
            registration_attempted: false,
        }
    }
}

pub async fn connect_existing(
    host: &crate::host::Host,
    request: SensitiveConnectionRequest,
) -> Result<ManualConnectionStatus> {
    if super::is_publication_home(&host.home)? {
        return Err(BackupError::Identity);
    }
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
    let previous = directory.read(CLOUD_CREDENTIALS_FILE)?;
    let mut credentials = match &previous {
        Some(bytes) => {
            let saved = SavedCredentials::parse(bytes).map_err(|_| BackupError::Invalid)?;
            if saved.api_url != exemem_api_url_for(environment)
                || saved.api_key.as_str() != request.api_key.as_str()
            {
                return Err(BackupError::Identity);
            }
            saved
        }
        None => SavedCredentials::new(exemem_api_url_for(environment), request.api_key.clone()),
    };
    let connection = SavedConnection {
        account_id: identity_before.0,
        source_home: identity_before.1,
        environment: request.environment.clone(),
        endpoint: client.canonical_endpoint(),
    };
    if credentials
        .connection
        .as_ref()
        .is_some_and(|saved| saved != &connection)
    {
        return Err(BackupError::Identity);
    }
    credentials.connection = Some(connection);
    let encoded = credentials.to_json().map_err(|_| BackupError::Invalid)?;
    // One atomic replace commits the key and the consent together.
    directory.publish(
        CLOUD_CREDENTIALS_FILE,
        previous.as_deref().map(Vec::as_slice),
        &encoded,
    )?;
    let (saved, _) = load(host)?.ok_or(BackupError::Identity)?;
    super::register_blob_source(host);
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

pub(super) fn status(host: &crate::host::Host) -> Result<Option<ManualConnectionStatus>> {
    let mut status: Option<ManualConnectionStatus> = load(host)?.map(|(record, _)| record.into());
    if let Some(status) = &mut status {
        status.automatic_backup_enabled =
            super::automatic::status(host)?.is_some_and(|state| state.enabled);
    }
    Ok(status)
}

/// The connected account, source home and endpoint, and the digest of the saved key.
pub(super) fn automatic_binding(
    host: &crate::host::Host,
) -> Result<(AccountId, Id, String, Digest)> {
    let (account, source) = identity(host)?;
    let directory = files::Directory::open(&host.home)?;
    let (connection, credentials) =
        read_connection(&directory, &account, &source)?.ok_or(BackupError::Disabled)?;
    let key = credentials.key_json().map_err(|_| BackupError::Invalid)?;
    Ok((
        connection.account_id,
        connection.source_home,
        connection.endpoint,
        bytes_sha256(&key),
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
    super::client_from_home(&host.home).map_err(|_| BackupError::Identity)
}

fn load(host: &crate::host::Host) -> Result<Option<(SavedConnection, ByteBackupClient)>> {
    let (account, source) = identity(host)?;
    let directory = files::Directory::open(&host.home)?;
    let Some((connection, credentials)) = read_connection(&directory, &account, &source)? else {
        return Ok(None);
    };
    let client = client(environment(&connection.environment)?, &credentials.api_key)?;
    if client.canonical_endpoint() != connection.endpoint {
        return Err(BackupError::Identity);
    }
    directory.check()?;
    Ok(Some((connection, client)))
}

fn read_connection(
    directory: &files::Directory,
    account: &AccountId,
    source: &Id,
) -> Result<Option<(SavedConnection, SavedCredentials)>> {
    let Some(bytes) = directory.read(CLOUD_CREDENTIALS_FILE)? else {
        return Ok(None);
    };
    let mut credentials = SavedCredentials::parse(&bytes).map_err(|_| BackupError::Invalid)?;
    let Some(connection) = credentials.connection.take() else {
        return Ok(None);
    };
    if &connection.account_id != account || &connection.source_home != source {
        return Err(BackupError::Identity);
    }
    let environment = environment(&connection.environment)?;
    if credentials.api_url != exemem_api_url_for(environment) || credentials.api_key.is_empty() {
        return Err(BackupError::Identity);
    }
    Ok(Some((connection, credentials)))
}

fn identity(host: &crate::host::Host) -> Result<(AccountId, Id)> {
    let runtime = host.db.db_ops().native_runtime();
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
