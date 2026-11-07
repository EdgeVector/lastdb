//! Private connection and automatic policy for one bound shared writer.

use crate::byte_cloud_backup::connection::files::Directory;
use crate::byte_cloud_backup::{BackupError, Result};
use crate::cloud::{load_saved_credentials, SavedCredentials};
use cloud_backup_protocol::{shared::WriterStream, AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{exemem_api_url_for, Environment};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use zeroize::{Zeroize, Zeroizing};

const FILE: &str = "shared-cloud-sync.json";
const MAX_INTERVAL: u64 = 7 * 86400;

// No Debug: this record includes the device seed and API credential.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub account: AccountId,
    pub home: PathBuf,
    pub stream: WriterStream,
    pub device: Id,
    pub credentials: SavedCredentials,
    seed: Vec<u8>,
    pub publisher: bool,
    pub enabled: bool,
    pub interval_seconds: u64,
    pub snapshot_interval_seconds: u64,
    pub next_snapshot_at: u64,
    pub last_snapshot_at: Option<u64>,
    pub last_snapshot_id: Option<Id>,
    pub pending_snapshot: Option<Id>,
    pub completed_snapshot: Option<Id>,
}

impl Drop for Config {
    fn drop(&mut self) {
        self.seed.zeroize();
    }
}

impl Config {
    pub fn signer(&self) -> Result<Ed25519KeyPair> {
        Ed25519KeyPair::from_secret_key(&self.seed).map_err(|_| BackupError::Identity)
    }

    pub fn validate(&self, home: &Path) -> Result<()> {
        if self.home != home.canonicalize()?
            || self.credentials.api_url != exemem_api_url_for(Environment::Dev)
            || self.credentials.api_key.trim().is_empty()
            || self.seed.len() != 32
            || !(1..=MAX_INTERVAL).contains(&self.interval_seconds)
            || !(60..=MAX_INTERVAL).contains(&self.snapshot_interval_seconds)
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
}

pub(super) fn load(home: &Path) -> Result<Option<Config>> {
    let Some(bytes) = Directory::open(home)?.read(FILE)? else {
        return Ok(None);
    };
    let config: Config = serde_json::from_slice(&bytes).map_err(|_| BackupError::Invalid)?;
    config.validate(home)?;
    Ok(Some(config))
}

pub(super) fn save(home: &Path, config: &Config) -> Result<()> {
    config.validate(home)?;
    let directory = Directory::open(home)?;
    let previous = directory.read(FILE)?;
    let encoded = Zeroizing::new(serde_json::to_vec(config).map_err(|_| BackupError::Invalid)?);
    directory.publish(FILE, previous.as_deref().map(Vec::as_slice), &encoded)
}

/// Save the already authenticated device after native writer binding succeeds.
/// Automatic work follows the owner's existing backup policy switch.
pub fn save_bound(
    home: &Path,
    account: &AccountId,
    stream: WriterStream,
    device: &Id,
    signer: &Ed25519KeyPair,
    publisher: bool,
) -> Result<()> {
    let credentials = load_saved_credentials(home)
        .map_err(|_| BackupError::Identity)?
        .ok_or(BackupError::Missing)?;
    let mut config = match load(home)? {
        Some(mut config) => {
            if config.stream != stream
                || config.device != *device
                || config.seed != signer.secret_key_bytes()
                || config.account != *account
            {
                return Err(BackupError::Identity);
            }
            config.credentials = credentials;
            config
        }
        None => Config {
            account: account.clone(),
            home: home.canonicalize()?,
            stream,
            device: device.clone(),
            credentials,
            seed: signer.secret_key_bytes().to_vec(),
            publisher,
            enabled: false,
            interval_seconds: 60,
            snapshot_interval_seconds: 86400,
            next_snapshot_at: 0,
            last_snapshot_at: None,
            last_snapshot_id: None,
            pending_snapshot: None,
            completed_snapshot: None,
        },
    };
    config.publisher |= publisher;
    save(home, &config)
}

pub(super) fn configure(host: &crate::Host, enabled: bool, log: u64, snapshot: u64) -> Result<()> {
    let _activity = host
        .shared_sync
        .gate
        .try_lock()
        .map_err(|_| BackupError::Busy)?;
    let mut config = load(&host.home)?.ok_or(BackupError::Missing)?;
    if config.account.as_str() != host.user_hash {
        return Err(BackupError::Identity);
    }
    if enabled {
        config.interval_seconds = log;
        config.snapshot_interval_seconds = snapshot;
    }
    config.enabled = enabled;
    save(&host.home, &config)?;
    host.shared_sync.update(|state| {
        state.next_sync_at = 0;
        state.retry_at = 0;
        state.blocked_reason = None;
        state.needs_restore = None;
    });
    Ok(())
}
