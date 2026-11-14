//! Descriptor-pinned home guard for self-serve cloud backup enrollment.
use super::setup::{canonical_backup_endpoint, encode_connected_credentials, PHRASE_FILE};
use cloud_backup_protocol::AccountId;
use lastdb_node::cloud::{
    SavedCredentials, CLOUD_CREDENTIALS_FILE, FIRST_BACKUP_INTENT, FIRST_BACKUP_INTENT_FILE,
};
use lastdb_node::host::IDENTITY_KEY_FILE;
use lastdb_node::offline_home::{
    entry_present, open_directory, open_private_at, read_private_file, require_owned_directory,
    same_inode,
};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use zeroize::Zeroizing;

const DEVICE_FILE: &str = ".native_backup_device_id";
const LOCK_FILE: &str = ".native_backup_enrollment.lock";
pub(super) const PENDING_FILE: &str = ".native_backup_credentials.pending";
const FILE_ERR: &str = "native enrollment file must be a private regular owner file";

pub(super) struct PreparedHome {
    parent: File,
    name: OsString,
    directory: File,
    _lock: File,
}

impl PreparedHome {
    pub(super) fn open(home: &Path) -> Result<Self, String> {
        let parent = open_directory(home.parent().ok_or("native enrollment needs a home")?)?;
        require_owned_directory(&parent)?;
        let directory = open_directory(home)?;
        require_owned_directory(&directory)?;
        let name = home
            .file_name()
            .ok_or("native enrollment needs an explicit home name")?
            .to_os_string();
        let lock = open_private_at(
            &directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK,
            Mode::from_raw_mode(0o600),
            FILE_ERR,
        )?;
        lock.try_lock()
            .map_err(|_| "native enrollment is already active for this home")?;
        let prepared = Self {
            parent,
            name,
            directory,
            _lock: lock,
        };
        prepared.verify_dir().map(|()| prepared)
    }

    fn verify_dir(&self) -> Result<(), String> {
        same_inode(&self.parent, &self.name, &self.directory)
            .then_some(())
            .ok_or_else(|| "selected native enrollment home changed".into())
    }

    pub(super) fn seed(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        self.verify_dir()?;
        let seed: Zeroizing<[u8; 32]> = read_private_file(&self.directory, IDENTITY_KEY_FILE, 32)?
            .as_slice()
            .try_into()
            .map(Zeroizing::new)
            .map_err(|_| "native identity must contain 32 bytes".to_owned())?;
        if !entry_present(&self.directory, PHRASE_FILE)? {
            return Ok(seed);
        }
        let words = Zeroizing::new(read_private_file(&self.directory, PHRASE_FILE, 4096)?);
        let phrase =
            std::str::from_utf8(&words).map_err(|_| "invalid protected recovery phrase")?;
        let recovered = Zeroizing::new(
            lastdb_node::cloud::seed_from_phrase(phrase)
                .map_err(|_| "invalid protected recovery phrase")?,
        );
        (recovered.as_ref() == seed.as_ref())
            .then_some(seed)
            .ok_or_else(|| "protected recovery phrase does not match native identity".into())
    }

    pub(super) fn device_id(&self) -> Result<String, String> {
        self.verify_dir()?;
        if !entry_present(&self.directory, DEVICE_FILE)? {
            self.write_new(DEVICE_FILE, uuid::Uuid::new_v4().to_string().as_bytes())?;
        }
        let text = String::from_utf8(read_private_file(&self.directory, DEVICE_FILE, 36)?)
            .map_err(|_| "invalid saved native device ID")?;
        let id = uuid::Uuid::parse_str(&text).map_err(|_| "invalid saved native device ID")?;
        if id.get_version() != Some(uuid::Version::Random) || id.to_string() != text {
            return Err("native device ID must be a canonical UUIDv4".into());
        }
        self.sync().map(|()| text)
    }

    pub(super) fn finish_saved_credentials(
        &self,
        env: Environment,
        account_id: &AccountId,
    ) -> Result<bool, String> {
        self.verify_dir()?;
        let name = match (
            entry_present(&self.directory, CLOUD_CREDENTIALS_FILE)?,
            entry_present(&self.directory, PENDING_FILE)?,
        ) {
            (true, true) => return Err("native enrollment credential state is ambiguous".into()),
            (true, false) => CLOUD_CREDENTIALS_FILE,
            (false, true) => PENDING_FILE,
            (false, false) => return Ok(false),
        };
        let bytes = Zeroizing::new(read_private_file(&self.directory, name, 65_536)?);
        let creds = SavedCredentials::parse(&bytes)
            .map_err(|_| "invalid saved native credentials; registration may have committed")?;
        if creds.api_url != exemem_api_url_for(env) || creds.api_key.is_empty() {
            return Err(
                "saved native credentials do not match the selected cloud environment".into(),
            );
        }
        let endpoint = canonical_backup_endpoint(env, creds.api_key.as_str())?;
        if let Some(saved) = &creds.connection {
            if &saved.account_id != account_id
                || saved.environment != env.as_str()
                || saved.endpoint != endpoint
            {
                return Err("saved cloud connection does not match this account".into());
            }
            return if name == PENDING_FILE {
                self.publish()
            } else {
                self.sync()
            }
            .map(|()| true);
        }
        let upgraded =
            encode_connected_credentials(env, account_id, creds.api_key.as_str().to_owned(), None)?;
        self.replace_credentials(&upgraded, name == PENDING_FILE)
            .map(|()| true)
    }

    fn replace_credentials(&self, bytes: &[u8], unlink_pending: bool) -> Result<(), String> {
        let temp = format!(".native_backup_upgrade.{}.pending", uuid::Uuid::new_v4());
        self.write_new(&temp, bytes)?;
        let res = fs::renameat(
            &self.directory,
            &temp,
            &self.directory,
            CLOUD_CREDENTIALS_FILE,
        )
        .map_err(|_| "cannot publish connected native credentials".to_owned());
        if res.is_err() {
            let _ = fs::unlinkat(&self.directory, &temp, AtFlags::empty());
            return res;
        }
        if unlink_pending {
            let _ = fs::unlinkat(&self.directory, PENDING_FILE, AtFlags::empty());
        }
        self.sync()
    }

    pub(super) fn publish(&self) -> Result<(), String> {
        self.verify_dir()?;
        if entry_present(&self.directory, CLOUD_CREDENTIALS_FILE)? {
            return Err("refusing to replace saved native credentials".into());
        }
        fs::renameat(
            &self.directory,
            PENDING_FILE,
            &self.directory,
            CLOUD_CREDENTIALS_FILE,
        )
        .map_err(|_| "cannot publish native credentials")?;
        self.sync()
    }

    pub(super) fn write_new(&self, name: &str, bytes: &[u8]) -> Result<(), String> {
        self.verify_dir()?;
        let mut file = open_private_at(
            &self.directory,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
            Mode::from_raw_mode(0o600),
            FILE_ERR,
        )?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write protected native enrollment file")?;
        self.sync()
    }

    pub(super) fn mark_first_backup(&self) -> Result<(), String> {
        if !entry_present(&self.directory, FIRST_BACKUP_INTENT_FILE)? {
            self.write_new(FIRST_BACKUP_INTENT_FILE, FIRST_BACKUP_INTENT)?;
        }
        let bytes = read_private_file(
            &self.directory,
            FIRST_BACKUP_INTENT_FILE,
            FIRST_BACKUP_INTENT.len() as u64,
        )?;
        if bytes != FIRST_BACKUP_INTENT {
            return Err("invalid first backup intent".into());
        }
        self.sync()
    }

    fn sync(&self) -> Result<(), String> {
        self.verify_dir()?;
        self.directory
            .sync_all()
            .and_then(|()| self.parent.sync_all())
            .map_err(|_| "cannot sync native enrollment home".into())
    }
}
