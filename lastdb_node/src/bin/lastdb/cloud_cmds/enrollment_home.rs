//! Descriptor-pinned home guards for self-serve cloud backup enrollment and fresh homes.
use super::setup::{canonical_backup_endpoint, encode_connected_credentials, PHRASE_FILE};
use cloud_backup_protocol::AccountId;
use lastdb_node::cloud::{SavedCredentials, CLOUD_CREDENTIALS_FILE};
use lastdb_node::offline_home::{
    dir_entry_names, entry_present, open_directory, open_private_at, read_identity_seed_at,
    read_private_file, require_absolute, require_mode_0700_directory, require_owned_directory,
    same_inode, DIR_FLAGS,
};
use lastdb_node::profile::endpoints::{exemem_api_url_for, Environment};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

const DEVICE_FILE: &str = ".native_backup_device_id";
const LOCK_FILE: &str = ".native_backup_enrollment.lock";
pub(super) const PENDING_FILE: &str = ".native_backup_credentials.pending";
const FILE_ERR: &str = "native enrollment file must be a private regular owner file";

pub(super) fn resolve_existing_home_path(home: &Path) -> Result<PathBuf, String> {
    let name = home
        .file_name()
        .ok_or("native enrollment needs an explicit home name")?;
    if home.is_absolute() {
        return Ok(home.to_path_buf());
    }
    let raw_parent = home
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = std::fs::canonicalize(raw_parent)
        .map_err(|_| "native enrollment needs an existing parent directory")?;
    Ok(parent.join(name))
}

pub(super) struct PreparedHome {
    parent: File,
    name: OsString,
    directory: File,
    _lock: File,
}

impl PreparedHome {
    pub(super) fn open(home: &Path) -> Result<Self, String> {
        let resolved = resolve_existing_home_path(home)?;
        let home = resolved.as_path();
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
        let seed = read_identity_seed_at(&self.directory)?;
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
        let endpoint = canonical_backup_endpoint(env)?;
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

    fn sync(&self) -> Result<(), String> {
        self.verify_dir()?;
        self.directory
            .sync_all()
            .and_then(|()| self.parent.sync_all())
            .map_err(|_| "cannot sync native enrollment home".into())
    }
}

pub(super) struct FreshHome {
    parent: File,
    name: OsString,
    directory: Option<File>,
    created_files: Vec<&'static str>,
    committed: bool,
}

impl FreshHome {
    pub(super) fn preflight_empty_parent(destination: &Path) -> Result<Self, String> {
        let target = Self::preflight_empty(destination)?;
        require_mode_0700_directory(
            &target.parent,
            "credential parent must be an owner-only directory",
        )?;
        if !dir_entry_names(&target.parent, "cannot inspect credential parent")?.is_empty() {
            return Err("credential parent must be empty".into());
        }
        Ok(target)
    }

    pub(super) fn preflight_empty(destination: &Path) -> Result<Self, String> {
        require_absolute(destination)?;
        let name = destination
            .file_name()
            .ok_or("destination requires a home name")?;
        let parent = open_directory(destination.parent().ok_or("missing destination parent")?)?;
        let mode = parent
            .metadata()
            .map_err(|_| "cannot inspect destination parent")?
            .permissions()
            .mode();
        if mode & 0o022 != 0 && mode & 0o1000 == 0 {
            return Err(
                "destination parent must prevent other users from replacing its entries".into(),
            );
        }
        if entry_present(&parent, name.to_str().ok_or("invalid home name")?)? {
            return Err("recovery home must not already exist".into());
        }
        Ok(Self {
            parent,
            name: name.to_os_string(),
            directory: None,
            created_files: Vec::new(),
            committed: false,
        })
    }

    pub(super) fn create(mut self) -> Result<Self, String> {
        fs::mkdirat(&self.parent, &self.name, Mode::from_raw_mode(0o700))
            .map_err(|_| "cannot exclusively create recovery home")?;
        let dir: File = fs::openat(&self.parent, &self.name, DIR_FLAGS, Mode::empty())
            .map_err(|_| "cannot open new recovery home without symlinks")?
            .into();
        fs::fchmod(&dir, Mode::from_raw_mode(0o700)).map_err(|_| "cannot protect recovery home")?;
        require_owned_directory(&dir)?;
        self.directory = Some(dir);
        Ok(self)
    }

    fn dir(&self) -> &File {
        self.directory.as_ref().expect("created recovery home")
    }

    pub(super) fn write(&mut self, name: &'static str, bytes: &[u8]) -> Result<(), String> {
        let mut file = open_private_at(
            self.dir(),
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
            Mode::from_raw_mode(0o600),
            "cannot exclusively create recovery credential file",
        )?;
        self.created_files.push(name);
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write recovery credential file".into())
    }

    pub(super) fn commit(&mut self) -> Result<(), String> {
        let dir = self.dir();
        if !same_inode(&self.parent, &self.name, dir) {
            return Err("recovery home changed during preparation".into());
        }
        const EXACT_ERR: &str = "recovery home requires the exact protected credential files";
        require_mode_0700_directory(dir, EXACT_ERR)?;
        let entries = dir_entry_names(dir, "cannot inspect final recovery-home entries")?;
        if self.created_files.is_empty()
            || entries.len() != self.created_files.len()
            || entries
                .iter()
                .any(|n| !self.created_files.iter().any(|f| f.as_bytes() == n))
        {
            return Err(EXACT_ERR.into());
        }
        dir.sync_all()
            .and_then(|()| self.parent.sync_all())
            .map_err(|_| "cannot sync recovery home".to_owned())?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for FreshHome {
    fn drop(&mut self) {
        if self.committed || self.directory.is_none() {
            return;
        }
        for name in &self.created_files {
            let _ = fs::unlinkat(self.dir(), *name, AtFlags::empty());
        }
        if same_inode(&self.parent, &self.name, self.dir()) {
            let _ = fs::unlinkat(&self.parent, &self.name, AtFlags::REMOVEDIR);
        }
    }
}
