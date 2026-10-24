//! Descriptor-bound fresh-home enrollment; retain one device identity across retries.

use super::{
    recovery_files, CLOUD_RESUME_REQUIRED_FILE, CLOUD_SYNC_PAUSED_FILE, IDENTITY_KEY_FILE,
    PHRASE_FILE,
};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

const DEVICE_FILE: &str = ".native_backup_device_id";
const LOCK_FILE: &str = ".native_backup_enrollment.lock";
const PENDING_FILE: &str = ".native_backup_credentials.pending";

#[path = "files/existing.rs"]
mod existing;
#[path = "files/restored.rs"]
mod restored;

pub(super) struct PreparedHome {
    parent: File,
    name: OsString,
    directory: File,
    _lock: File,
    restored: Option<restored::Validation>,
    existing: Option<existing::Validation>,
}

impl PreparedHome {
    pub(super) fn open(home: &Path) -> Result<Self, String> {
        let parent = home
            .parent()
            .ok_or("native enrollment needs an explicit home")?;
        let parent = recovery_files::open_directory(parent)?;
        recovery_files::require_owned_directory(&parent)?;
        let directory = recovery_files::open_directory(home)?;
        require_private_directory(&directory)?;
        verify_contents(&directory)?;
        read_seed(&directory)?;
        let lock = File::from(
            fs::openat(
                &directory,
                LOCK_FILE,
                OFlags::RDWR
                    | OFlags::CREATE
                    | OFlags::NOFOLLOW
                    | OFlags::CLOEXEC
                    | OFlags::NONBLOCK,
                Mode::from_raw_mode(0o600),
            )
            .map_err(|_| "cannot open native enrollment lock")?,
        );
        require_private_file(&lock)?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err("native enrollment is already active for this home".into());
        }
        let prepared = Self {
            parent,
            name: home
                .file_name()
                .ok_or("native enrollment needs an explicit home name")?
                .to_os_string(),
            directory,
            _lock: lock,
            restored: None,
            existing: None,
        };
        prepared.verify_entries()?;
        prepared.seed()?;
        Ok(prepared)
    }

    fn verify_entries(&self) -> Result<(), String> {
        self.verify_binding()?;
        if let Some(existing) = &self.existing {
            existing.check(self)
        } else if let Some(restored) = &self.restored {
            restored.check(self)
        } else {
            verify_contents(&self.directory)
        }
    }

    pub(super) fn seed(&self) -> Result<zeroize::Zeroizing<[u8; 32]>, String> {
        self.verify_binding()?;
        if let Some(existing) = &self.existing {
            existing.seed(self)
        } else if let Some(restored) = &self.restored {
            restored.seed(self)
        } else {
            read_seed(&self.directory)
        }
    }

    pub(super) fn device_id(&self) -> Result<String, String> {
        self.verify_entries()?;
        if !recovery_files::entry_present(&self.directory, DEVICE_FILE)? {
            self.write_new(DEVICE_FILE, uuid::Uuid::new_v4().to_string().as_bytes())?;
        }
        let bytes = recovery_files::read_private_file(&self.directory, DEVICE_FILE, 36)?;
        let text = String::from_utf8(bytes).map_err(|_| "invalid saved native device ID")?;
        let id = uuid::Uuid::parse_str(&text).map_err(|_| "invalid saved native device ID")?;
        if id.get_version() != Some(uuid::Version::Random) || id.to_string() != text {
            return Err("native device ID must be a canonical UUIDv4".into());
        }
        self.sync_file(DEVICE_FILE)?;
        self.sync()?;
        Ok(text)
    }

    pub(super) fn finish_saved_credentials(&self, api_url: &str) -> Result<bool, String> {
        self.verify_entries()?;
        let paused = recovery_files::entry_present(&self.directory, CLOUD_SYNC_PAUSED_FILE)?;
        let pending = recovery_files::entry_present(&self.directory, PENDING_FILE)?;
        if paused && pending {
            return Err("native enrollment credential state is ambiguous".into());
        }
        let name = if paused {
            CLOUD_SYNC_PAUSED_FILE
        } else if pending {
            PENDING_FILE
        } else {
            return Ok(false);
        };
        let bytes = zeroize::Zeroizing::new(recovery_files::read_private_file(
            &self.directory,
            name,
            65_536,
        )?);
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Credentials {
            api_url: String,
            api_key: String,
        }
        let credentials: Credentials = serde_json::from_slice(&bytes)
            .map_err(|_| "invalid saved native credentials; registration may have committed")?;
        if credentials.api_url != api_url || credentials.api_key.is_empty() {
            return Err("saved native credentials do not match the compiled endpoint".into());
        }
        if let Some(restored) = &self.restored {
            super::validate_saved_credentials(api_url, &credentials.api_key, &restored.account)?;
            self.verify_entries()?;
        }
        if let Some(existing) = &self.existing {
            super::validate_saved_credentials(api_url, &credentials.api_key, &existing.account)?;
            existing.verify_owner()?;
            self.verify_entries()?;
        }
        self.sync_file(name)?;
        if pending {
            self.publish()?;
        }
        self.sync()?;
        Ok(true)
    }

    pub(super) fn save_credentials(&self, bytes: &[u8]) -> Result<(), String> {
        self.verify_entries()?;
        self.write_new(PENDING_FILE, bytes)?;
        self.publish()
    }

    fn publish(&self) -> Result<(), String> {
        self.verify_entries()?;
        if let Some(existing) = &self.existing {
            existing.verify_owner()?;
        }
        self.verify_entries()?;
        if recovery_files::entry_present(&self.directory, CLOUD_SYNC_PAUSED_FILE)? {
            return Err("refusing to replace paused native credentials".into());
        }
        fs::renameat_with(
            &self.directory,
            PENDING_FILE,
            &self.directory,
            CLOUD_SYNC_PAUSED_FILE,
            fs::RenameFlags::NOREPLACE,
        )
        .map_err(|_| "cannot publish paused native credentials")?;
        self.sync()
    }

    fn write_new(&self, name: &str, bytes: &[u8]) -> Result<(), String> {
        self.verify_entries()?;
        let mut file = File::from(
            fs::openat(
                &self.directory,
                name,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )
            .map_err(|_| "cannot create protected native enrollment file")?,
        );
        require_private_file(&file)?;
        file.write_all(bytes)
            .map_err(|_| "cannot write protected native enrollment file")?;
        file.sync_all()
            .map_err(|_| "cannot sync protected native enrollment file")?;
        self.sync()
    }

    fn sync_file(&self, name: &str) -> Result<(), String> {
        let file = File::from(
            fs::openat(
                &self.directory,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| "cannot reopen native enrollment file")?,
        );
        require_private_file(&file)?;
        file.sync_all()
            .map_err(|_| "cannot sync saved native enrollment file".into())
    }

    fn sync(&self) -> Result<(), String> {
        self.verify_entries()?;
        self.directory
            .sync_all()
            .map_err(|_| "cannot sync native enrollment home")?;
        self.parent
            .sync_all()
            .map_err(|_| "cannot sync native enrollment parent".into())
    }

    fn verify_binding(&self) -> Result<(), String> {
        let held =
            fs::fstat(&self.directory).map_err(|_| "cannot inspect held native enrollment home")?;
        let current = fs::statat(&self.parent, &self.name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| "cannot inspect selected native enrollment home")?;
        if held.st_dev != current.st_dev || held.st_ino != current.st_ino {
            return Err("selected native enrollment home changed".into());
        }
        Ok(())
    }
}

fn verify_contents(directory: &File) -> Result<(), String> {
    let allowed = [
        IDENTITY_KEY_FILE,
        PHRASE_FILE,
        CLOUD_RESUME_REQUIRED_FILE,
        DEVICE_FILE,
        LOCK_FILE,
        PENDING_FILE,
        CLOUD_SYNC_PAUSED_FILE,
    ];
    let mut count = 0;
    for entry in
        fs::Dir::read_from(directory).map_err(|_| "cannot inspect native enrollment home")?
    {
        let entry = entry.map_err(|_| "cannot inspect native enrollment entry")?;
        let name = entry.file_name().to_bytes();
        if matches!(name, b"." | b"..") {
            continue;
        }
        count += 1;
        if count > allowed.len() || !allowed.iter().any(|allowed| allowed.as_bytes() == name) {
            return Err("native enrollment home contains a store or an unexpected entry".into());
        }
    }
    if !recovery_files::entry_present(directory, CLOUD_RESUME_REQUIRED_FILE)?
        || !recovery_files::read_private_file(directory, CLOUD_RESUME_REQUIRED_FILE, 0)?.is_empty()
    {
        return Err("native enrollment requires the fresh Cloud Sync Off marker".into());
    }
    Ok(())
}

fn read_seed(directory: &File) -> Result<zeroize::Zeroizing<[u8; 32]>, String> {
    let bytes = zeroize::Zeroizing::new(recovery_files::read_private_file(
        directory,
        IDENTITY_KEY_FILE,
        32,
    )?);
    let seed: zeroize::Zeroizing<[u8; 32]> = zeroize::Zeroizing::new(
        bytes
            .as_slice()
            .try_into()
            .map_err(|_| "native identity must contain 32 bytes")?,
    );
    let words = zeroize::Zeroizing::new(recovery_files::read_private_file(
        directory,
        PHRASE_FILE,
        4096,
    )?);
    let phrase = std::str::from_utf8(&words).map_err(|_| "invalid protected recovery phrase")?;
    let recovered = zeroize::Zeroizing::new(
        lastdb_node::cloud::seed_from_phrase(phrase)
            .map_err(|_| "invalid protected recovery phrase")?,
    );
    if recovered.as_ref() != seed.as_ref() {
        return Err("protected recovery phrase does not match native identity".into());
    }
    Ok(seed)
}

fn require_private_directory(directory: &File) -> Result<(), String> {
    let metadata = directory
        .metadata()
        .map_err(|_| "cannot inspect native enrollment directory")?;
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err("native enrollment directory must be owner-only with mode0700".into());
    }
    Ok(())
}

fn require_private_file(file: &File) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect native enrollment file")?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.nlink() != 1
    {
        return Err("native enrollment file must be a private regular owner file".into());
    }
    Ok(())
}
