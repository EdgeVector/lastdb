//! Descriptor-bound fresh-home enrollment and restored-home reconnect.

use super::{recovery_files, CLOUD_CREDENTIALS_FILE, IDENTITY_KEY_FILE, PHRASE_FILE};
use cloud_backup_protocol::{bytes_sha256, Digest};
use fold_db::crypto::E2eKeys;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::disk_pack::run_restore;
use lastdb_node::byte_cloud_restore::{verify_restore_receipt, RESTORE_RECEIPT_NAME};
use rustix::fs::{self, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

const DEVICE_FILE: &str = ".native_backup_device_id";
const LOCK_FILE: &str = ".native_backup_enrollment.lock";
const PENDING_FILE: &str = ".native_backup_credentials.pending";

pub(super) struct PreparedHome {
    parent: File,
    name: OsString,
    directory: File,
    _lock: File,
    restored: Option<RestoredValidation>,
}

struct RestoredValidation {
    guard: run_restore::RunNodeGuard,
    identity: File,
    seed: Zeroizing<[u8; 32]>,
    home: PathBuf,
    receipt: File,
    receipt_digest: Digest,
    account: String,
}

impl PreparedHome {
    fn from_parts(
        parent: File,
        name: OsString,
        directory: File,
        restored: Option<RestoredValidation>,
    ) -> Result<Self, String> {
        let lock = open_file(
            &directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK,
            Mode::from_raw_mode(0o600),
        )?;
        lock.try_lock()
            .map_err(|_| "native enrollment is already active for this home")?;
        let prepared = Self {
            parent,
            name,
            _lock: lock,
            directory,
            restored,
        };
        prepared.verify_entries()?;
        Ok(prepared)
    }

    pub(super) fn open(home: &Path) -> Result<Self, String> {
        let (parent, name, directory) = open_home_directory(home)?;
        let prepared = Self::from_parts(parent, name, directory, None)?;
        prepared.seed()?;
        Ok(prepared)
    }

    pub(crate) fn open_restored(home: &Path) -> Result<Self, String> {
        if !home.is_absolute() {
            return Err("restored reconnect requires an explicit absolute home".into());
        }
        let user = std::env::var_os("HOME").ok_or("HOME is unset")?;
        if home.starts_with(PathBuf::from(user).join(".primary-lastdb-archive-20261014")) {
            return Err("restored reconnect cannot use the primary archive".into());
        }
        lastdb_node::offline_home::refuse_primary(home)?;
        let (parent, name, directory) = open_home_directory(home)?;
        refuse_daemon(home)?;
        let (receipt, receipt_digest) = open_receipt(&directory, home)?;
        let identity = open_readonly(&directory, IDENTITY_KEY_FILE)?;
        identity
            .try_lock()
            .map_err(|_| "restored identity is already owned")?;
        let seed = identity_seed(&directory, Some(&identity))?;
        let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| "invalid restored account identity")?;
        let account = lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
        let content = Zeroizing::new(
            E2eKeys::from_ed25519_seed(&seed)
                .map_err(|_| "cannot derive restored native account key")?
                .encryption_key(),
        );
        let data = home.join("data");
        recovery_files::open_directory(&data)?;
        let guard = run_restore::validate_restored_node(&data, &content).map_err(|_| {
            "restored native home is active, unsupported, or belongs to another identity"
        })?;
        Self::from_parts(
            parent,
            name,
            directory,
            Some(RestoredValidation {
                guard,
                identity,
                seed,
                home: home.to_path_buf(),
                receipt,
                receipt_digest,
                account,
            }),
        )
    }

    fn verify_entries(&self) -> Result<(), String> {
        self.verify_binding()?;
        if let Some(restored) = &self.restored {
            restored.check(&self.directory)
        } else {
            verify_contents(&self.directory)
        }
    }

    pub(super) fn seed(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        self.verify_binding()?;
        if let Some(restored) = &self.restored {
            restored.seed(&self.directory)
        } else {
            read_fresh_seed(&self.directory)
        }
    }

    pub(super) fn device_id(&self) -> Result<String, String> {
        self.verify_entries()?;
        if !recovery_files::entry_present(&self.directory, DEVICE_FILE)? {
            self.write_new(DEVICE_FILE, uuid::Uuid::new_v4().to_string().as_bytes())?;
        }
        let text = String::from_utf8(recovery_files::read_private_file(
            &self.directory,
            DEVICE_FILE,
            36,
        )?)
        .map_err(|_| "invalid saved native device ID")?;
        let id = uuid::Uuid::parse_str(&text).map_err(|_| "invalid saved native device ID")?;
        if id.get_version() != Some(uuid::Version::Random) || id.to_string() != text {
            return Err("native device ID must be a canonical UUIDv4".into());
        }
        open_readonly(&self.directory, DEVICE_FILE)?
            .sync_all()
            .map_err(|_| "cannot sync saved native enrollment file")?;
        self.sync()?;
        Ok(text)
    }

    pub(super) fn finish_saved_credentials(&self, api_url: &str) -> Result<bool, String> {
        self.verify_entries()?;
        let name = match (
            recovery_files::entry_present(&self.directory, CLOUD_CREDENTIALS_FILE)?,
            recovery_files::entry_present(&self.directory, PENDING_FILE)?,
        ) {
            (true, true) => return Err("native enrollment credential state is ambiguous".into()),
            (true, false) => CLOUD_CREDENTIALS_FILE,
            (false, true) => PENDING_FILE,
            (false, false) => return Ok(false),
        };
        let bytes = Zeroizing::new(recovery_files::read_private_file(
            &self.directory,
            name,
            65_536,
        )?);
        let credentials = lastdb_node::cloud::SavedCredentials::parse(&bytes)
            .map_err(|_| "invalid saved native credentials; registration may have committed")?;
        if credentials.api_url != api_url || credentials.api_key.is_empty() {
            return Err("saved native credentials do not match the compiled DEV endpoint".into());
        }
        if let Some(restored) = &self.restored {
            super::validate_restored_credentials(api_url, &credentials.api_key, &restored.account)?;
            self.verify_entries()?;
        }
        open_readonly(&self.directory, name)?
            .sync_all()
            .map_err(|_| "cannot sync saved native enrollment file")?;
        if name == PENDING_FILE {
            self.publish()?;
        } else {
            self.sync()?;
        }
        Ok(true)
    }

    pub(super) fn save_credentials(&self, bytes: &[u8]) -> Result<(), String> {
        self.verify_entries()?;
        self.write_new(PENDING_FILE, bytes)?;
        self.publish()
    }

    fn publish(&self) -> Result<(), String> {
        self.verify_entries()?;
        if recovery_files::entry_present(&self.directory, CLOUD_CREDENTIALS_FILE)? {
            return Err("refusing to replace paused native credentials".into());
        }
        fs::renameat(
            &self.directory,
            PENDING_FILE,
            &self.directory,
            CLOUD_CREDENTIALS_FILE,
        )
        .map_err(|_| "cannot publish paused native credentials")?;
        self.sync()
    }

    fn write_new(&self, name: &str, bytes: &[u8]) -> Result<(), String> {
        self.verify_entries()?;
        let mut file = open_file(
            &self.directory,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
            Mode::from_raw_mode(0o600),
        )?;
        file.write_all(bytes)
            .map_err(|_| "cannot write protected native enrollment file")?;
        file.sync_all()
            .map_err(|_| "cannot sync protected native enrollment file")?;
        self.sync()
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
        if !recovery_files::same_inode(&self.parent, &self.name, &self.directory) {
            return Err("selected native enrollment home changed".into());
        }
        Ok(())
    }
}

impl RestoredValidation {
    fn check(&self, directory: &File) -> Result<(), String> {
        refuse_daemon(&self.home)?;
        self.guard
            .check_binding()
            .map_err(|_| "restored native home changed during reconnect")?;
        self.seed(directory)?;
        if bytes_sha256(&read_receipt(directory, &self.receipt)?) != self.receipt_digest {
            return Err("restore receipt changed during reconnect".into());
        }
        Ok(())
    }

    fn seed(&self, directory: &File) -> Result<Zeroizing<[u8; 32]>, String> {
        let seed = identity_seed(directory, Some(&self.identity))?;
        if seed.as_ref() != self.seed.as_ref() {
            return Err("restored account identity changed during reconnect".into());
        }
        Ok(seed)
    }
}

fn open_receipt(directory: &File, home: &Path) -> Result<(File, Digest), String> {
    if !recovery_files::entry_present(directory, RESTORE_RECEIPT_NAME)? {
        return Err("restored reconnect requires the completed restore receipt".into());
    }
    let file = open_readonly(directory, RESTORE_RECEIPT_NAME)?;
    let bytes = read_receipt(directory, &file)?;
    verify_restore_receipt(&bytes, home)
        .map_err(|_| "native restore provenance does not match this home")?;
    Ok((file, bytes_sha256(&bytes)))
}

fn read_receipt(directory: &File, file: &File) -> Result<Vec<u8>, String> {
    recovery_files::read_private_file_bound(directory, RESTORE_RECEIPT_NAME, 65_536, Some(file))
        .map_err(|_| "restored reconnect requires the completed restore receipt".into())
}

fn open_home_directory(home: &Path) -> Result<(File, OsString, File), String> {
    let parent =
        recovery_files::open_directory(home.parent().ok_or("native enrollment needs a home")?)?;
    recovery_files::require_owned_directory(&parent)?;
    let directory = recovery_files::open_directory(home)?;
    recovery_files::require_mode_0700_directory(
        &directory,
        "native enrollment directory must be owner-only with mode0700",
    )?;
    let name = home
        .file_name()
        .ok_or("native enrollment needs an explicit home name")?
        .to_os_string();
    Ok((parent, name, directory))
}

fn open_file(directory: &File, name: &str, flags: OFlags, mode: Mode) -> Result<File, String> {
    let flags = flags | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let fd = fs::openat(directory, name, flags, mode)
        .map_err(|_| "cannot open protected native enrollment file")?;
    let file = File::from(fd);
    recovery_files::require_private_file(
        &file,
        None,
        "native enrollment file must be a private regular owner file",
    )?;
    Ok(file)
}

fn open_readonly(directory: &File, name: &str) -> Result<File, String> {
    open_file(
        directory,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK,
        Mode::empty(),
    )
}

fn verify_contents(directory: &File) -> Result<(), String> {
    let allowed = [
        IDENTITY_KEY_FILE,
        PHRASE_FILE,
        DEVICE_FILE,
        LOCK_FILE,
        PENDING_FILE,
        CLOUD_CREDENTIALS_FILE,
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
        if count > allowed.len() || !allowed.iter().any(|item| item.as_bytes() == name) {
            return Err("native enrollment home contains a store or an unexpected entry".into());
        }
    }
    Ok(())
}

fn identity_seed(directory: &File, expected: Option<&File>) -> Result<Zeroizing<[u8; 32]>, String> {
    let raw = recovery_files::read_private_file_bound(directory, IDENTITY_KEY_FILE, 32, expected)?;
    let bytes = Zeroizing::new(raw);
    bytes
        .as_slice()
        .try_into()
        .map(Zeroizing::new)
        .map_err(|_| "native identity must contain 32 bytes".into())
}

fn read_fresh_seed(directory: &File) -> Result<Zeroizing<[u8; 32]>, String> {
    let seed = identity_seed(directory, None)?;
    let words = Zeroizing::new(recovery_files::read_private_file(
        directory,
        PHRASE_FILE,
        4096,
    )?);
    let phrase = std::str::from_utf8(&words).map_err(|_| "invalid protected recovery phrase")?;
    let recovered = Zeroizing::new(
        lastdb_node::cloud::seed_from_phrase(phrase)
            .map_err(|_| "invalid protected recovery phrase")?,
    );
    if recovered.as_ref() != seed.as_ref() {
        return Err("protected recovery phrase does not match native identity".into());
    }
    Ok(seed)
}

fn refuse_daemon(home: &Path) -> Result<(), String> {
    let sock = lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"));
    if std::os::unix::net::UnixStream::connect(sock).is_ok() {
        return Err("stop the restored home's daemon before account reconnect".into());
    }
    Ok(())
}
