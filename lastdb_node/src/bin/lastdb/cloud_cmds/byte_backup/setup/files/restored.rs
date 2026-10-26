//! Explicit account reconnect for a completed native restore. No source write occurs.

use super::*;
use fold_db::crypto::E2eKeys;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::disk_pack::{run_restore, validate_restored_node, RestoredNodeGuard};
use fold_db::storage::error::StorageResult;
use std::path::PathBuf;

#[path = "restored_receipt.rs"]
mod receipt;

pub(super) struct Validation {
    guard: NativeGuard,
    identity: File,
    seed: zeroize::Zeroizing<[u8; 32]>,
    home: PathBuf,
    receipt: receipt::Proof,
    pub(super) account: String,
}

enum NativeGuard {
    Pack(RestoredNodeGuard),
    Run(Box<run_restore::RunNodeGuard>),
}

impl NativeGuard {
    fn open(data: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        match run_restore::validate_restored_node(data, key)? {
            Some(guard) => Ok(Self::Run(Box::new(guard))),
            None => validate_restored_node(data, key).map(Self::Pack),
        }
    }

    fn check_binding(&self) -> StorageResult<()> {
        match self {
            Self::Pack(guard) => guard.check_binding(),
            Self::Run(guard) => guard.check_binding(),
        }
    }
}

impl PreparedHome {
    pub(crate) fn open_restored(home: &Path) -> Result<Self, String> {
        if !home.is_absolute() {
            return Err("restored reconnect requires an explicit absolute home".into());
        }
        refuse_protected_home(home)?;
        let parent =
            recovery_files::open_directory(home.parent().ok_or("restored home has no parent")?)?;
        recovery_files::require_owned_directory(&parent)?;
        let directory = recovery_files::open_directory(home)?;
        require_private_directory(&directory)?;
        refuse_daemon(home)?;
        let receipt = receipt::Proof::open(&directory, home)?;
        let identity = File::from(
            fs::openat(
                &directory,
                IDENTITY_KEY_FILE,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| "cannot open restored account identity")?,
        );
        require_private_file(&identity)?;
        identity
            .try_lock()
            .map_err(|_| "restored identity is already owned")?;
        let seed = identity_seed(&directory)?;
        let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| "invalid restored account identity")?;
        let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())?;
        let keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|_| "cannot derive restored native account key")?;
        let content = zeroize::Zeroizing::new(keys.encryption_key());
        let _data = recovery_files::open_directory(&home.join("data"))?;
        let guard = NativeGuard::open(&home.join("data"), &content).map_err(|_| {
            "restored native home is active, unsupported, or belongs to another identity"
        })?;
        let lock = enrollment_lock(&directory)?;
        let prepared = Self {
            parent,
            name: home
                .file_name()
                .ok_or("restored home has no name")?
                .to_os_string(),
            directory,
            _lock: lock,
            restored: Some(Validation {
                guard,
                identity,
                seed,
                home: home.to_path_buf(),
                receipt,
                account,
            }),
        };
        prepared.verify_entries()?;
        if !recovery_files::entry_present(&prepared.directory, CLOUD_RESUME_REQUIRED_FILE)? {
            prepared.write_new(CLOUD_RESUME_REQUIRED_FILE, b"")?;
        }
        Ok(prepared)
    }
}

impl Validation {
    pub(super) fn check(&self, prepared: &PreparedHome) -> Result<(), String> {
        refuse_daemon(&self.home)?;
        self.guard
            .check_binding()
            .map_err(|_| "restored native home changed during reconnect")?;
        self.seed(prepared)?;
        if recovery_files::entry_present(&prepared.directory, "cloud_sync.json")? {
            return Err("restored reconnect requires legacy Cloud Sync Off".into());
        }
        if recovery_files::entry_present(&prepared.directory, CLOUD_RESUME_REQUIRED_FILE)? {
            recovery_files::read_private_file(&prepared.directory, CLOUD_RESUME_REQUIRED_FILE, 0)?;
        }
        verify_automatic_off(&self.home)?;
        self.receipt.check(&prepared.directory)
    }

    pub(super) fn seed(
        &self,
        prepared: &PreparedHome,
    ) -> Result<zeroize::Zeroizing<[u8; 32]>, String> {
        let held = fs::fstat(&self.identity).map_err(|_| "cannot inspect restored identity")?;
        let current = fs::statat(
            &prepared.directory,
            IDENTITY_KEY_FILE,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| "cannot inspect selected restored identity")?;
        if held.st_dev != current.st_dev || held.st_ino != current.st_ino {
            return Err("restored account identity changed during reconnect".into());
        }
        let seed = identity_seed(&prepared.directory)?;
        if seed.as_ref() != self.seed.as_ref() {
            return Err("restored account identity changed during reconnect".into());
        }
        Ok(seed)
    }
}

fn enrollment_lock(directory: &File) -> Result<File, String> {
    let file = File::from(
        fs::openat(
            directory,
            LOCK_FILE,
            OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::from_raw_mode(0o600),
        )
        .map_err(|_| "cannot open restored enrollment lock")?,
    );
    require_private_file(&file)?;
    file.try_lock()
        .map_err(|_| "native enrollment is already active for this home")?;
    Ok(file)
}

fn identity_seed(directory: &File) -> Result<zeroize::Zeroizing<[u8; 32]>, String> {
    let bytes = zeroize::Zeroizing::new(recovery_files::read_private_file(
        directory,
        IDENTITY_KEY_FILE,
        32,
    )?);
    let seed = bytes
        .as_slice()
        .try_into()
        .map_err(|_| "restored identity must contain exactly 32 bytes")?;
    Ok(zeroize::Zeroizing::new(seed))
}

fn verify_automatic_off(home: &Path) -> Result<(), String> {
    let data = recovery_files::open_directory(&home.join("data"))?;
    if !recovery_files::entry_present(&data, "native-cloud-backup-v1")? {
        return Ok(());
    }
    let manager = recovery_files::open_directory(&home.join("data/native-cloud-backup-v1"))?;
    recovery_files::require_owned_directory(&manager)?;
    if !recovery_files::entry_present(&manager, "automatic.json")? {
        return Ok(());
    }
    let bytes = recovery_files::read_private_file(&manager, "automatic.json", 8192)?;
    let consent: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "invalid saved automatic backup consent")?;
    if consent["format"].as_u64() != Some(1) || consent["enabled"].as_bool() != Some(false) {
        return Err("automatic backup must already be off before restored reconnect".into());
    }
    Ok(())
}

fn refuse_protected_home(home: &Path) -> Result<(), String> {
    let user = std::env::var_os("HOME").ok_or("HOME is unset")?;
    if home.starts_with(PathBuf::from(user).join(".primary-lastdb-archive-20261014")) {
        return Err("restored reconnect cannot use the primary archive".into());
    }
    lastdb_node::offline_home::refuse_primary(home)
}

fn refuse_daemon(home: &Path) -> Result<(), String> {
    if lastdb_node::crash_attribution::live_daemon_pid_alive(home) {
        return Err("stop the restored home's daemon before account reconnect".into());
    }
    for name in ["folddb.sock", "folddb-full.sock"] {
        if std::os::unix::net::UnixStream::connect(home.join("data").join(name)).is_ok() {
            return Err("stop the restored home's daemon before account reconnect".into());
        }
    }
    Ok(())
}
