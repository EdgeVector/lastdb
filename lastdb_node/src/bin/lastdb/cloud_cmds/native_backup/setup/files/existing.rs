//! Live existing-home enrollment. No native reader, writer, or identity replacement.

use super::*;
use fold_db::security::Ed25519KeyPair;
use std::path::PathBuf;

#[path = "existing/owner.rs"]
mod owner;

pub(super) struct Validation {
    identity: File,
    seed: zeroize::Zeroizing<[u8; 32]>,
    data: File,
    owner: owner::Owner,
    home: PathBuf,
    pub(super) account: String,
}

impl PreparedHome {
    pub(crate) fn open_existing(home: &Path) -> Result<Self, String> {
        let user = std::env::var_os("HOME").ok_or("HOME is unset")?;
        if home.starts_with(PathBuf::from(user).join(".primary-lastdb-archive-20261014")) {
            return Err("existing enrollment cannot use the primary archive".into());
        }
        let parent = recovery_files::open_directory(home.parent().ok_or("home has no parent")?)?;
        recovery_files::require_owned_directory(&parent)?;
        let directory = recovery_files::open_directory(home)?;
        require_private_directory(&directory)?;
        let data = recovery_files::open_directory(&home.join("data"))?;
        recovery_files::require_owned_directory(&data)?;
        require_off(&directory, home)?;
        let identity = File::from(
            fs::openat(
                &directory,
                IDENTITY_KEY_FILE,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| "cannot open existing account identity")?,
        );
        require_private_file(&identity)?;
        identity
            .try_lock()
            .map_err(|_| "existing account identity is already owned")?;
        let seed = restored::identity_seed(&directory)?;
        let pair = Ed25519KeyPair::from_secret_key(seed.as_ref())
            .map_err(|_| "invalid existing account identity")?;
        let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())?;
        let owner = owner::Owner::open(home, &account)?;
        let lock = restored::enrollment_lock(&directory)?;
        let prepared = Self {
            parent,
            name: home.file_name().ok_or("home has no name")?.to_os_string(),
            directory,
            _lock: lock,
            restored: None,
            existing: Some(Validation {
                identity,
                seed,
                data,
                owner,
                home: home.into(),
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
        self.seed(prepared)?;
        require_off(&prepared.directory, &self.home)?;
        let held = fs::fstat(&self.data).map_err(|_| "cannot inspect held data directory")?;
        let actual = fs::statat(&prepared.directory, "data", AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| "cannot inspect existing data directory")?;
        if held.st_dev != actual.st_dev || held.st_ino != actual.st_ino {
            return Err("existing data directory changed during enrollment".into());
        }
        self.owner.check_socket()
    }

    pub(super) fn seed(
        &self,
        prepared: &PreparedHome,
    ) -> Result<zeroize::Zeroizing<[u8; 32]>, String> {
        let held = fs::fstat(&self.identity).map_err(|_| "cannot inspect held identity")?;
        let actual = fs::statat(
            &prepared.directory,
            IDENTITY_KEY_FILE,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| "cannot inspect existing identity")?;
        let seed = restored::identity_seed(&prepared.directory)?;
        if held.st_dev != actual.st_dev
            || held.st_ino != actual.st_ino
            || seed.as_ref() != self.seed.as_ref()
        {
            return Err("existing identity changed during enrollment".into());
        }
        Ok(seed)
    }

    pub(super) fn verify_owner(&self) -> Result<(), String> {
        self.owner.verify()
    }
}

fn require_off(directory: &File, home: &Path) -> Result<(), String> {
    for name in [
        "cloud_sync.json",
        lastdb_node::cloud::CLOUD_RESUME_REQUESTED_FILE,
    ] {
        if recovery_files::entry_present(directory, name)? {
            return Err("existing enrollment requires legacy Cloud Sync Off".into());
        }
    }
    if recovery_files::entry_present(directory, CLOUD_RESUME_REQUIRED_FILE)? {
        recovery_files::read_private_file(directory, CLOUD_RESUME_REQUIRED_FILE, 0)?;
    }
    restored::verify_automatic_off(home)
}
