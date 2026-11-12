//! Descriptor-bound fresh-home enrollment and restored-home reconnect.
use super::{block_on_cloud, CLOUD_CREDENTIALS_FILE, IDENTITY_KEY_FILE, PHRASE_FILE};
use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::{bytes_sha256, AccountId, Digest};
use fold_db::{crypto::E2eKeys, security::Ed25519KeyPair, storage::disk_pack::run_restore};
use lastdb_node::byte_cloud_restore::{verify_restore_receipt, RESTORE_RECEIPT_NAME};
use lastdb_node::cloud_backup_client::{byte_backup::ByteBackupClient, AuthClient};
use lastdb_node::offline_home::{
    dir_entry_names, entry_present, open_directory, open_private_at, read_private_file,
    read_private_file_bound, require_mode_0700_directory, require_owned_directory, same_inode,
};
use rustix::fs::{self, Mode, OFlags};
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

const DEVICE_FILE: &str = ".native_backup_device_id";
const LOCK_FILE: &str = ".native_backup_enrollment.lock";
const PENDING_FILE: &str = ".native_backup_credentials.pending";
const FILE_ERR: &str = "native enrollment file must be a private regular owner file";
const MODE_ERR: &str = "native enrollment directory must be owner-only with mode0700";
const ALLOWED: [&str; 6] = [
    IDENTITY_KEY_FILE,
    PHRASE_FILE,
    DEVICE_FILE,
    LOCK_FILE,
    PENDING_FILE,
    CLOUD_CREDENTIALS_FILE,
];

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
    fn lock_home(home: &Path, restored: Option<RestoredValidation>) -> Result<Self, String> {
        let parent = open_directory(home.parent().ok_or("native enrollment needs a home")?)?;
        require_owned_directory(&parent)?;
        let directory = open_directory(home)?;
        require_mode_0700_directory(&directory, MODE_ERR)?;
        let name = home
            .file_name()
            .ok_or("native enrollment needs an explicit home name")?
            .to_os_string();
        let flags = OFlags::RDWR | OFlags::CREATE | OFlags::NONBLOCK;
        let _lock = open_private_at(
            &directory,
            LOCK_FILE,
            flags,
            Mode::from_raw_mode(0o600),
            FILE_ERR,
        )?;
        _lock
            .try_lock()
            .map_err(|_| "native enrollment is already active for this home")?;
        let prepared = Self {
            parent,
            name,
            directory,
            _lock,
            restored,
        };
        prepared.verify_entries().map(|()| prepared)
    }

    pub(super) fn open(home: &Path, restored: bool) -> Result<Self, String> {
        if !restored {
            let prepared = Self::lock_home(home, None)?;
            return prepared.seed().map(|_| prepared);
        }
        if !home.is_absolute() {
            return Err("restored reconnect requires an explicit absolute home".into());
        }
        let user = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is unset")?);
        if home.starts_with(user.join(".primary-lastdb-archive-20261014")) {
            return Err("restored reconnect cannot use the primary archive".into());
        }
        lastdb_node::offline_home::refuse_primary(home)?;
        let directory = open_directory(home)?;
        require_mode_0700_directory(&directory, MODE_ERR)?;
        refuse_daemon(home)?;
        if !entry_present(&directory, RESTORE_RECEIPT_NAME)? {
            return Err("restored reconnect requires the completed restore receipt".into());
        }
        let receipt = open_ro(&directory, RESTORE_RECEIPT_NAME)?;
        let receipt_bytes = read_receipt(&directory, &receipt)?;
        verify_restore_receipt(&receipt_bytes, home)
            .map_err(|_| "native restore provenance does not match this home")?;
        let identity = open_ro(&directory, IDENTITY_KEY_FILE)?;
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
        open_directory(&data)?;
        let guard = run_restore::validate_restored_node(&data, &content).map_err(|_| {
            "restored native home is active, unsupported, or belongs to another identity"
        })?;
        let receipt_digest = bytes_sha256(&receipt_bytes);
        Self::lock_home(
            home,
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

    fn check_inode(&self) -> Result<(), String> {
        same_inode(&self.parent, &self.name, &self.directory)
            .then_some(())
            .ok_or_else(|| "selected native enrollment home changed".into())
    }

    fn verify_entries(&self) -> Result<(), String> {
        self.check_inode()?;
        if let Some(r) = &self.restored {
            return r.check(&self.directory);
        }
        let entries = dir_entry_names(&self.directory, "cannot inspect native enrollment home")?;
        if entries.len() > ALLOWED.len()
            || entries
                .iter()
                .any(|n| !ALLOWED.iter().any(|a| a.as_bytes() == n))
        {
            return Err("native enrollment home contains a store or an unexpected entry".into());
        }
        Ok(())
    }

    pub(super) fn seed(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        self.check_inode()?;
        match &self.restored {
            Some(r) => r.seed(&self.directory),
            None => read_fresh_seed(&self.directory),
        }
    }

    pub(super) fn device_id(&self) -> Result<String, String> {
        self.verify_entries()?;
        if !entry_present(&self.directory, DEVICE_FILE)? {
            self.write_new(DEVICE_FILE, uuid::Uuid::new_v4().to_string().as_bytes())?;
        }
        let bytes = read_private_file(&self.directory, DEVICE_FILE, 36)?;
        let text = String::from_utf8(bytes).map_err(|_| "invalid saved native device ID")?;
        let id = uuid::Uuid::parse_str(&text).map_err(|_| "invalid saved native device ID")?;
        if id.get_version() != Some(uuid::Version::Random) || id.to_string() != text {
            return Err("native device ID must be a canonical UUIDv4".into());
        }
        sync_ro(&self.directory, DEVICE_FILE)?;
        self.sync().map(|()| text)
    }

    pub(super) fn finish_saved_credentials(&self, api_url: &str) -> Result<bool, String> {
        self.verify_entries()?;
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
        let creds = lastdb_node::cloud::SavedCredentials::parse(&bytes)
            .map_err(|_| "invalid saved native credentials; registration may have committed")?;
        if creds.api_url != api_url || creds.api_key.is_empty() {
            return Err("saved native credentials do not match the compiled DEV endpoint".into());
        }
        if let Some(restored) = &self.restored {
            validate_restored_credentials(api_url, &creds.api_key, &restored.account)?;
            self.verify_entries()?;
        }
        sync_ro(&self.directory, name)?;
        if name == PENDING_FILE {
            self.publish()?;
        } else {
            self.sync()?;
        }
        Ok(true)
    }

    pub(super) fn save_credentials(&self, bytes: &[u8]) -> Result<(), String> {
        self.write_new(PENDING_FILE, bytes)?;
        self.publish()
    }

    fn publish(&self) -> Result<(), String> {
        self.verify_entries()?;
        if entry_present(&self.directory, CLOUD_CREDENTIALS_FILE)? {
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
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL;
        let mut file = open_private_at(
            &self.directory,
            name,
            flags,
            Mode::from_raw_mode(0o600),
            FILE_ERR,
        )?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| "cannot write protected native enrollment file")?;
        self.sync()
    }

    fn sync(&self) -> Result<(), String> {
        self.verify_entries()?;
        self.directory
            .sync_all()
            .and_then(|()| self.parent.sync_all())
            .map_err(|_| "cannot sync native enrollment home".into())
    }
}

impl RestoredValidation {
    fn check(&self, directory: &File) -> Result<(), String> {
        refuse_daemon(&self.home)?;
        self.guard
            .check_binding()
            .map_err(|_| "restored native home changed during reconnect")?;
        self.seed(directory)?;
        (bytes_sha256(&read_receipt(directory, &self.receipt)?) == self.receipt_digest)
            .then_some(())
            .ok_or_else(|| "restore receipt changed during reconnect".into())
    }

    fn seed(&self, directory: &File) -> Result<Zeroizing<[u8; 32]>, String> {
        let seed = identity_seed(directory, Some(&self.identity))?;
        (seed.as_ref() == self.seed.as_ref())
            .then_some(seed)
            .ok_or_else(|| "restored account identity changed during reconnect".into())
    }
}

fn validate_restored_credentials(
    api_url: &str,
    api_key: &str,
    account: &str,
) -> Result<(), String> {
    let client = ByteBackupClient::from_auth(AuthClient::new(api_url.into(), api_key.into()))
        .map_err(|_| "invalid restored native account endpoint")?;
    let account_id =
        AccountId::try_from(account.to_owned()).map_err(|_| "invalid restored account identity")?;
    let req = ContextReadRequest {
        version: Version::V2,
        account_id,
        required_destinations: Vec::new(),
    };
    block_on_cloud("restored credential validation", async move {
        client
            .context(&req)
            .await
            .map(|_| ())
            .map_err(|_| "saved native credentials do not authorize the restored account".into())
    })
}

fn read_receipt(dir: &File, file: &File) -> Result<Vec<u8>, String> {
    read_private_file_bound(dir, RESTORE_RECEIPT_NAME, 65_536, Some(file))
        .map_err(|_| "restored reconnect requires the completed restore receipt".into())
}

fn open_ro(dir: &File, name: &str) -> Result<File, String> {
    open_private_at(
        dir,
        name,
        OFlags::RDONLY | OFlags::NONBLOCK,
        Mode::empty(),
        FILE_ERR,
    )
}

fn sync_ro(dir: &File, name: &str) -> Result<(), String> {
    open_ro(dir, name)?
        .sync_all()
        .map_err(|_| "cannot sync saved native enrollment file".into())
}

fn identity_seed(directory: &File, expected: Option<&File>) -> Result<Zeroizing<[u8; 32]>, String> {
    read_private_file_bound(directory, IDENTITY_KEY_FILE, 32, expected)?
        .as_slice()
        .try_into()
        .map(Zeroizing::new)
        .map_err(|_| "native identity must contain 32 bytes".into())
}

fn read_fresh_seed(directory: &File) -> Result<Zeroizing<[u8; 32]>, String> {
    let seed = identity_seed(directory, None)?;
    let words = Zeroizing::new(read_private_file(directory, PHRASE_FILE, 4096)?);
    let phrase = std::str::from_utf8(&words).map_err(|_| "invalid protected recovery phrase")?;
    let recovered = Zeroizing::new(
        lastdb_node::cloud::seed_from_phrase(phrase)
            .map_err(|_| "invalid protected recovery phrase")?,
    );
    (recovered.as_ref() == seed.as_ref())
        .then_some(seed)
        .ok_or_else(|| "protected recovery phrase does not match native identity".into())
}

fn refuse_daemon(home: &Path) -> Result<(), String> {
    let sock = lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"));
    std::os::unix::net::UnixStream::connect(sock)
        .is_err()
        .then_some(())
        .ok_or_else(|| "stop the restored home's daemon before account reconnect".into())
}
