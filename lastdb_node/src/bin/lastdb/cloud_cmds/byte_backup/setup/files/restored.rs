//! Explicit account reconnect for a completed native restore. No source write occurs.

use super::*;
use cloud_backup_protocol::{bytes_sha256, Digest};
use fold_db::crypto::E2eKeys;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::disk_pack::run_restore;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

const RECEIPT: &str = "byte-cloud-restore-receipt.json";
const RECEIPT_VERSION: u8 = 2;
const RECEIPT_MAX_BYTES: u64 = 65_536;

pub(super) struct Validation {
    guard: run_restore::RunNodeGuard,
    identity: File,
    seed: zeroize::Zeroizing<[u8; 32]>,
    home: PathBuf,
    receipt: ReceiptProof,
    pub(super) account: String,
}

struct ReceiptProof {
    file: File,
    digest: Digest,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u8,
    session_id: uuid::Uuid,
    report: lastdb_node::byte_cloud_restore::RestoreReport,
}

impl ReceiptProof {
    fn open(directory: &File, home: &Path) -> Result<Self, String> {
        require_receipt_selected(directory)?;
        let file = File::from(
            fs::openat(
                directory,
                RECEIPT,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| "cannot open completed restore receipt")?,
        );
        let bytes = receipt_contents(&file)?;
        validate_receipt(&bytes, home)?;
        let proof = Self {
            file,
            digest: bytes_sha256(&bytes),
        };
        proof.binding(directory)?;
        Ok(proof)
    }

    fn check(&self, directory: &File) -> Result<(), String> {
        self.binding(directory)?;
        if bytes_sha256(&receipt_contents(&self.file)?) != self.digest {
            return Err("restore receipt changed during reconnect".into());
        }
        self.binding(directory)
    }

    fn binding(&self, directory: &File) -> Result<(), String> {
        require_receipt_selected(directory)?;
        let held = fs::fstat(&self.file).map_err(|_| "cannot inspect held restore receipt")?;
        let current = fs::statat(directory, RECEIPT, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| "cannot inspect selected restore receipt")?;
        if held.st_dev != current.st_dev || held.st_ino != current.st_ino {
            return Err("restore receipt identity changed during reconnect".into());
        }
        Ok(())
    }
}

fn require_receipt_selected(directory: &File) -> Result<(), String> {
    if recovery_files::entry_present(directory, RECEIPT)? {
        Ok(())
    } else {
        Err("restored reconnect requires the completed restore receipt".into())
    }
}

fn validate_receipt(bytes: &[u8], home: &Path) -> Result<(), String> {
    let receipt: Receipt =
        serde_json::from_slice(bytes).map_err(|_| "invalid native restore receipt")?;
    if receipt.version != RECEIPT_VERSION
        || receipt.session_id.is_nil()
        || receipt.report.destination != home
        || receipt.report.native_files == 0
        || receipt.report.native_bytes == 0
        || Digest::try_from(receipt.report.commit_sha256).is_err()
    {
        return Err("native restore provenance does not match this home".into());
    }
    Ok(())
}

fn receipt_contents(file: &File) -> Result<Vec<u8>, String> {
    require_private_file(file)?;
    let before = file
        .metadata()
        .map_err(|_| "cannot inspect completed restore receipt")?;
    if before.len() > RECEIPT_MAX_BYTES {
        return Err("completed restore receipt exceeds its byte limit".into());
    }
    let mut bytes = vec![0; before.len() as usize];
    file.read_exact_at(&mut bytes, 0)
        .map_err(|_| "cannot read completed restore receipt")?;
    let mut extra = [0; 1];
    if file
        .read_at(&mut extra, before.len())
        .map_err(|_| "cannot inspect completed restore receipt length")?
        != 0
    {
        return Err("completed restore receipt changed during read".into());
    }
    require_private_file(file)?;
    let after = file
        .metadata()
        .map_err(|_| "cannot inspect completed restore receipt")?;
    if before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
    {
        return Err("completed restore receipt changed during read".into());
    }
    Ok(bytes)
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
        let receipt = ReceiptProof::open(&directory, home)?;
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
        let account = lastdb_node::identity::user_hash_from_pubkey(&pair.public_key_base64())?;
        let keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|_| "cannot derive restored native account key")?;
        let content = zeroize::Zeroizing::new(keys.encryption_key());
        let _data = recovery_files::open_directory(&home.join("data"))?;
        let guard =
            run_restore::validate_restored_node(&home.join("data"), &content).map_err(|_| {
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

fn refuse_protected_home(home: &Path) -> Result<(), String> {
    let user = std::env::var_os("HOME").ok_or("HOME is unset")?;
    if home.starts_with(PathBuf::from(user).join(".primary-lastdb-archive-20261014")) {
        return Err("restored reconnect cannot use the primary archive".into());
    }
    lastdb_node::offline_home::refuse_primary(home)
}

fn refuse_daemon(home: &Path) -> Result<(), String> {
    if std::os::unix::net::UnixStream::connect(lastdb_node::lastdb_uds::uds::socket_path(
        &home.join("data"),
    ))
    .is_ok()
    {
        return Err("stop the restored home's daemon before account reconnect".into());
    }
    Ok(())
}
