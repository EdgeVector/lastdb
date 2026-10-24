use super::{keys::RecoveryKeys, RestoreError, RestoreSource, Result};
use cloud_backup_protocol::publication::AcceptedBackup;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

// Leave space inside the one-GiB spool cap for directory metadata, selection,
// bounded atomic control temporaries, and the eight-MiB native output temporary.
const TRACKED_SPOOL_MAX: u64 = cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES
    - lastdb_storage_policy::backup_decode::RESTORE_SPOOL_RESERVE_BYTES;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreReport {
    pub commit_sha256: String,
    pub native_files: u64,
    pub native_bytes: u64,
    pub verified_atoms: u64,
    pub file_dependencies_verified: bool,
    #[serde(default)]
    pub verified_external_files: u64,
    pub destination: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    pub version: u8,
    pub session_id: uuid::Uuid,
    pub accepted: AcceptedBackup,
    pub destination: PathBuf,
}

/// A durable selection and an exclusive lock over caller-owned scratch data.
/// Drop retains incomplete work for an explicit resume with the recovery phrase.
pub struct RestoreSession {
    pub(super) root: PathBuf,
    pub(super) selection: Selection,
    _lock: File,
    spool_bytes: AtomicU64,
}

impl RestoreSession {
    pub fn begin(destination: &Path, accepted: AcceptedBackup) -> Result<Self> {
        accepted.validate()?;
        crate::offline_home::refuse_primary(destination)
            .map_err(|_| RestoreError::Invalid("destination"))?;
        let parent = destination
            .parent()
            .ok_or(RestoreError::Invalid("destination parent"))?
            .canonicalize()?;
        let name = destination
            .file_name()
            .ok_or(RestoreError::Invalid("destination name"))?;
        let destination = parent.join(name);
        match fs::symlink_metadata(&destination) {
            Ok(_) => return Err(RestoreError::Invalid("destination exists")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let id = uuid::Uuid::new_v4();
        let root = parent.join(format!(".native-cloud-restore-{id}"));
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let lock = owner_file(&root.join("owner.lock"), true)?;
        lock.try_lock()
            .map_err(|_| RestoreError::Invalid("session already owned"))?;
        let selection = Selection {
            version: 1,
            session_id: id,
            accepted,
            destination,
        };
        atomic_json(&root.join("selection.json"), &selection)?;
        scaffold(&root)?;
        File::open(&root)?.sync_all()?;
        File::open(&parent)?.sync_all()?;
        Ok(Self {
            root,
            selection,
            _lock: lock,
            spool_bytes: AtomicU64::new(0),
        })
    }

    pub fn resume(root: &Path) -> Result<Self> {
        if !fs::symlink_metadata(root)?.is_dir() {
            return Err(RestoreError::Invalid("session directory"));
        }
        crate::offline_home::refuse_primary(root)
            .map_err(|_| RestoreError::Invalid("session directory"))?;
        let root = root.canonicalize()?;
        let lock = owner_file(&root.join("owner.lock"), false)?;
        lock.try_lock()
            .map_err(|_| RestoreError::Invalid("session already owned"))?;
        let selection: Selection = read_json(&root.join("selection.json"), 64 * 1024)?;
        selection.accepted.validate()?;
        if selection.version != 1
            || selection.session_id.is_nil()
            || root.file_name().and_then(|name| name.to_str())
                != Some(&format!(".native-cloud-restore-{}", selection.session_id))
            || selection.destination.parent() != root.parent()
        {
            return Err(RestoreError::Invalid("saved selection"));
        }
        if !selection.destination.try_exists()? {
            scaffold(&root)?;
        }
        let session = Self {
            root,
            selection,
            _lock: lock,
            spool_bytes: AtomicU64::new(0),
        };
        session.check_spool()?;
        Ok(session)
    }

    pub fn path(&self) -> &Path {
        &self.root
    }
    pub fn accepted(&self) -> &AcceptedBackup {
        &self.selection.accepted
    }

    pub(super) fn verified_context(&self) -> super::VerifiedNativeContext<'_> {
        super::VerifiedNativeContext {
            root: &self.root,
            destination: &self.selection.destination,
            session_id: self.selection.session_id,
            commit_sha256: &self.accepted().commit_sha256,
            native_root: &self.accepted().native_root,
            receipt_name: "native-cloud-restore-receipt.json",
            receipt_version: 1,
        }
    }

    /// Import one frozen accepted backup. No head discovery occurs on resume.
    pub async fn restore(
        &mut self,
        source: &dyn RestoreSource,
        keys: &RecoveryKeys,
    ) -> Result<RestoreReport> {
        let _admission = fold_db::storage::disk_pack::NativeCloudWorkGuard::acquire()
            .map_err(|_| RestoreError::Limit("active native cloud operation"))?;
        keys.validate_for(self.accepted())?;
        // Reconcile partial writes after any prior failed call, including disk errors.
        self.check_spool()?;
        if let Some(report) = super::install::published(self)? {
            return Ok(report);
        }
        let authoritative = source.accepted(&self.accepted().job).await?;
        authoritative.validate()?;
        if authoritative != *self.accepted() {
            return Err(RestoreError::Invalid("authoritative accepted selection"));
        }
        let inventory = super::closure::restore(self, source, keys).await?;
        let atoms = super::records::verify(self, &keys.content, inventory.dependencies)?;
        super::records::verify_metadata(self, &keys.content)?;
        super::install::finish(self, keys, inventory, atoms)
    }

    pub(super) fn physical(&self) -> PathBuf {
        self.root.join("home/data/disk-pack")
    }
    pub(super) fn reserve_spool(&self, bytes: u64) -> Result<()> {
        let allocated = bytes
            .div_ceil(4096)
            .checked_mul(4096)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        if self
            .spool_bytes
            .load(Ordering::Relaxed)
            .checked_add(allocated)
            .is_none_or(|total| total > TRACKED_SPOOL_MAX)
        {
            return Err(RestoreError::Limit("spool bytes"));
        }
        Ok(())
    }
    pub(super) fn add_spool_file(&self, path: &Path) -> Result<()> {
        let allocated = fs::symlink_metadata(path)?
            .blocks()
            .checked_mul(512)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        let total = self
            .spool_bytes
            .load(Ordering::Relaxed)
            .checked_add(allocated)
            .ok_or(RestoreError::Limit("spool bytes"))?;
        if total > TRACKED_SPOOL_MAX {
            return Err(RestoreError::Limit("spool bytes"));
        }
        self.spool_bytes.store(total, Ordering::Relaxed);
        Ok(())
    }
    pub(super) fn remove_spool_file(&self, path: &Path) -> Result<()> {
        match fs::symlink_metadata(path) {
            Ok(meta) if meta.is_file() => {
                fs::remove_file(path)?;
                self.spool_bytes.store(
                    self.spool_bytes.load(Ordering::Relaxed).saturating_sub(
                        meta.blocks()
                            .checked_mul(512)
                            .ok_or(RestoreError::Limit("spool bytes"))?,
                    ),
                    Ordering::Relaxed,
                );
                Ok(())
            }
            Ok(_) => Err(RestoreError::Invalid("spool file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    pub(super) fn check_spool(&self) -> Result<()> {
        let mut bytes = 0u64;
        for folder in ["cache", "entries"] {
            let path = self.root.join(folder);
            if !fs::symlink_metadata(&path)?.is_dir() {
                return Err(RestoreError::Invalid("spool directory"));
            }
            let mut count = 0usize;
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let meta = fs::symlink_metadata(entry.path())?;
                if !meta.is_file() {
                    return Err(RestoreError::Invalid("spool member"));
                }
                count += 1;
                if count
                    > cloud_backup_protocol::limits::NATIVE_FILES_MAX
                        + 2 * cloud_backup_protocol::limits::MANIFEST_PAGES_MAX
                        + 8
                {
                    return Err(RestoreError::Limit("spool entries"));
                }
                bytes = bytes
                    .checked_add(
                        meta.blocks()
                            .checked_mul(512)
                            .ok_or(RestoreError::Limit("spool bytes"))?,
                    )
                    .ok_or(RestoreError::Limit("spool bytes"))?;
                if bytes > TRACKED_SPOOL_MAX {
                    return Err(RestoreError::Limit("spool bytes"));
                }
            }
        }
        self.spool_bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }
}

fn scaffold(root: &Path) -> Result<()> {
    for name in [
        "cache",
        "entries",
        "home",
        "home/data",
        "home/data/disk-pack",
    ] {
        let path = root.join(name);
        match fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if !fs::symlink_metadata(path)?.is_dir() {
            return Err(RestoreError::Invalid("session member"));
        }
    }
    Ok(())
}

pub(super) fn owner_file(path: &Path, new: bool) -> Result<File> {
    if !new && !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("owned file"));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).mode(0o600);
    if new {
        options.create_new(true);
    }
    Ok(options.open(path)?)
}
pub(super) fn read_json<T: serde::de::DeserializeOwned>(path: &Path, max: u64) -> Result<T> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(RestoreError::Invalid("metadata file"));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err(RestoreError::Limit("metadata bytes"));
    }
    serde_json::from_slice(&bytes).map_err(|_| RestoreError::Invalid("metadata JSON"))
}
pub(super) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = cloud_backup_protocol::canonical_bytes(value)?;
    if bytes.len() > 256 * 1024 {
        return Err(RestoreError::Limit("saved metadata"));
    }
    let pending = path.with_extension("pending");
    if pending.try_exists()? && !fs::symlink_metadata(&pending)?.is_file() {
        return Err(RestoreError::Invalid("metadata temporary"));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    File::open(
        path.parent()
            .ok_or(RestoreError::Invalid("metadata parent"))?,
    )?
    .sync_all()?;
    Ok(())
}
