//! Prototype identity and account key stay outside replaceable native generations.

use super::*;
use rand::RngCore;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use zeroize::Zeroizing;

const MARKER: &[u8] = b"lastdb-peer-sync-spike-v1\n";

pub(super) struct RootControl {
    pub(super) root: PathBuf,
    pub(super) writer: Writer,
    pub(super) key: Zeroizing<[u8; 32]>,
    pub(super) failed: AtomicBool,
    created: bool,
    _lock: File,
}

pub(super) async fn open_replica(path: &Path, key: &[u8; 32]) -> StorageResult<Replica> {
    let control = Arc::new(RootControl::open(path, key)?);
    let (generation, fresh) = match control.active()? {
        Some(generation) => (generation, false),
        None if control.created => (control.new_generation()?, true),
        None => return Err(invalid("existing prototype root lacks its ACTIVE selector")),
    };
    let path = control.generation(&generation)?;
    if !fresh {
        if crate::storage::disk_pack::run_node::creation_pending(&path)? {
            return Err(invalid("selected prototype generation is incomplete"));
        }
        crate::storage::disk_pack::run_admission::admit(&path, key)?;
    }
    let mut replica = Replica::open_generation(&path, key).await?;
    if fresh {
        replica
            .kv
            .put(
                super::keys::STATE_KEY,
                encode(&super::model::State::default())?,
            )
            .await?;
        replica.kv.flush().await?;
        control.select(&generation)?;
    } else {
        super::snapshot::required_state(&replica).await?;
        // Complete a visible selector whose prior directory barrier was uncertain.
        File::open(&control.root)?.sync_all()?;
    }
    replica.control = Some(control);
    Ok(replica)
}

impl Replica {
    /// Open only an existing private prototype root with its saved account key.
    pub async fn open_saved(path: &Path) -> StorageResult<Self> {
        private_directory(path)?;
        if read_private(&path.join("FORMAT"), MARKER.len())? != MARKER {
            return Err(invalid("this is not a peer-sync prototype root"));
        }
        let bytes = Zeroizing::new(read_private(&path.join("ACCOUNT_KEY"), 32)?);
        let key: Zeroizing<[u8; 32]> = Zeroizing::new(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| invalid("invalid prototype account key"))?,
        );
        Self::open(path, &key).await
    }

    /// Return the original local signer; snapshots never replace this identity.
    pub fn writer(&self) -> StorageResult<Writer> {
        self.control
            .as_ref()
            .map(|control| control.writer.clone())
            .ok_or_else(|| invalid("a snapshot generation has no local writer identity"))
    }
}

impl RootControl {
    fn open(path: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        let mut created = false;
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create(path, key)?;
                created = true;
            }
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        private_directory(path)?;
        let root = path.canonicalize()?;
        let lock = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(root.join("LOCK"))?;
        private_file(&lock.metadata()?, 0)?;
        lock.try_lock()
            .map_err(|_| invalid("another command owns this prototype root"))?;
        if read_private(&root.join("FORMAT"), MARKER.len())? != MARKER {
            return Err(invalid("this is not a peer-sync prototype root"));
        }
        let saved = Zeroizing::new(read_private(&root.join("ACCOUNT_KEY"), 32)?);
        if saved.as_slice() != key {
            return Err(invalid("prototype account key differs"));
        }
        let signer = Zeroizing::new(read_private(&root.join("SIGNER"), 48)?);
        let seed = signer[..32]
            .try_into()
            .map_err(|_| invalid("invalid prototype signer"))?;
        let generation = uuid::Uuid::from_slice(&signer[32..])
            .map_err(|_| invalid("invalid prototype signer generation"))?;
        Ok(Self {
            root,
            writer: Writer::with_generation(seed, &generation.to_string())?,
            key: Zeroizing::new(*key),
            failed: AtomicBool::new(false),
            created,
            _lock: lock,
        })
    }

    pub(super) fn require_ready(&self) -> StorageResult<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(invalid(
                "prototype selector publication is uncertain; reopen the root",
            ));
        }
        Ok(())
    }
}

fn create(path: &Path, key: &[u8; 32]) -> StorageResult<()> {
    fs::DirBuilder::new().mode(0o700).create(path)?;
    let root = path.canonicalize()?;
    write_new(&root.join("LOCK"), b"")?;
    write_new(&root.join("ACCOUNT_KEY"), key)?;
    let mut signer = Zeroizing::new([0u8; 48]);
    rand::rngs::OsRng.fill_bytes(&mut signer[..32]);
    signer[32..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    write_new(&root.join("SIGNER"), signer.as_ref())?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("generations"))?;
    write_new(&root.join("FORMAT"), MARKER)?;
    File::open(&root)?.sync_all()?;
    File::open(
        root.parent()
            .ok_or_else(|| invalid("prototype root needs a parent"))?,
    )?
    .sync_all()?;
    Ok(())
}

pub(super) fn private_directory(path: &Path) -> StorageResult<()> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.mode() & 0o077 != 0 || meta.uid() != current_uid() {
        return Err(invalid("prototype directory must be private and owned"));
    }
    Ok(())
}

pub(super) fn read_private(path: &Path, length: usize) -> StorageResult<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    private_file(&file.metadata()?, length)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(length as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() != length {
        return Err(invalid("prototype file length differs"));
    }
    Ok(bytes)
}

fn private_file(meta: &fs::Metadata, length: usize) -> StorageResult<()> {
    if !meta.is_file()
        || meta.uid() != current_uid()
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
        || meta.len() != length as u64
    {
        return Err(invalid(
            "prototype control file must be private, owned, and exact",
        ));
    }
    Ok(())
}

pub(super) fn write_new(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn current_uid() -> u32 {
    // The prototype is Unix-only, as are the native home locks it exercises.
    unsafe { libc::geteuid() }
}
