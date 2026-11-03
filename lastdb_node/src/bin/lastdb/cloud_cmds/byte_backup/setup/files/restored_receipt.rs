//! Bind the completed restore receipt throughout account reconnect.
use super::super::{recovery_files, require_private_file};
use cloud_backup_protocol::{bytes_sha256, Digest};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;

const RECEIPT: &str = "byte-cloud-restore-receipt.json";
const VERSION: u8 = 2;
const MAX_BYTES: u64 = 65_536;

pub(super) struct Proof {
    file: File,
    digest: Digest,
}

impl Proof {
    pub fn open(directory: &File, home: &Path) -> Result<Self, String> {
        selected(directory)?;
        let file = File::from(
            fs::openat(
                directory,
                RECEIPT,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| "cannot open completed restore receipt")?,
        );
        let bytes = contents(&file)?;
        validate(&bytes, home)?;
        let proof = Self {
            file,
            digest: bytes_sha256(&bytes),
        };
        proof.binding(directory)?;
        Ok(proof)
    }

    pub fn check(&self, directory: &File) -> Result<(), String> {
        self.binding(directory)?;
        if bytes_sha256(&contents(&self.file)?) != self.digest {
            return Err("restore receipt changed during reconnect".into());
        }
        self.binding(directory)
    }

    fn binding(&self, directory: &File) -> Result<(), String> {
        selected(directory)?;
        let held = fs::fstat(&self.file).map_err(|_| "cannot inspect held restore receipt")?;
        let current = fs::statat(directory, RECEIPT, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| "cannot inspect selected restore receipt")?;
        if held.st_dev != current.st_dev || held.st_ino != current.st_ino {
            return Err("restore receipt identity changed during reconnect".into());
        }
        Ok(())
    }
}

fn selected(directory: &File) -> Result<(), String> {
    if recovery_files::entry_present(directory, RECEIPT)? {
        Ok(())
    } else {
        Err("restored reconnect requires the completed restore receipt".into())
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u8,
    session_id: uuid::Uuid,
    report: lastdb_node::byte_cloud_restore::RestoreReport,
}

fn validate(bytes: &[u8], home: &Path) -> Result<(), String> {
    let receipt: Receipt =
        serde_json::from_slice(bytes).map_err(|_| "invalid native restore receipt")?;
    if receipt.version != VERSION
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

fn contents(file: &File) -> Result<Vec<u8>, String> {
    require_private_file(file)?;
    let before = file
        .metadata()
        .map_err(|_| "cannot inspect completed restore receipt")?;
    if before.len() > MAX_BYTES {
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
