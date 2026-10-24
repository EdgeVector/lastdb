//! Bind one completed V1 or V2 restore receipt throughout account reconnect.
use super::super::{recovery_files, require_private_file};
use cloud_backup_protocol::{bytes_sha256, Digest};
use rustix::fs::{self, AtFlags, Mode, OFlags};
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;

const V1: &str = "native-cloud-restore-receipt.json";
const V2: &str = "byte-cloud-restore-receipt.json";
const MAX_BYTES: u64 = 65_536;

pub(super) struct Proof {
    file: File,
    name: &'static str,
    version: u8,
    digest: Digest,
}

impl Proof {
    pub fn open(directory: &File, home: &Path) -> Result<Self, String> {
        let (name, version) = selected(directory)?;
        let file = File::from(
            fs::openat(
                directory,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|_| "cannot open completed restore receipt")?,
        );
        let bytes = contents(&file)?;
        validate(&bytes, version, home)?;
        let proof = Self {
            file,
            name,
            version,
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
        if selected(directory)? != (self.name, self.version) {
            return Err("restore receipt format changed during reconnect".into());
        }
        let held = fs::fstat(&self.file).map_err(|_| "cannot inspect held restore receipt")?;
        let current = fs::statat(directory, self.name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| "cannot inspect selected restore receipt")?;
        if held.st_dev != current.st_dev || held.st_ino != current.st_ino {
            return Err("restore receipt identity changed during reconnect".into());
        }
        Ok(())
    }
}

fn selected(directory: &File) -> Result<(&'static str, u8), String> {
    match (
        recovery_files::entry_present(directory, V1)?,
        recovery_files::entry_present(directory, V2)?,
    ) {
        (true, false) => Ok((V1, 1)),
        (false, true) => Ok((V2, 2)),
        _ => Err("restored reconnect requires exactly one completed restore receipt".into()),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u8,
    session_id: uuid::Uuid,
    report: lastdb_node::native_cloud_restore::RestoreReport,
}

fn validate(bytes: &[u8], version: u8, home: &Path) -> Result<(), String> {
    let receipt: Receipt =
        serde_json::from_slice(bytes).map_err(|_| "invalid native restore receipt")?;
    if receipt.version != version
        || receipt.session_id.is_nil()
        || receipt.report.destination != home
        || !receipt.report.file_dependencies_verified
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
