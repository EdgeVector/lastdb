//! Identify the exact native files and install them in a fresh home.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{DecimalU64, Digest, RelativePath};
use fold_db::storage::disk_pack::{snapshot::DatabaseSnapshot, CAS_DIR, NODE_MARKER, RUN_DIR};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileEntry {
    pub path: RelativePath,
    pub destination: Destination,
    pub bytes: DecimalU64,
    pub sha256: Digest,
}

pub(super) fn inventory(cut: &DatabaseSnapshot) -> Result<Vec<FileEntry>, String> {
    let files = cut.files().map_err(|error| error.to_string())?;
    let mut entries = Vec::with_capacity(files.len());
    for (path, len) in files {
        let name = path.to_str().ok_or("snapshot path is not UTF-8")?;
        let destination = provider(name)?;
        let mut file = cut.open_file(&path).map_err(|error| error.to_string())?;
        let mut hash = Sha256::new();
        let bytes = std::io::copy(&mut (&mut file).take(len + 1), &mut hash)
            .map_err(|error| error.to_string())?;
        file.rewind().map_err(|error| error.to_string())?;
        if bytes != len {
            return Err("snapshot source file length changed".into());
        }
        entries.push(FileEntry {
            path: name
                .to_owned()
                .try_into()
                .map_err(|_| "invalid snapshot path")?,
            destination,
            bytes: DecimalU64::new(bytes),
            sha256: digest(hash)?,
        });
    }
    validate(&entries)?;
    Ok(entries)
}

pub(super) fn digest(hash: Sha256) -> Result<Digest, String> {
    Digest::try_from(hex::encode(hash.finalize())).map_err(|error| error.to_string())
}

pub(super) fn validate(files: &[FileEntry]) -> Result<(), String> {
    let mut paths = BTreeSet::new();
    for entry in files {
        if provider(entry.path.as_str())? != entry.destination || !paths.insert(entry.path.as_str())
        {
            return Err("invalid or duplicate snapshot file".into());
        }
    }
    if !paths.contains(NODE_MARKER)
        || [RUN_DIR, CAS_DIR].iter().any(|root| {
            ["CURRENT", "laststore-run-home-v2"]
                .iter()
                .any(|file| !paths.contains(format!("{root}/{file}").as_str()))
        })
    {
        return Err("snapshot is missing native database metadata".into());
    }
    Ok(())
}

fn provider(path: &str) -> Result<Destination, String> {
    if path == NODE_MARKER {
        return Ok(Destination::R2);
    }
    let (root, name) = path.split_once('/').ok_or("invalid snapshot path")?;
    let allowed = matches!(name, "CURRENT" | "laststore-run-home-v2")
        || name
            .strip_prefix("runs/")
            .and_then(|run| run.strip_suffix(".ldrun"))
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id));
    if !allowed {
        return Err("unexpected snapshot file".into());
    }
    match root {
        RUN_DIR => Ok(Destination::R2),
        CAS_DIR => Ok(Destination::B2),
        _ => Err("unexpected snapshot directory".into()),
    }
}

pub(super) fn create_layout(destination: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new().mode(0o700).create(destination)?;
    for name in [RUN_DIR, CAS_DIR] {
        let home = destination.join(name);
        fs::DirBuilder::new().mode(0o700).create(&home)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(home.join("runs"))?;
        create_file(&home.join("LOCK"))?.sync_all()?;
    }
    Ok(())
}

pub(super) fn create_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

pub(super) fn sync_layout(destination: &Path) -> std::io::Result<()> {
    for name in [RUN_DIR, CAS_DIR] {
        File::open(destination.join(name).join("runs"))?.sync_all()?;
        File::open(destination.join(name))?.sync_all()?;
    }
    File::open(destination)?.sync_all()
}
