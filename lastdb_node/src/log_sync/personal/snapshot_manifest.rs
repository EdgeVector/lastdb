//! The manifest of a snapshot: the native files of the frozen database, by digest.
//!
//! The files are the files of the store as they are on disk. They are already
//! encrypted at rest (AES-256-GCM, with keys derived from the node seed), so
//! the manifest lists them by path, length and SHA-256 and nothing seals them
//! again. The manifest itself holds no key and no record: it is a blob on the
//! relay, and the `snapshot` entry names it by digest and length.

use std::collections::BTreeSet;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{Read, Seek};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

use fold_db::storage::disk_pack::{snapshot::DatabaseSnapshot, CAS_DIR, NODE_MARKER, RUN_DIR};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A manifest is read whole, so a larger one is refused. It lists at most a few
/// thousand files of about a hundred bytes each.
pub(super) const MANIFEST_LIMIT: u64 = 16 * 1024 * 1024;
/// The most files one snapshot may list.
const MAX_FILES: usize = 4096;

/// One native file of the snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileEntry {
    /// The path under the data directory, for example `disk-run/runs/<uuid>.ldrun`.
    pub(super) path: String,
    /// The length in bytes.
    pub(super) bytes: u64,
    /// The SHA-256 of the file, 64 lowercase hex characters.
    pub(super) sha256: String,
}

/// The files of one snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    /// Names this upload. Two manifests of equal files still differ by it.
    pub(super) upload_id: String,
    /// The files, ascending by path.
    pub(super) files: Vec<FileEntry>,
}

impl Manifest {
    /// The total length of the files.
    pub(super) fn bytes(&self) -> u64 {
        self.files.iter().map(|file| file.bytes).sum()
    }
}

/// Hash every file of the frozen database, one at a time and in chunks.
pub(super) fn inventory(cut: &DatabaseSnapshot) -> Result<Vec<FileEntry>, String> {
    let listed = cut.files().map_err(|error| error.to_string())?;
    let mut entries = Vec::with_capacity(listed.len());
    for (path, len) in listed {
        let name = path.to_str().ok_or("a snapshot path is not UTF-8")?;
        let mut file = cut.open_file(&path).map_err(|error| error.to_string())?;
        file.rewind().map_err(|error| error.to_string())?;
        let mut hash = Sha256::new();
        let bytes = std::io::copy(&mut (&mut file).take(len + 1), &mut hash)
            .map_err(|error| error.to_string())?;
        if bytes != len {
            return Err("a snapshot source file changed its length".into());
        }
        entries.push(FileEntry {
            path: name.to_owned(),
            bytes,
            sha256: app_identity_crypto::hex_lower(&hash.finalize()),
        });
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    validate(&entries)?;
    Ok(entries)
}

/// Refuse a file list that is not exactly the native files of a store.
pub(super) fn validate(files: &[FileEntry]) -> Result<(), String> {
    if files.len() > MAX_FILES {
        return Err("a snapshot lists too many files".into());
    }
    let mut paths = BTreeSet::new();
    for entry in files {
        if !native_path(&entry.path) || !paths.insert(entry.path.as_str()) {
            return Err("a snapshot lists an unexpected or repeated file".into());
        }
        let hex = entry.sha256.len() == 64
            && entry
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !hex {
            return Err("a snapshot lists a file with a bad digest".into());
        }
        // The relay does not store an empty blob.
        if entry.bytes == 0 {
            return Err("a snapshot lists an empty file".into());
        }
    }
    let complete = paths.contains(NODE_MARKER)
        && [RUN_DIR, CAS_DIR].iter().all(|root| {
            ["CURRENT", "laststore-run-home-v2"]
                .iter()
                .all(|file| paths.contains(format!("{root}/{file}").as_str()))
        });
    if complete {
        Ok(())
    } else {
        Err("a snapshot lacks native database metadata".into())
    }
}

/// True for the node marker, the two manifests of a store and its run files.
fn native_path(path: &str) -> bool {
    if path == NODE_MARKER {
        return true;
    }
    let Some((root, name)) = path.split_once('/') else {
        return false;
    };
    if root != RUN_DIR && root != CAS_DIR {
        return false;
    }
    matches!(name, "CURRENT" | "laststore-run-home-v2")
        || name
            .strip_prefix("runs/")
            .and_then(|run| run.strip_suffix(".ldrun"))
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id))
}

/// Make the empty directory layout of a store at `destination`, which must not exist.
pub(super) fn create_layout(destination: &Path) -> std::io::Result<()> {
    DirBuilder::new().mode(0o700).create(destination)?;
    for name in [RUN_DIR, CAS_DIR] {
        let home = destination.join(name);
        DirBuilder::new().mode(0o700).create(&home)?;
        DirBuilder::new().mode(0o700).create(home.join("runs"))?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(home.join("LOCK"))?
            .sync_all()?;
    }
    Ok(())
}

/// Make the installed layout durable.
pub(super) fn sync_layout(destination: &Path) -> std::io::Result<()> {
    for name in [RUN_DIR, CAS_DIR] {
        File::open(destination.join(name).join("runs"))?.sync_all()?;
        File::open(destination.join(name))?.sync_all()?;
    }
    File::open(destination)?.sync_all()
}

/// Remove a directory tree, if it exists.
pub(super) fn remove_tree(path: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}
