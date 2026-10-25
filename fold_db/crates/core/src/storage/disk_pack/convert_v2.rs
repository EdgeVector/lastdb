//! One-time conversion of a v1 node home to disk format v2.
//!
//! The owner asks for it with `LASTDB_DISK_FORMAT=2` when a node with the
//! node key opens a v1 home. The conversion copies each stored record of the
//! current v1 generation into new run homes (the CAS files apart), closes them, opens them
//! again, and compares each record with the v1 source. Only then does the
//! node home switch:
//!
//! ```text
//! <node>/laststore-disk-convert.lock    held for the whole conversion; never moves
//! <node>/disk-run.converting/           the new run homes while they are built and checked
//! <node>/disk-cas.converting/            (records, and the CAS files apart)
//! <node>/laststore-disk-convert-v2      the switch intent, written before the first move
//! <node>-disk-format-v1/                the v1 files, kept until the owner deletes them
//! <node>/disk-run/, <node>/disk-cas/    the run homes after the switch
//! <node>/laststore-pack-node-v1         the v2 marker, written last
//! ```
//!
//! The switch moves the v1 marker first, so every crash state inside the
//! switch has the intent file. The v2 marker is the commit point; the intent
//! goes after it. A keyed open that finds the intent and no v2 marker rolls
//! the switch back: it removes the partial run homes, moves the kept v1 files
//! back with the marker last, and the node is v1 again (and converts again
//! when the owner still asks). Without the intent nothing is rolled back or
//! deleted. The conversion does not change a v1 record; it opens the v1
//! writer as a normal v1 open does.

use super::convert_v2_copy::{copy, verify};
use super::convert_v2_switch::{remove_if_present, resume, switch, sync_dir};
use super::run_node::{cas_key, home_key, CAS_DIR, RUN_DIR};
use super::*;
use laststore::disk_run::home::RunHome;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::PathBuf;
use std::time::Instant;

pub(super) const CONVERTING: &str = "disk-run.converting";
pub(super) const CAS_CONVERTING: &str = "disk-cas.converting";
/// Each run home being built, with its final name.
pub(super) const BUILDS: [(&str, &str); 2] = [(CONVERTING, RUN_DIR), (CAS_CONVERTING, CAS_DIR)];
const LOCK: &str = "laststore-disk-convert.lock";
pub(super) const INTENT: &str = "laststore-disk-convert-v2";
pub(super) const INTENT_BYTES: &[u8] = b"switch=v1-to-v2\n";
/// Moved in this order; the roll-back moves them back in reverse.
pub(super) const V1_ENTRIES: [&str; 3] = [NODE_MARKER, "laststore-layout-v1", "disk-pack"];

/// What one conversion did.
#[derive(Debug, Clone)]
pub struct DiskConversionReport {
    pub records: u64,
    pub logical_bytes: u64,
    pub v1_disk_bytes: u64,
    pub v2_disk_bytes: u64,
    pub seconds: f64,
    pub kept_v1: PathBuf,
}

/// The folder that keeps the v1 files of `node` after a conversion.
pub fn kept_v1_path(node: &Path) -> StorageResult<PathBuf> {
    let name = node
        .file_name()
        .ok_or_else(|| invalid("a node home path has no name"))?;
    let parent = node
        .parent()
        .ok_or_else(|| invalid("a node home path has no parent"))?;
    Ok(parent.join(format!("{}-disk-format-v1", name.to_string_lossy())))
}

/// Finish or roll back an unfinished switch, then convert a v1 home when
/// the owner asks. Returns the report when this call converted the home.
pub(super) fn resume_or_convert(
    node: &Path,
    node_key: &[u8; 32],
) -> StorageResult<Option<DiskConversionReport>> {
    if !node.try_exists()? {
        return Ok(None);
    }
    // The kept folder must be on the volume of the real node folder.
    let node = &node.canonicalize()?;
    let kept = kept_v1_path(node)?;
    if node.join(INTENT).try_exists()? {
        let _lock = take_lock(node)?;
        // Another opener can finish the resume before this one has the lock.
        if node.join(INTENT).try_exists()? {
            resume(node, &kept)?;
        }
        drop(_lock);
        remove_lock_in_v1(node)?;
    }
    let asked = std::env::var("LASTDB_DISK_FORMAT").as_deref() == Ok("2");
    let marker = match std::fs::read(node.join(NODE_MARKER)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !asked || marker != initialization::NODE_BYTES {
        return Ok(None);
    }
    if node
        .join("laststore-current-keys-v3-initializing")
        .try_exists()?
    {
        return Err(invalid(
            "disk format conversion needs a complete v1 home; open it as v1 first",
        ));
    }
    if kept.try_exists()? {
        return Err(invalid(&format!(
            "disk format conversion stopped: {} already exists and this conversion does not \
             replace it; rename it, then open again",
            kept.display()
        )));
    }
    if node.join("native-file-recovery-v1").try_exists()? {
        return Err(invalid(
            "disk format conversion does not move native file recovery state yet; \
             the home stays v1",
        ));
    }
    same_volume(node)?;
    let lock = take_lock(node)?;
    // Another opener can have converted the home before this one had the lock.
    if std::fs::read(node.join(NODE_MARKER)).ok().as_deref() != Some(initialization::NODE_BYTES) {
        return Ok(None);
    }
    let converted = convert(node, node_key, &kept);
    if converted.is_err() && !node.join(INTENT).try_exists()? {
        // Failed before the switch: v1 is complete; remove the partial copy.
        if let Err(error) = remove_partial_builds(node) {
            tracing::warn!(target: "lastdb::disk_convert", %error, "partial v2 copy not removed");
        }
    }
    drop(lock);
    if converted.is_err() {
        remove_lock_in_v1(node)?;
    }
    converted.map(Some)
}

/// One exclusive lock that does not move during the switch. A second opener
/// that finds it held is refused.
fn take_lock(node: &Path) -> StorageResult<File> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(node.join(LOCK))?;
    lock.try_lock().map_err(|error| {
        invalid(&format!(
            "another node owner holds the disk format conversion lock or its lock failed: {error}"
        ))
    })?;
    Ok(lock)
}

/// A v1 home keeps no conversion lock file: other v1 tools refuse a file
/// they do not know. A v2 home keeps it.
fn remove_lock_in_v1(node: &Path) -> StorageResult<()> {
    if std::fs::read(node.join(NODE_MARKER)).ok().as_deref() == Some(initialization::NODE_BYTES) {
        remove_if_present(&node.join(LOCK))?;
    }
    Ok(())
}

/// The kept folder is made beside the node folder and the switch renames
/// into it, so both must be on one volume.
fn same_volume(node: &Path) -> StorageResult<()> {
    use std::os::unix::fs::MetadataExt;
    let parent = node
        .parent()
        .ok_or_else(|| invalid("a node home path has no parent"))?;
    if std::fs::metadata(node)?.dev() != std::fs::metadata(parent)?.dev() {
        return Err(invalid(
            "disk format conversion needs the node home and its parent folder on one volume",
        ));
    }
    Ok(())
}

fn convert(node: &Path, node_key: &[u8; 32], kept: &Path) -> StorageResult<DiskConversionReport> {
    let started = Instant::now();
    // The v1 owner lock: a normal v1 open is refused during the copy.
    let v1_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(node.join(NODE_MARKER))?;
    v1_lock.try_lock().map_err(|error| {
        invalid(&format!(
            "another node owner holds the home or its lock failed: {error}"
        ))
    })?;
    let mut fence = Vec::new();
    File::open(node.join("laststore-layout-v1"))?
        .take(laststore::disk_pack::home::LEGACY_FENCE_BYTES.len() as u64 + 1)
        .read_to_end(&mut fence)?;
    if fence != laststore::disk_pack::home::LEGACY_FENCE_BYTES {
        return Err(invalid(
            "disk format conversion found an unknown v1 layout file",
        ));
    }
    remove_partial_builds(node)?;
    let v1_root = node.join("disk-pack");
    let source = DiskHome::open(&v1_root).map_err(map)?;
    if restore_safety::pending(&source, &v1_root)? {
        return Err(invalid(
            "disk format conversion refuses a home with a pending counter restore validation",
        ));
    }
    let writer = source.snapshot().map_err(map)?;
    let current = writer.current().clone();
    if current.required_features & physical::V2_FEATURES != physical::V2_FEATURES {
        return Err(invalid(
            "disk format conversion needs a v1 home with schema record refs, large batches, \
             and atom records; this home lacks one of them",
        ));
    }
    let keys = [home_key(node_key), cas_key(node_key)];
    let (records, logical_bytes) = {
        let homes = [
            RunHome::create(&node.join(CONVERTING), &keys[0]).map_err(map)?,
            RunHome::create(&node.join(CAS_CONVERTING), &keys[1]).map_err(map)?,
        ];
        let copied = copy(&source, &current, &homes)?;
        for home in &homes {
            while home.merge_step().map_err(map)?.is_some() {}
        }
        copied
    };
    // Compare with a new open: the files and the derived keys, not memory.
    let verified = {
        let homes = [
            RunHome::open(&node.join(CONVERTING), &keys[0]).map_err(map)?,
            RunHome::open(&node.join(CAS_CONVERTING), &keys[1]).map_err(map)?,
        ];
        let stored: u64 = homes.iter().map(|home| home.status().records()).sum();
        if stored != records {
            return Err(invalid(
                "disk format conversion: the run homes hold a different record count",
            ));
        }
        verify(&source, &current, &homes)?
    };
    if verified != records {
        return Err(invalid(
            "disk format conversion verified a different record count",
        ));
    }
    drop(writer);
    let v1_disk_bytes = directory_bytes(&v1_root, 8)?;
    let v2_disk_bytes = directory_bytes(&node.join(CONVERTING), 8)?
        + directory_bytes(&node.join(CAS_CONVERTING), 8)?;
    switch(node, kept)?;
    drop(v1_lock);
    let report = DiskConversionReport {
        records,
        logical_bytes,
        v1_disk_bytes,
        v2_disk_bytes,
        seconds: started.elapsed().as_secs_f64(),
        kept_v1: kept.to_path_buf(),
    };
    tracing::info!(
        target: "lastdb::disk_convert",
        records = report.records,
        logical_bytes = report.logical_bytes,
        v1_disk_bytes = report.v1_disk_bytes,
        v2_disk_bytes = report.v2_disk_bytes,
        seconds = report.seconds,
        kept_v1 = %report.kept_v1.display(),
        "disk format conversion to v2 complete; each record verified after reopen"
    );
    Ok(report)
}

/// Remove what an earlier conversion left before its switch: the run homes
/// being built and the temporary folders of their creation. No v1 file is
/// among them.
fn remove_partial_builds(node: &Path) -> StorageResult<()> {
    for entry in std::fs::read_dir(node)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let partial = BUILDS
            .iter()
            .any(|(build, _)| name == *build || name.starts_with(&format!(".{build}.create-")));
        if partial && entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        }
    }
    sync_dir(node)
}

/// The file bytes under `path`, to `depth` folder levels.
fn directory_bytes(path: &Path, depth: usize) -> StorageResult<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() && depth > 0 {
            total += directory_bytes(&entry.path(), depth - 1)?;
        } else if kind.is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

pub(super) fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
