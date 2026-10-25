//! One-time conversion of a v1 node home to disk format v2.
//!
//! The owner asks for it with `LASTDB_DISK_FORMAT=2` when a node with the
//! node key opens a v1 home. The conversion copies each stored record of the
//! current v1 generation into a new run home, closes the run home, opens it
//! again, and compares each record with the v1 source. Only then does the
//! node home switch:
//!
//! ```text
//! <node>/laststore-disk-convert.lock    held for the whole conversion; never moves
//! <node>/disk-run.converting/           the new run home while it is built and checked
//! <node>/laststore-disk-convert-v2      the switch intent, written before the first move
//! <node>-disk-format-v1/                the v1 files, kept until the owner deletes them
//! <node>/disk-run/                      the run home after the switch
//! <node>/laststore-pack-node-v1         "format=3\nhome=disk-run\n", written last
//! ```
//!
//! The switch moves the v1 marker first, so every crash state inside the
//! switch has the intent file. The v2 marker is the commit point; the intent
//! goes after it. A keyed open that finds the intent and no v2 marker rolls
//! the switch back: it removes the partial run home, moves the kept v1 files
//! back with the marker last, and the node is v1 again (and converts again
//! when the owner still asks). Without the intent nothing is rolled back or
//! deleted. The conversion does not change a v1 record; it opens the v1
//! writer as a normal v1 open does.

use super::run_node::{home_key, RUN_DIR, RUN_NODE_BYTES};
use super::*;
use laststore::disk_pack::maintenance::visit_generation;
use laststore::disk_run::home::RunHome;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Instant;

const CONVERTING: &str = "disk-run.converting";
const LOCK: &str = "laststore-disk-convert.lock";
const INTENT: &str = "laststore-disk-convert-v2";
const INTENT_BYTES: &[u8] = b"switch=v1-to-v2\n";
/// Moved in this order; the roll-back moves them back in reverse.
const V1_ENTRIES: [&str; 3] = [NODE_MARKER, "laststore-layout-v1", "disk-pack"];
const BATCH_RECORDS: usize = 4096;
const BATCH_BYTES: usize = 16 * 1024 * 1024;
const VERIFY_RECORDS: usize = 1000;

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
        resume(node, &kept)?;
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
    let lock = take_lock(node)?;
    let report = convert(node, node_key, &kept)?;
    drop(lock);
    Ok(Some(report))
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
    let key = home_key(node_key);
    let (records, logical_bytes) = {
        let run = RunHome::create(&node.join(CONVERTING), &key).map_err(map)?;
        let copied = copy(&source, &current, &run)?;
        while run.merge_step().map_err(map)?.is_some() {}
        copied
    };
    // Compare with a new open: the files and the derived key, not memory.
    let verified = {
        let run = RunHome::open(&node.join(CONVERTING), &key).map_err(map)?;
        let stored: u64 = run.status().levels.iter().map(|level| level.1).sum();
        if stored != records {
            return Err(invalid(
                "disk format conversion: the run home holds a different record count",
            ));
        }
        verify(&source, &current, &run)?
    };
    if verified != records {
        return Err(invalid(
            "disk format conversion verified a different record count",
        ));
    }
    drop(writer);
    let v1_disk_bytes = directory_bytes(&v1_root, 8)?;
    let v2_disk_bytes = directory_bytes(&node.join(CONVERTING), 8)?;
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

/// Remove what an earlier conversion left before its switch: the run home
/// being built and the temporary folders of its creation. No v1 file is
/// among them.
fn remove_partial_builds(node: &Path) -> StorageResult<()> {
    let temporary = format!(".{CONVERTING}.create-");
    for entry in std::fs::read_dir(node)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if (name == CONVERTING || name.starts_with(&temporary)) && entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        }
    }
    sync_dir(node)
}

/// Copy each stored record of `current`. Tombstones are not copied: an
/// absent key in a new home reads the same.
fn copy(source: &DiskHome, current: &Current, run: &RunHome) -> StorageResult<(u64, u64)> {
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;
    let mut records = 0u64;
    let mut logical = 0u64;
    let commit = |batch: Vec<HomeMutation>| -> laststore::Result<()> {
        RunHome::check_mutations(&batch)?;
        run.commit(batch).map(drop)
    };
    visit_generation(source.path(), current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        let value = read_body(body, record.body_bytes)?;
        let size = record.collection.len() + record.key.len() + value.len();
        batch_bytes += size;
        logical += size as u64;
        records += 1;
        batch.push(HomeMutation {
            collection: record.collection.into(),
            key: record.key.into(),
            body: Some(value),
        });
        if batch.len() >= BATCH_RECORDS || batch_bytes >= BATCH_BYTES {
            commit(std::mem::take(&mut batch))?;
            batch_bytes = 0;
        }
        Ok(())
    })
    .map_err(map)?;
    if !batch.is_empty() {
        commit(batch).map_err(map)?;
    }
    Ok((records, logical))
}

/// Read every v1 record again and compare it with the run home in batches.
fn verify(source: &DiskHome, current: &Current, run: &RunHome) -> StorageResult<u64> {
    let mut pending: Vec<((String, String), Vec<u8>)> = Vec::new();
    let mut pending_bytes = 0usize;
    let mut verified = 0u64;
    let check = |pending: Vec<((String, String), Vec<u8>)>| -> laststore::Result<u64> {
        let (coordinates, expected): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
        let found = run.read(unlimited()).get_many(&coordinates)?;
        for ((coordinate, expected), found) in coordinates.iter().zip(&expected).zip(found) {
            if found?.as_deref() != Some(expected.as_slice()) {
                return Err(laststore::Error::Config(format!(
                    "disk format conversion: record differs after reopen in collection {}",
                    coordinate.0
                )));
            }
        }
        Ok(coordinates.len() as u64)
    };
    visit_generation(source.path(), current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        let value = read_body(body, record.body_bytes)?;
        pending_bytes += value.len();
        pending.push(((record.collection.into(), record.key.into()), value));
        if pending.len() >= VERIFY_RECORDS || pending_bytes >= BATCH_BYTES {
            verified += check(std::mem::take(&mut pending))?;
            pending_bytes = 0;
        }
        Ok(())
    })
    .map_err(map)?;
    if !pending.is_empty() {
        verified += check(pending).map_err(map)?;
    }
    Ok(verified)
}

fn read_body(body: &mut dyn Read, size: u64) -> laststore::Result<Vec<u8>> {
    let limit = laststore::disk_run::MAX_RUN_VALUE_BYTES as u64;
    if size > limit {
        return Err(laststore::Error::Config(
            "disk format conversion: a v1 value exceeds the run value limit".into(),
        ));
    }
    let mut value = Vec::with_capacity(size as usize);
    body.take(limit + 1).read_to_end(&mut value)?;
    if value.len() as u64 != size {
        return Err(laststore::Error::Config(
            "disk format conversion: a v1 body has a different size than its record".into(),
        ));
    }
    Ok(value)
}

fn unlimited() -> ReadBudget {
    ReadBudget {
        pages: u64::MAX,
        blocks: u64::MAX,
        bytes: u64::MAX,
        keys: u64::MAX,
    }
}

/// Write the intent, move the v1 files to the kept folder (marker first),
/// put the run home in place, write the v2 marker, then remove the intent.
fn switch(node: &Path, kept: &Path) -> StorageResult<()> {
    write_durable(&node.join(INTENT), INTENT_BYTES)?;
    sync_dir(node)?;
    std::fs::create_dir(kept)?;
    sync_dir(kept.parent().unwrap_or(node))?;
    for name in V1_ENTRIES {
        std::fs::rename(node.join(name), kept.join(name))?;
        sync_dir(kept)?;
        sync_dir(node)?;
    }
    std::fs::rename(node.join(CONVERTING), node.join(RUN_DIR))?;
    sync_dir(node)?;
    let staged = node.join(format!(".{NODE_MARKER}.tmp"));
    remove_if_present(&staged)?;
    write_durable(&staged, RUN_NODE_BYTES)?;
    std::fs::rename(&staged, node.join(NODE_MARKER))?;
    sync_dir(node)?;
    std::fs::remove_file(node.join(INTENT))?;
    sync_dir(node)
}

/// The caller holds the conversion lock and found the intent. With the v2
/// marker the switch is complete and only the intent goes. Without it, put
/// the v1 home back: the partial run home goes first, then the kept files
/// come back with the marker last.
fn resume(node: &Path, kept: &Path) -> StorageResult<()> {
    let marker = match std::fs::read(node.join(NODE_MARKER)) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if marker.as_deref() == Some(RUN_NODE_BYTES) {
        std::fs::remove_file(node.join(INTENT))?;
        return sync_dir(node);
    }
    for name in V1_ENTRIES {
        if !node.join(name).try_exists()? && !kept.join(name).try_exists()? {
            return Err(invalid(&format!(
                "an unfinished disk format conversion cannot find the v1 {name} in {} or {}; \
                 nothing was changed",
                node.display(),
                kept.display()
            )));
        }
    }
    for name in [RUN_DIR, CONVERTING] {
        let partial = node.join(name);
        if partial.try_exists()? {
            std::fs::remove_dir_all(&partial)?;
        }
    }
    remove_if_present(&node.join(format!(".{NODE_MARKER}.tmp")))?;
    sync_dir(node)?;
    for name in V1_ENTRIES.iter().rev() {
        let from = kept.join(name);
        if from.try_exists()? && !node.join(name).try_exists()? {
            std::fs::rename(&from, node.join(name))?;
            sync_dir(node)?;
        }
    }
    if kept.try_exists()? && std::fs::read_dir(kept)?.next().is_none() {
        std::fs::remove_dir(kept)?;
        sync_dir(kept.parent().unwrap_or(node))?;
    }
    std::fs::remove_file(node.join(INTENT))?;
    sync_dir(node)?;
    tracing::warn!(
        target: "lastdb::disk_convert",
        node = %node.display(),
        "an unfinished disk format conversion was rolled back; the home is v1"
    );
    Ok(())
}

fn write_durable(path: &Path, bytes: &[u8]) -> StorageResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn remove_if_present(path: &Path) -> StorageResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn sync_dir(path: &Path) -> StorageResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
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

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
