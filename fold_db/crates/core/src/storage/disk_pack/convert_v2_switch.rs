//! The file moves of a v1 to v2 disk format conversion: the switch, and the
//! resume that finishes or rolls back an unfinished switch.

use super::convert_v2::{
    invalid, BUILDS, CAS_CONVERTING, CONVERTING, INTENT, INTENT_BYTES, V1_ENTRIES,
};
use super::run_node::{CAS_DIR, RUN_DIR, RUN_NODE_BYTES};
use super::*;
use std::fs::{File, OpenOptions};
use std::io::Write;

/// Write the intent, move the v1 files to the kept folder (marker first),
/// put the run homes in place, write the v2 marker, then remove the intent.
pub(super) fn switch(node: &Path, kept: &Path) -> StorageResult<()> {
    write_durable(&node.join(INTENT), INTENT_BYTES)?;
    sync_dir(node)?;
    std::fs::create_dir(kept)?;
    sync_dir(kept.parent().unwrap_or(node))?;
    for name in V1_ENTRIES {
        std::fs::rename(node.join(name), kept.join(name))?;
        sync_dir(kept)?;
        sync_dir(node)?;
    }
    for (build, home) in BUILDS {
        std::fs::rename(node.join(build), node.join(home))?;
    }
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
/// the v1 home back: the partial run homes go first, then the kept files
/// come back with the marker last.
pub(super) fn resume(node: &Path, kept: &Path) -> StorageResult<()> {
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
    for name in [RUN_DIR, CAS_DIR, CONVERTING, CAS_CONVERTING] {
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

pub(super) fn remove_if_present(path: &Path) -> StorageResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn sync_dir(path: &Path) -> StorageResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
