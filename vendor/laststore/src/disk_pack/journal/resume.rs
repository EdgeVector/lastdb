use super::tail::{archive_path, inspect_archive, Archived};
use super::{JournalTail, TailPredecessor};
use crate::disk_pack::bytes::digest;
use crate::disk_pack::page_write::directory;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{
    artifact, corrupt, publish, stream_checkpoint, Current, PublishedGeneration,
};
use crate::{durability, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

pub(super) fn initial(lock: &WriteLock, current: &Current) -> Result<JournalTail> {
    let path = lock
        .root()
        .join("journal")
        .join(current.journal_stream.to_string());
    let unadmitted = current.required_features & crate::disk_pack::FEATURE_JOURNAL_LIFECYCLE != 0
        && current.previous_generation == 0;
    if current.previous_generation != 0 {
        return Err(corrupt("admitted journal has no durable TAIL"));
    }
    if !path.try_exists()? {
        directory(&path)?;
    } else if !unadmitted || !path.is_dir() {
        return Err(corrupt("existing admitted stream has no durable TAIL"));
    }
    // The mandatory lifecycle feature proves that initialization admitted no batch.
    // Old incomplete identities are retained; rotation never writes frames under them.
    let tail = JournalTail::initial(current)?;
    tail.publish(lock.root())?;
    Ok(tail)
}

pub(super) fn checkpoint(lock: &WriteLock) -> Result<(PublishedGeneration, JournalTail)> {
    let recovered = publish::publish_locked(lock)?;
    let (mut publication, tail) = rotate(lock, &recovered.current)?;
    publication.mutations = recovered.mutations;
    publication.metadata_pages_written = recovered.metadata_pages_written;
    publication.metadata_parts_written = recovered.metadata_parts_written;
    Ok((publication, tail))
}

fn rotate(lock: &WriteLock, current: &Current) -> Result<(PublishedGeneration, JournalTail)> {
    let root = lock.root();
    let old = JournalTail::read(root)?;
    old.validate_for_current(root, current)?;
    if old.stream != current.journal_stream || old.last_batch != current.frontier {
        return Err(corrupt(
            "new journal stream requires the old complete frontier",
        ));
    }
    let bytes = old.encode()?;
    archive(root, &old, &bytes)?;
    let stream = Uuid::new_v4();
    directory(&root.join("journal").join(stream.to_string()))?;
    let mut tail = JournalTail::initial(current)?;
    tail.stream = stream;
    tail.published_ms = artifact::unix_ms();
    tail.predecessor = Some(TailPredecessor {
        stream: old.stream,
        checkpoint: current.frontier,
        tail_digest: digest(&bytes),
    });
    // No admission can occur between these two independently durable publications.
    tail.publish(root)?;
    let publication = stream_checkpoint::publish_stream(lock, current, &tail)?;
    Ok((publication, tail))
}

/// Make the exact old TAIL durable at its archive name, whole or not at all.
///
/// The bytes reach the archive name only by rename, after a file barrier, so a
/// crash leaves no archive, an unused pending file, or a complete archive.
/// A short or undecodable file at the archive name comes from an older
/// in-place write or from a failed write. Nothing references it before the new
/// TAIL exists, so the exact bytes replace it by rename. A complete archive
/// that decodes to a different TAIL is still refused.
fn archive(root: &Path, tail: &JournalTail, bytes: &[u8; 4096]) -> Result<()> {
    let path = archive_path(root, tail.stream, tail.last_batch);
    match inspect_archive(&path, bytes)? {
        Archived::Exact => {
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            durability::sync_dirty_file(&file)?;
        }
        Archived::Different => {
            return Err(corrupt(
                "an existing archived TAIL differs from its checkpoint",
            ));
        }
        Archived::Absent | Archived::Torn => {
            // The exclusive writer lock makes one fixed pending name safe to reuse.
            let pending = path.with_extension("pending");
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&pending)?;
            file.write_all(bytes)?;
            durability::sync_publication_file(&file)?;
            fs::rename(&pending, &path)?;
        }
    }
    durability::sync_directory(&File::open(path.parent().expect("stream directory"))?)
}
