use super::tail::{archive_path, read_archived};
use super::{JournalTail, TailPredecessor};
use crate::disk_pack::bytes::digest;
use crate::disk_pack::page_write::directory;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{
    artifact, corrupt, publish, stream_checkpoint, Current, PublishedGeneration,
};
use crate::{durability, Result};
use std::fs::{File, OpenOptions};
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

fn archive(root: &Path, tail: &JournalTail, bytes: &[u8; 4096]) -> Result<()> {
    let path = archive_path(root, tail.stream, tail.last_batch);
    let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if read_archived(root, tail.stream, tail.last_batch)? != *bytes {
                return Err(corrupt(
                    "an existing archived TAIL differs from its checkpoint",
                ));
            }
            OpenOptions::new().read(true).write(true).open(&path)?
        }
        Err(error) => return Err(error.into()),
    };
    durability::sync_dirty_file(&file)?;
    durability::sync_directory(&File::open(path.parent().expect("stream directory"))?)
}
