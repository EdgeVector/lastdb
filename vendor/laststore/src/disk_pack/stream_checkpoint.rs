use super::journal::JournalTail;
use super::publish::{ensure_generation, fresh_generation, publish_current};
use super::write_lock::WriteLock;
use super::{artifact, corrupt, generation, Current, PublishedGeneration};
use crate::Result;

/// Complete the second half of a verified empty-stream change.
pub(super) fn publish_stream(
    lock: &WriteLock,
    previous: &Current,
    tail: &JournalTail,
) -> Result<PublishedGeneration> {
    let root = lock.root();
    tail.validate_for_current(root, previous)?;
    if tail.stream == previous.journal_stream
        || tail.last_batch != previous.frontier
        || tail.encoded_bytes != 0
        || previous.journal_frontier != previous.frontier
    {
        return Err(corrupt(
            "stream change requires a complete unchanged root checkpoint",
        ));
    }
    let old = ensure_generation(lock, previous)?;
    let mut current = previous.clone();
    current.generation = fresh_generation(root, previous.generation)?;
    current.previous_generation = previous.generation;
    current.required_features |= super::FEATURE_JOURNAL_LIFECYCLE;
    current.journal_stream = tail.stream;
    current.published_ms = artifact::unix_ms();
    let receipt = generation::write_carried(root, &current, &old.coordinates, &old)?;
    if artifact::current(root)? != *previous {
        return Err(corrupt("CURRENT changed during stream checkpoint"));
    }
    publish_current(root, &current)?;
    lock.remember_checked(&current)?;
    Ok(PublishedGeneration {
        current,
        receipt: receipt.object,
        dependency_parts: receipt.dependencies,
        mutations: 0,
        metadata_pages_written: 0,
        metadata_parts_written: 0,
    })
}
