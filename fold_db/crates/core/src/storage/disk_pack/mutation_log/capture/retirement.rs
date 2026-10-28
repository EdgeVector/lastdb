//! Durable cleanup intent outlives the previous active capture's metadata.
use super::*;
use cloud_backup_protocol::Digest;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retirement {
    version: u16,
    previous: Id,
    previous_begin_sha256: Digest,
    next: Id,
    cancelled: bool,
}
fn marker(root: &Path) -> StorageResult<Option<Retirement>> {
    let marker: Option<Retirement> = store::read(&root.join("retiring.json"))?;
    if marker
        .as_ref()
        .is_some_and(|m| m.version != 1 || m.previous == m.next)
    {
        return Err(invalid("mutation retirement identity"));
    }
    Ok(marker)
}

pub(super) fn prepare(root: &Path, request: &Id) -> StorageResult<()> {
    if let Some(retiring) = marker(root)? {
        let active: Option<Id> = store::read(&root.join("active.json"))?;
        if retiring.cancelled {
            cancel_finish(root, &retiring, active.as_ref())?;
        } else {
            if active.as_ref() != Some(&retiring.previous)
                && active.as_ref() != Some(&retiring.next)
                || active.as_ref() == Some(&retiring.previous) && request != &retiring.next
            {
                return Err(invalid("mutation retirement belongs to another capture"));
            }
            remove_previous(root, &retiring)?;
            if active.as_ref() == Some(&retiring.next) {
                clear(root)?;
            }
        }
    }
    let Some(previous): Option<Id> = store::read(&root.join("active.json"))? else {
        return Ok(());
    };
    if previous == *request {
        return Ok(());
    }
    // A prior retirement for this same request can already remove the old state.
    if let Some(retiring) = marker(root)? {
        if retiring.previous == previous && retiring.next == *request {
            return Ok(());
        }
        return Err(invalid("mutation retirement changed"));
    }
    let state: FrozenLogCapture = store::read(&root.join(previous.as_str()).join("capture.json"))?
        .ok_or_else(|| invalid("previous mutation capture is absent"))?;
    require_released(&state, &previous)?;
    let retiring = Retirement {
        version: 1,
        previous,
        previous_begin_sha256: state
            .begin
            .input_sha256()
            .map_err(|_| invalid("mutation retirement begin digest"))?,
        next: request.clone(),
        cancelled: false,
    };
    store::write(&root.join("retiring.json"), &retiring)?;
    remove_previous(root, &retiring)
}

pub(super) fn finish(root: &Path, request: &Id) -> StorageResult<()> {
    let Some(retiring) = marker(root)? else {
        return Ok(());
    };
    let active: Option<Id> = store::read(&root.join("active.json"))?;
    if retiring.cancelled || retiring.next != *request || active.as_ref() != Some(request) {
        return Err(invalid("mutation retirement publication"));
    }
    remove_previous(root, &retiring)?;
    clear(root)
}

/// Never-submitted cancellation can end an intent that created no new capture.
pub(super) fn cancel_absent(root: &Path, request: &Id) -> StorageResult<()> {
    let Some(mut retiring) = marker(root)? else {
        return Ok(());
    };
    let active: Option<Id> = store::read(&root.join("active.json"))?;
    if retiring.next != *request || active.as_ref().is_some_and(|id| id != &retiring.previous) {
        return Err(invalid("mutation absent cancellation retirement"));
    }
    retiring.cancelled = true;
    store::write(&root.join("retiring.json"), &retiring)?;
    cancel_finish(root, &retiring, active.as_ref())
}
fn cancel_finish(root: &Path, retiring: &Retirement, active: Option<&Id>) -> StorageResult<()> {
    if !retiring.cancelled || active.is_some_and(|id| id != &retiring.previous) {
        return Err(invalid("mutation retirement cancellation authority"));
    }
    remove_previous(root, retiring)?;
    store::remove_metadata(&root.join("active.json"))?;
    File::open(root)?.sync_all()?;
    clear(root)
}
fn remove_previous(root: &Path, retiring: &Retirement) -> StorageResult<()> {
    let directory = root.join(retiring.previous.as_str());
    if let Some(state) = store::read::<FrozenLogCapture>(&directory.join("capture.json"))? {
        require_released(&state, &retiring.previous)?;
        if state
            .begin
            .input_sha256()
            .map_err(|_| invalid("mutation retirement begin"))?
            != retiring.previous_begin_sha256
        {
            return Err(invalid("mutation retired begin differs"));
        }
    }
    for name in ["capture.json", "capture.pending"] {
        store::remove_metadata(&directory.join(name))?;
    }
    // An unexpected native link or unknown allocation prevents directory removal.
    store::remove_empty(&directory)?;
    File::open(root)?.sync_all()?;
    Ok(())
}
fn require_released(state: &FrozenLogCapture, previous: &Id) -> StorageResult<()> {
    state.validate()?;
    if state.begin.job.commit_id != *previous
        || !state.released
        || state.terminal.is_none() && !state.local_cancelled
    {
        return Err(invalid("previous mutation capture retains its source"));
    }
    Ok(())
}
fn clear(root: &Path) -> StorageResult<()> {
    for name in ["retiring.json", "retiring.pending"] {
        store::remove_metadata(&root.join(name))?;
    }
    File::open(root)?.sync_all()?;
    Ok(())
}
