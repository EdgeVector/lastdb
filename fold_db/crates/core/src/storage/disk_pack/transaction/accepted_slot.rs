//! Accepted notes staged on the open native write batch.

use super::PendingBatch;
use crate::storage::disk_backup::accepted::Note;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::{StorageError, StorageResult};
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl PendingBatch {
    pub(in crate::storage) fn stage_accepted_notes(&self, notes: Vec<Note>) -> StorageResult<()> {
        if self.frozen.load(Ordering::Acquire) {
            return Err(StorageError::InvalidOperation(
                "accepted notes require an open write batch".into(),
            ));
        }
        self.accepted.lock().expect("accepted notes").extend(notes);
        Ok(())
    }

    pub(in crate::storage) fn has_accepted_notes(&self) -> bool {
        !self.accepted.lock().expect("accepted notes").is_empty()
    }

    pub(in crate::storage) fn take_accepted_notes(&self) -> Vec<Note> {
        std::mem::take(&mut *self.accepted.lock().expect("accepted notes"))
    }
}

pub(in crate::storage) fn stage_accepted(
    runtime: &PackRuntime,
    notes: Vec<Note>,
) -> StorageResult<()> {
    if notes.is_empty() {
        return Ok(());
    }
    let batch = PendingBatch::current(runtime).ok_or_else(|| {
        StorageError::InvalidOperation("accepted notes require an open write batch".into())
    })?;
    batch.stage_accepted_notes(notes)
}

pub(in crate::storage) fn take_accepted_notes(requests: &[Arc<PendingBatch>]) -> Vec<Note> {
    let mut notes = Vec::new();
    for request in requests {
        notes.extend(request.take_accepted_notes());
    }
    notes
}

pub(in crate::storage) fn has_accepted(requests: &[Arc<PendingBatch>]) -> bool {
    requests.iter().any(|request| request.has_accepted_notes())
}
