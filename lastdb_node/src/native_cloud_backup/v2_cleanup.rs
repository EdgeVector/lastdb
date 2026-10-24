//! Read-only V1 completion guard before a new, independent V2 stream.
use super::{
    model::{CloseIntent, Saved},
    store::{self, Store},
    BackupError, Result,
};
use cloud_backup_protocol::{jobs::JobResult, AccountId, Id};
use std::path::Path;

pub(crate) fn require_complete(home: &Path, account: &AccountId, source_home: &Id) -> Result<()> {
    let Some(id) = Store::active(home)? else {
        return Ok(());
    };
    let root = Store::base(home).join("jobs").join(id.as_str());
    let saved: Saved = store::read(&root.join("state.json"))?.ok_or(BackupError::Invalid)?;
    saved.validate()?;
    if saved.request_id != id || &saved.account != account || &saved.source_home != source_home {
        return Err(BackupError::Identity);
    }
    if saved.retained_source_bytes != Some(0) {
        return Err(BackupError::Busy);
    }
    if saved.locally_closed() {
        let close: CloseIntent =
            store::read(&root.join("close.json"))?.ok_or(BackupError::Invalid)?;
        if close.request_id != id {
            return Err(BackupError::Identity);
        }
    } else {
        let terminal: JobResult =
            store::read(&root.join("terminal.json"))?.ok_or(BackupError::Busy)?;
        saved.validate_terminal(&terminal)?;
    }
    Ok(())
}
