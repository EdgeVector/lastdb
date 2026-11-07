//! Cloud backup must not take a source that a local snapshot still owns.
//!
//! The owner route that prepared those snapshots is gone. An unreleased
//! intent file can still exist, and admission refuses it.

mod intent;

use super::{BackupError, Result};
use std::path::Path;

const LOCAL_INTENT_FILE: &str = "logical-snapshot-local-v1.json";

/// Cloud admission must never take over a still-owned local-only source.
pub(super) fn require_inactive(home: &Path) -> Result<()> {
    if intent::load(home)?.is_some_and(|intent| !intent.released) {
        return Err(BackupError::Busy);
    }
    Ok(())
}
