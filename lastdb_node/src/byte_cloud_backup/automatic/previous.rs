//! Retire the automatic backup baseline of the previous policy version.
//!
//! A policy of version 2 was saved by a program whose personal backups
//! carried the CAS home. Its setting stays exactly: enabled, revision,
//! cadence and connection. Its backup baseline cannot continue under the
//! new format, so it is discarded: the backed-up frontiers, the pending job,
//! the local recovery point, the active job marker and the snapshot capture
//! of that job. The next run takes a full snapshot at once. Nothing is
//! converted, and a failure leaves the version 2 file in place, so the next
//! tick retries; backup is never turned off here.

use super::*;
use crate::byte_cloud_backup::store::AUTOMATIC_FILE as FILE;
use fold_db::storage::disk_pack::PackRuntime;
use serde::Deserialize;
use std::path::Path;

pub(super) const PREVIOUS_VERSION: u32 = 2;

#[derive(Deserialize)]
pub(super) struct Version {
    pub version: u32,
}

/// The setting fields of a version 2 policy. Unknown fields are ignored.
#[derive(Deserialize)]
struct Previous {
    account: cloud_backup_protocol::AccountId,
    source_home: Id,
    endpoint: String,
    credential_sha256: cloud_backup_protocol::Digest,
    state: PreviousState,
    #[serde(default)]
    pending: Option<PreviousPending>,
}

#[derive(Deserialize)]
struct PreviousState {
    enabled: bool,
    revision: u64,
    log_interval_seconds: u64,
    snapshot_interval_seconds: u64,
}

#[derive(Deserialize)]
struct PreviousPending {
    id: Id,
}

/// Replace a version 2 policy file with the same setting and no baseline.
/// Each step repeats safely; the new policy is written last.
pub(super) fn retire(home: &Path, runtime: &PackRuntime) -> Result<()> {
    let base = Store::base(home);
    let Some(previous) = store::read::<Previous>(&base.join(FILE))? else {
        return Ok(());
    };
    let dropped = previous
        .pending
        .map(|pending| pending.id.as_str().to_owned());
    if !runtime.is_dedicated_shared_home() {
        runtime.retire_previous_backup_capture(dropped.as_deref())?;
    }
    for name in ["recovery.json", "active.json"] {
        store::remove_owned(&base.join(name))?;
    }
    let mut state = Policy::empty_state(
        previous.state.log_interval_seconds,
        previous.state.snapshot_interval_seconds,
    );
    state.enabled = previous.state.enabled;
    // A higher revision binds no job of the previous program.
    state.revision = previous
        .state
        .revision
        .checked_add(1)
        .ok_or(BackupError::Limit)?;
    state.snapshot_reason = Some("backup format upgrade".to_owned());
    let policy = Policy::new(
        previous.account,
        previous.source_home,
        previous.endpoint,
        previous.credential_sha256,
        state,
    );
    Store::write_configuration(home, FILE, &policy)
}
