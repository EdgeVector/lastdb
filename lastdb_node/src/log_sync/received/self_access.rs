//! The stored view of this node's access to a received database.
//!
//! The pull loop records it after the member-list read and the settings get.
//! A database list reads it back as `selfAccess`.

use std::path::{Path, PathBuf};

use cloud_backup_protocol::v2::share_access::{
    ShareMemberRole, ShareRead, ShareWrite, ShareWriteScope,
};
use cloud_backup_protocol::DecimalU64;
use serde::{Deserialize, Serialize};

use crate::profile::private_fs as disk;

use super::ObservedAccess;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccessView {
    state: AccessState,
    role: Option<ShareMemberRole>,
    #[serde(default = "super::share_settings::default_read")]
    read: ShareRead,
    #[serde(default = "super::share_settings::default_write")]
    write: ShareWrite,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    write_schemas: Option<ShareWriteScope>,
    revision: Option<DecimalU64>,
    confirmed_at_ms: Option<DecimalU64>,
    stale: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AccessState {
    Unknown,
    Active,
    Revoked,
    Absent,
}

impl AccessView {
    fn unknown() -> Self {
        Self {
            state: AccessState::Unknown,
            role: None,
            read: ShareRead::Members,
            write: ShareWrite::Proposals,
            write_schemas: None,
            revision: None,
            confirmed_at_ms: None,
            stale: true,
        }
    }
}

pub(crate) fn view(home: &Path, id: &str) -> AccessView {
    let mut result = disk::read_limited(&self_access_path(home, id), super::MEMBER_LIMIT)
        .ok()
        .flatten()
        .and_then(|bytes| serde_json::from_slice::<AccessView>(&bytes).ok())
        .unwrap_or_else(AccessView::unknown);
    let now = fold_db::clock::unix_millis();
    result.stale |= result
        .confirmed_at_ms
        .is_none_or(|stamp| now.checked_sub(stamp.get()).is_none_or(|age| age > 60_000));
    result
}

pub(super) fn record(
    home: &Path,
    id: &str,
    observed: Option<&ObservedAccess>,
    settings: Option<(ShareRead, ShareWrite)>,
) {
    let mut previous = view(home, id);
    let result = match observed {
        Some(observed) => confirmed(observed, settings, &previous),
        None if previous.stale => return,
        None => {
            previous.stale = true;
            save_access(home, id, &previous);
            return;
        }
    };
    if previous.state == result.state
        && previous.role == result.role
        && previous.read == result.read
        && previous.write == result.write
        && previous.revision == result.revision
        && previous.write_schemas == result.write_schemas
        && previous.stale == result.stale
        && previous
            .confirmed_at_ms
            .zip(result.confirmed_at_ms)
            .is_some_and(|(a, b)| b.get().checked_sub(a.get()).is_some_and(|age| age < 10_000))
    {
        return;
    }
    save_access(home, id, &result);
}

fn save_access(home: &Path, id: &str, result: &AccessView) {
    if let Ok(bytes) = serde_json::to_vec(result) {
        let _ = disk::write_private(&self_access_path(home, id), &bytes);
    }
}

fn confirmed(
    observed: &ObservedAccess,
    settings: Option<(ShareRead, ShareWrite)>,
    previous: &AccessView,
) -> AccessView {
    let (read, write) = settings.unwrap_or((previous.read, previous.write));
    let (state, role, revision, write_schemas) = match observed {
        ObservedAccess::Active {
            revision,
            role,
            write_schemas,
        } => (
            AccessState::Active,
            *role,
            Some(DecimalU64::new(*revision)),
            write_schemas.clone(),
        ),
        ObservedAccess::Revoked => (AccessState::Revoked, None, None, None),
        ObservedAccess::Absent => (AccessState::Absent, None, None, None),
    };
    AccessView {
        state,
        role,
        read,
        write,
        revision,
        write_schemas,
        confirmed_at_ms: Some(DecimalU64::new(fold_db::clock::unix_millis())),
        stale: false,
    }
}

fn self_access_path(home: &Path, id: &str) -> PathBuf {
    home.join("log-sync").join(id).join("self-access.json")
}
