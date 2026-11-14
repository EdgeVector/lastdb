//! Recipient-only observations from the signed member read already used by sync.

use std::path::{Path, PathBuf};

use cloud_backup_protocol::{
    v2::share_access::{ShareMemberRole, ShareWriteScope},
    DecimalU64,
};
use serde::{Deserialize, Serialize};

use super::{disk, members::ObservedAccess};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccessView {
    state: AccessState,
    role: Option<ShareMemberRole>,
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
            write_schemas: None,
            revision: None,
            confirmed_at_ms: None,
            stale: true,
        }
    }
}

fn path(home: &Path, id: &str) -> PathBuf {
    home.join("log-sync").join(id).join("self-access.json")
}

impl AccessView {
    pub(crate) fn can_write(&self) -> bool {
        !self.stale
            && self.state == AccessState::Active
            && self.role == Some(ShareMemberRole::Submit)
            && self.write_schemas.is_some()
    }
}

pub(crate) fn view(home: &Path, id: &str) -> AccessView {
    let mut result = disk::read_limited(&path(home, id), 65_536)
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

pub(super) fn record(home: &Path, id: &str, observed: Option<&ObservedAccess>) {
    let previous = view(home, id);
    let result = match observed {
        Some(observed) => confirmed(observed),
        None if previous.stale => return,
        None => AccessView {
            stale: true,
            ..previous
        },
    };
    if previous.state == result.state
        && previous.role == result.role
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
    if let Ok(bytes) = serde_json::to_vec(&result) {
        let _ = disk::write_private(&path(home, id), &bytes);
    }
}

fn confirmed(observed: &ObservedAccess) -> AccessView {
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
        revision,
        write_schemas,
        confirmed_at_ms: Some(DecimalU64::new(fold_db::clock::unix_millis())),
        stale: false,
    }
}
