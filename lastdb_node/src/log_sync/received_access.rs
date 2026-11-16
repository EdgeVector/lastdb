//! Signed member-list revision reads and recipient self-access observations.

use std::path::{Path, PathBuf};

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, MemberListReadResponse, ShareMemberRole, ShareWriteScope,
    SHARE_MEMBER_LIST_READ_PATH,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{DecimalU64, ErrorCode, Id, ProtocolError};
use serde::{Deserialize, Serialize};

use super::cloud::CloudLog;
use super::disk;
use super::http::Proof;

pub(super) enum ObservedAccess {
    Active {
        revision: u64,
        role: Option<ShareMemberRole>,
        write_schemas: Option<ShareWriteScope>,
    },
    Revoked,
    Absent,
}

pub(super) async fn revision(log: &CloudLog) -> Option<u64> {
    match read_member_access(log, Proof::Data).await {
        Some(ObservedAccess::Active { revision, .. }) => Some(revision),
        _ => None,
    }
}

pub(super) async fn access(log: &CloudLog) -> Option<ObservedAccess> {
    read_member_access(log, Proof::SelfAccess).await
}

async fn read_member_access(log: &CloudLog, proof: Proof) -> Option<ObservedAccess> {
    let request = MemberListReadRequest {
        version: Version::V2,
        stream: Stream {
            scope: log.scope.clone(),
            stream_id: Id::try_from(log.database_id().to_owned()).ok()?,
        },
    };
    request.validate().ok()?;
    let (status, bytes) = log
        .post_status(
            SHARE_MEMBER_LIST_READ_PATH,
            canonical_bytes(&request).ok()?,
            proof,
        )
        .await
        .ok()?;
    if (200..300).contains(&status) {
        return active_access(&bytes, &request.stream);
    }
    let refusal: ProtocolError = serde_json::from_slice(&bytes).ok()?;
    match (refusal.code, refusal.field.as_str()) {
        (ErrorCode::Unauthorized, "share.grant.revoked") => Some(ObservedAccess::Revoked),
        (ErrorCode::Unauthorized, "share.grant.absent") => Some(ObservedAccess::Absent),
        _ => None,
    }
}

#[derive(Deserialize)]
struct AccessRevision {
    version: Version,
    database_id: Id,
    revision: DecimalU64,
    role: Option<ShareMemberRole>,
    write_schemas: Option<ShareWriteScope>,
}

fn active_access(bytes: &[u8], stream: &Stream) -> Option<ObservedAccess> {
    if let Ok(view) = serde_json::from_slice::<AccessRevision>(bytes) {
        if view.version != Version::V2 || view.database_id != stream.stream_id {
            return None;
        }
        return Some(ObservedAccess::Active {
            revision: view.revision.get(),
            role: view.role,
            write_schemas: view.write_schemas,
        });
    }
    let view = serde_json::from_slice::<MemberListReadResponse>(bytes).ok()?;
    if view.version != Version::V2 || view.stream != *stream {
        return None;
    }
    Some(ObservedAccess::Active {
        revision: view.revision.get(),
        role: None,
        write_schemas: None,
    })
}

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

    pub(crate) fn can_submit(&self) -> bool {
        !self.stale && self.state == AccessState::Active
    }
}

fn path(home: &Path, id: &str) -> PathBuf {
    home.join("log-sync").join(id).join("self-access.json")
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
    let mut previous = view(home, id);
    let result = match observed {
        Some(observed) => confirmed(observed),
        None if previous.stale => return,
        None => {
            previous.stale = true;
            save(home, id, &previous);
            return;
        }
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
    save(home, id, &result);
}

fn save(home: &Path, id: &str, result: &AccessView) {
    if let Ok(bytes) = serde_json::to_vec(result) {
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
