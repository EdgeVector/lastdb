//! Read the current grant revision with the same signed stream as data reads.

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, MemberListReadResponse, ShareMemberRole,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{DecimalU64, ErrorCode, Id, ProtocolError};
use serde::Deserialize;

use crate::node_read::CanonicalPost;

use super::cloud::CloudLog;
use super::http::Proof;

pub(super) async fn revision(log: &CloudLog) -> Option<u64> {
    match access(log).await {
        Some(ObservedAccess::Active { revision, .. }) => Some(revision),
        _ => None,
    }
}

pub(super) enum ObservedAccess {
    Active {
        revision: u64,
        role: Option<ShareMemberRole>,
    },
    Revoked,
    Absent,
}

pub(super) async fn access(log: &CloudLog) -> Option<ObservedAccess> {
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
            CanonicalPost::MemberRead.path(),
            canonical_bytes(&request).ok()?,
            Proof::Canonical(CanonicalPost::MemberRead),
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
}

fn active_access(bytes: &[u8], stream: &Stream) -> Option<ObservedAccess> {
    if let Ok(view) = serde_json::from_slice::<AccessRevision>(bytes) {
        if view.version != Version::V2 || view.database_id != stream.stream_id {
            return None;
        }
        return Some(ObservedAccess::Active {
            revision: view.revision.get(),
            role: view.role,
        });
    }
    let view = serde_json::from_slice::<MemberListReadResponse>(bytes).ok()?;
    if view.version != Version::V2 || view.stream != *stream {
        return None;
    }
    Some(ObservedAccess::Active {
        revision: view.revision.get(),
        role: None,
    })
}
