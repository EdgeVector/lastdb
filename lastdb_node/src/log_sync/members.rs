//! Read the current grant revision with the same signed stream as data reads.

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, MemberListReadResponse, MemberListRevision,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::Id;

use crate::node_read::CanonicalPost;

use super::cloud::CloudLog;
use super::http::Proof;

pub(super) async fn revision(log: &CloudLog) -> Option<u64> {
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
    if !(200..300).contains(&status) {
        return None;
    }
    revision_of(&bytes, &request.stream)
}

fn revision_of(bytes: &[u8], stream: &Stream) -> Option<u64> {
    if let Ok(view) = serde_json::from_slice::<MemberListRevision>(bytes) {
        if view.version != Version::V2 || view.database_id != stream.stream_id {
            return None;
        }
        return Some(view.revision.get());
    }
    let view = serde_json::from_slice::<MemberListReadResponse>(bytes).ok()?;
    if view.version != Version::V2 || view.stream != *stream {
        return None;
    }
    Some(view.revision.get())
}
