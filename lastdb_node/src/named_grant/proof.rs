//! Member-list read body. The `LastDBNode` header is `node_read`, not a second signer.

use super::session::PublisherSession;
use crate::node_read::{self, CanonicalPost, SignedPostTarget};
use cloud_backup_protocol::v2::share_access::MemberListReadRequest;
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::Id;

pub(super) fn read_body(session: &PublisherSession, database_id: &str) -> Result<Vec<u8>, String> {
    let request = MemberListReadRequest {
        version: Version::V2,
        stream: stream(session, database_id)?,
    };
    request.validate().map_err(|error| error.field)?;
    serde_json::to_vec(&request).map_err(|_| "cannot encode member list read".into())
}

/// Sign `body` exactly. Do not serialize the request again before the post.
pub(super) fn authorization(
    session: &PublisherSession,
    body: &[u8],
    database_id: &str,
    revision: u64,
) -> Result<String, String> {
    let database_id = Id::try_from(database_id.to_owned())
        .map_err(|_| "database registry is unavailable".to_string())?;
    node_read::authorization(
        &session.signer,
        SignedPostTarget {
            scope: &session.scope,
            database_id: &database_id,
            environment: session.environment,
            route: CanonicalPost::MemberRead,
        },
        body,
        revision,
        fold_db::clock::unix_millis(),
    )
}

fn stream(session: &PublisherSession, database_id: &str) -> Result<Stream, String> {
    Ok(Stream {
        scope: session.scope.clone(),
        stream_id: Id::try_from(database_id.to_owned())
            .map_err(|_| "database registry is unavailable")?,
    })
}
