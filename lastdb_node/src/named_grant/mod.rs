//! Publisher grant and revoke for one named database.
//!
//! DEV and PROD use the same member routes. Success is a service
//! acknowledgement, or a publisher read that already shows the desired row.

mod config;
mod exchange;
mod files;
mod invite;
mod pending;
mod session;
mod successor;

use crate::node_read::{self, PathPostTarget};
use cloud_backup_protocol::v2::share_access::{MemberListReadRequest, SHARE_MEMBER_LIST_READ_PATH};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{Digest, Id};
use session::PublisherSession;
use std::path::Path;

pub(crate) use config::{lock_applied, read_config, set_config, ConfigError, ConfigUpdate};
pub(crate) use invite::{issue_invitation, seal_named_invitation};

/// What the publisher wants the member row to become.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemberIntent {
    /// Active reader. `regrant` restores a revoked key.
    Read { regrant: bool },
    /// Active contributor. `regrant` restores a revoked key.
    Submit { regrant: bool },
    /// Keep the row and mark it revoked.
    Revoke,
}

/// Service acknowledgement of one member row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemberAck {
    /// Stable database id.
    pub(crate) database_id: String,
    /// Stored member-list revision.
    pub(crate) revision: u64,
    /// True when the read already showed this row, so nothing was signed.
    pub(crate) unchanged: bool,
}

/// Grant or revoke one node key. `Ok` means the service has the desired row.
pub(crate) async fn update_member(
    home: &Path,
    selector: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<MemberAck, String> {
    let entry = crate::host::owned::find_shareable(home, selector)?;
    if entry.whole_database() && matches!(intent, MemberIntent::Submit { .. }) {
        return Err("whole-database writers require a configured schema scope".into());
    }
    let _lock = pending::lock(home, &entry.id)?;
    let session = session::open(home).await?;
    if &session.publisher == node {
        return Err("share.members.publisher".into());
    }
    if let Some(saved) = pending::load(home, &entry.id)? {
        return pending::retry(home, &session, &entry.id, node, intent, saved).await;
    }
    let document = exchange::read_document(&session, &entry.id).await?;
    successor::require_stream(&document, &session, &entry.id)?;
    if let Some(ack) = successor::unchanged(&document, &entry.id, node, intent)? {
        return Ok(ack);
    }
    let signed = successor::sign(&session, &document, node, intent)?;
    pending::store(home, &entry.id, node, intent, &signed.body)?;
    exchange::commit(&session, home, &entry.id, &signed).await
}

fn read_body(session: &PublisherSession, database_id: &str) -> Result<Vec<u8>, String> {
    let request = MemberListReadRequest {
        version: Version::V2,
        stream: Stream {
            scope: session.scope.clone(),
            stream_id: Id::try_from(database_id.to_owned())
                .map_err(|_| "database registry is unavailable")?,
        },
    };
    request.validate().map_err(|error| error.field)?;
    serde_json::to_vec(&request).map_err(|_| "cannot encode member list read".into())
}

fn authorization(
    session: &PublisherSession,
    body: &[u8],
    database_id: &str,
    revision: u64,
) -> Result<String, String> {
    let database_id = Id::try_from(database_id.to_owned())
        .map_err(|_| "database registry is unavailable".to_string())?;
    node_read::authorization_post(
        &session.signer,
        PathPostTarget {
            scope: &session.scope,
            database_id: &database_id,
            environment: session.environment,
            path: SHARE_MEMBER_LIST_READ_PATH,
        },
        body,
        revision,
        fold_db::clock::unix_millis(),
    )
}
