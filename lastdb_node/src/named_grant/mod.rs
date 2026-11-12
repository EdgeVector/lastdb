//! Publisher grant and revoke for one named database.
//!
//! DEV and PROD use the same member routes when Exemem credentials are saved.
//! With no saved credentials, grant and revoke store the node key on this node.
//! Success is a service acknowledgement, a publisher read that already shows
//! the desired row, or the local row written here.

mod exchange;
mod files;
mod invite;
mod local;
mod pending;
mod proof;
mod session;
mod successor;

pub use local::list;

use cloud_backup_protocol::Digest;
use std::path::Path;

pub use invite::seal_named_invitation;

/// What the publisher wants the member row to become.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberIntent {
    /// Active reader. `regrant` restores a revoked key.
    Read { regrant: bool },
    /// Active contributor. `regrant` restores a revoked key.
    Submit { regrant: bool },
    /// Keep the row and mark it revoked.
    Revoke,
}

/// Service acknowledgement of one member row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberAck {
    /// Stable database id.
    pub database_id: String,
    /// Stored member-list revision.
    pub revision: u64,
    /// True when the read already showed this row, so nothing was signed.
    pub unchanged: bool,
    /// True when this node stored the row because no Exemem credentials are saved.
    pub local: bool,
}

/// Grant or revoke one node key. `Ok` means the service has the desired row.
pub async fn update_member(
    home: &Path,
    selector: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<MemberAck, String> {
    let entry = crate::host::owned::find_shareable(home, selector)?;
    match session::open(home).await {
        Ok(session) => cloud_update(home, session, &entry.id, node, intent).await,
        Err(error)
            if error == "share publisher has no saved Exemem credentials"
                && !matches!(intent, MemberIntent::Read { .. }) =>
        {
            local::update(home, &entry.id, node, intent)
        }
        Err(error) => Err(error),
    }
}

async fn cloud_update(
    home: &Path,
    session: session::PublisherSession,
    database_id: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<MemberAck, String> {
    if &session.publisher == node {
        return Err("share.members.publisher".into());
    }
    if let Some(saved) = pending::load(home, database_id)? {
        return pending::retry(home, &session, database_id, node, intent, saved).await;
    }
    let document = exchange::read_document(&session, database_id).await?;
    successor::require_stream(&document, &session, database_id)?;
    if let Some(ack) = successor::unchanged(&document, database_id, node, intent)? {
        return Ok(ack);
    }
    let signed = successor::sign(&session, &document, node, intent)?;
    pending::store(home, database_id, node, intent, &signed.body)?;
    exchange::commit(&session, home, database_id, &signed).await
}
