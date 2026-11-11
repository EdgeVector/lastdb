//! Publisher grant and revoke for one named database.
//!
//! DEV and PROD use the same member routes. Success is a service
//! acknowledgement, or a publisher read that already shows the desired row.

mod exchange;
mod files;
mod invite;
mod pending;
mod proof;
mod session;
mod successor;

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
}

/// Grant or revoke one node key. `Ok` means the service has the desired row.
pub async fn update_member(
    home: &Path,
    selector: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<MemberAck, String> {
    let entry = crate::host::owned::find_shareable(home, selector)?;
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
