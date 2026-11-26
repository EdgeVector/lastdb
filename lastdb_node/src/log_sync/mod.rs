//! Signed-history sync between this node and the relay.
//!
//! Every shared database has one durable log of the entries this node signed
//! and an upload cursor. The sync loop uploads the entries past the cursor and
//! pulls what other nodes signed into a received log. Only the publisher's
//! signature changes records; members propose, and the publisher accepts a
//! proposal or signs that it is not accepted. Grants exist only as signed
//! entries, and the relay is a mailbox that checks them but never decides.
//!
//! The personal database is the same log with no grants. Its owner is the only
//! publisher, every device of the owner signs its own stream with the one node
//! key, and every payload is sealed before it is signed, so the relay cannot
//! read it. Its entries come from the spool of accepted writes and are applied
//! through the store with the last-writer stamp, with no history in memory and
//! no log files: see `personal`. The copy on the relay is the backup, and
//! reading it is how another device catches up.

pub(crate) mod birth;
mod blob_transfer;
mod chain_author;
pub(crate) mod chain_error;
pub(crate) mod chain_grant;
pub(crate) mod chain_read;
mod device;
mod history_store;
mod incorporate;
mod lease_member;
mod lease_publish;
mod lease_resign;
pub(crate) mod lease_status;
mod materialize;
mod mirror;
mod personal;
mod proposal;
mod publish_state;
mod pull;
pub(crate) mod received;
mod relay;
mod spawn;
mod upload;
pub(crate) mod usage;
mod wake;

use std::path::Path;

use cloud_backup_protocol::Digest;
use fold_db::schema::types::Schema;

pub use crate::cloud::{explicit_relay_url, relay_base_url};
pub(crate) use history_store::histories_for;
pub(crate) use lease_member::{may_write_direct, mirror_received as mirror_received_write};
pub(crate) use materialize::{revert_unsigned, store_for as owned_store};
pub(crate) use mirror::{mirror_owned_write, OwnedMutation};
pub use personal::{
    install_newest_snapshot, restore_personal_database, RestoreReport, SnapshotBase,
};
pub(crate) use personal::{
    known_snapshot, last_reclaim, reclaim_status, request_reclaim, request_snapshot,
    snapshot_status,
};
pub(crate) use proposal::{
    has_submitted as share_write_submitted, status as share_write_status,
    submit as submit_share_write, ContributionError,
};
pub use relay::hung_relay_must_timeout;
pub use spawn::spawn;
pub(crate) use wake::wake_loop;
pub use wake::{publish_signed_history_intent, request_wake};

/// An owned database that a write names.
pub(crate) struct WriteTarget {
    /// Its 64-hex id.
    pub(crate) id: String,
    /// True for the personal database, whose entries are sealed.
    pub(crate) personal: bool,
}

/// The owned database a write names by id or by name, or the personal database
/// when it names none. `None` for a database this node does not own.
pub(crate) fn write_target(
    host: &crate::host::Host,
    selector: Option<&str>,
) -> Option<WriteTarget> {
    let owner = host.owned.owner().ok()?;
    if selector.is_none_or(|name| name == owner.id || owner.name.as_deref() == Some(name)) {
        return Some(WriteTarget {
            id: owner.id,
            personal: true,
        });
    }
    let selector = selector?;
    host.owned
        .owned_entries()
        .ok()?
        .into_iter()
        .find(|entry| entry.id == selector || entry.name.as_deref() == Some(selector))
        .map(|entry| WriteTarget {
            id: entry.id,
            personal: false,
        })
}

/// Catalog identity digest for a declared schema.
pub(crate) fn schema_id(schema: &Schema) -> Result<Digest, String> {
    let id = schema
        .identity_hash
        .as_deref()
        .ok_or("declared schema has no catalog identity")?;
    let portable = schema_types::PortableSchemaId::from_catalog(id, schema.to_wire())?;
    Digest::try_from(portable.as_str().to_owned()).map_err(|e| e.to_string())
}

/// How far this node's cloud enrollment has got, as the share routes see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloudState {
    /// No cloud credentials on disk.
    NoAccount,
    /// Ready for signed-history sync when a relay URL is configured.
    Enrolled,
}

pub(crate) fn database_cloud_state(home: &Path, _database_id: &str) -> CloudState {
    if !crate::host::has_cloud_credentials(home) {
        return CloudState::NoAccount;
    }
    // Saved credentials count as enrolled; this check does not ask the
    // account service.
    CloudState::Enrolled
}

pub(crate) fn wait_for_enrollment(host: &crate::host::Host, database_id: &str) -> CloudState {
    database_cloud_state(&host.home, database_id)
}

/// Record the pending count for a received database the caller already opened.
pub(crate) fn note_receiver(home: &Path, database_id: &str) {
    let pending = pending_uploads(home, database_id);
    publish_state::note_pending(database_id, pending);
}

pub(crate) fn publish_row(database_id: &str, signed_log: bool) -> serde_json::Value {
    publish_state::row(database_id, signed_log)
}

fn pending_uploads(home: &Path, database_id: &str) -> u64 {
    history_store::pending_count(home, database_id).unwrap_or(0)
}
