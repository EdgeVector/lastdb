//! Signed-history sync between this node and the relay.
//!
//! Every shared database has one durable log of the entries this node signed
//! and an upload cursor. The sync loop uploads the entries past the cursor and
//! pulls what other nodes signed into a received log. Only the publisher's
//! signature changes records; members propose, and the publisher accepts a
//! proposal or signs that it is not accepted. Grants exist only as signed
//! entries, and the relay is a mailbox that checks them but never decides.

pub(crate) mod birth;
mod chain_author;
pub(crate) mod chain_error;
pub(crate) mod chain_grant;
pub(crate) mod chain_read;
mod config;
mod device;
mod history_store;
mod incorporate;
mod materialize;
mod mirror;
mod proposal;
mod publish_state;
mod pull;
pub(crate) mod received;
mod relay;
mod restore;
mod spawn;
mod upload;
mod wake;

use std::path::Path;

use cloud_backup_protocol::Digest;
use fold_db::schema::types::Schema;

pub use config::relay_base_url;
pub(crate) use history_store::histories_for;
pub(crate) use materialize::{revert_unsigned, store_for as owned_store};
pub(crate) use mirror::{mirror_owned_write, OwnedMutation};
pub(crate) use proposal::{
    status as share_write_status, submit as submit_share_write, ContributionError,
};
pub use relay::hung_relay_must_timeout;
pub use restore::{restore_owned_database, OwnedStream, RestoreOwnedParams, RestoreOwnedReport};
pub use spawn::spawn;
pub use wake::{ensure_wake, publish_signed_history_intent, request_wake};

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
    /// Credentials present but enrollment unfinished.
    Enrolling,
    /// Identity not yet verified with the account service.
    Unverified,
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
