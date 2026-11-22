//! Signed-history sync: AcceptEntry locally, relay as blind transport.
//!
//! No snapshots, checkpoints, landed field, or manage/access side channel.
//! Grants exist only as signed chain entries. Relays gate access from publisher-signed grant entries and never run AcceptEntry.

#[allow(dead_code)]
mod apply;
pub(crate) mod birth;
#[allow(dead_code)]
mod bootstrap;
mod chain_author;
pub(crate) mod chain_error;
pub(crate) mod chain_grant;
pub(crate) mod chain_read;
mod config;
mod device;
mod history_store;
mod incorporate;
mod materialize;
mod member_write;
mod mirror;
mod outbox_upload;
mod proposal;
mod publish_state;
mod pull;
pub(crate) mod received;
mod relay;
mod restore;
mod spawn;
mod wake;

use std::path::Path;

use cloud_backup_protocol::Digest;
use fold_db::schema::types::Schema;
use mutation_log::{NodeKey, Role};

pub use config::{relay_base_url, store_relay_base_url};
pub(crate) use history_store::histories_for;
pub(crate) use member_write::mirror_received_write;
pub(crate) use mirror::{mirror_owned_write, OwnedMutation};
pub(crate) use proposal::{
    status as share_write_status, submit as submit_share_write, ContributionError,
};
pub use relay::hung_relay_must_timeout;
pub use restore::{restore_owned_database, OwnedStream, RestoreOwnedParams, RestoreOwnedReport};
pub use spawn::spawn;
pub use wake::{ensure_wake, publish_signed_history_intent, request_wake};

/// No-op: snapshot native recovery is retired. Restore uses signed history.
pub fn install_prepared_native_recovery(_home: &Path) -> Result<bool, String> {
    Ok(false)
}

/// Catalog identity digest for a declared schema (unchanged helper).
pub(crate) fn schema_id(schema: &Schema) -> Result<Digest, String> {
    let id = schema
        .identity_hash
        .as_deref()
        .ok_or("declared schema has no catalog identity")?;
    let portable = schema_types::PortableSchemaId::from_catalog(id, schema.to_wire())?;
    Digest::try_from(portable.as_str().to_owned()).map_err(|e| e.to_string())
}

/// Enrollment wait used by share routes until People owns that surface.
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
    // DESIGN: ExeMem enrollment phases are account-service concerns; Phase 1
    // treats any saved credentials as enrolled for relay-sync gating.
    CloudState::Enrolled
}

pub(crate) fn wait_for_enrollment(host: &crate::host::Host, database_id: &str) -> CloudState {
    database_cloud_state(&host.home, database_id)
}

/// Direct `/v1/write` on a received database: only a folded Write role.
///
/// Matches the ExeMem relay `may_write` gate. No history or no Write role
/// keeps the existing 403 read-only path.
pub(crate) fn direct_write_allowed(
    host: &crate::host::Host,
    share: &crate::registered_share::RegisteredShare,
) -> bool {
    let node = NodeKey::from_bytes(host.keypair.public_key_bytes());
    histories_for(&host.home)
        .peek_with(share.id.as_str(), |history| {
            received::role_of(history, node) == Some(Role::Write)
        })
        .unwrap_or(false)
}

/// Record outbox pending for a received database the caller already opened.
pub(crate) fn note_receiver(home: &Path, database_id: &str) {
    let pending = outbox_len(home, database_id);
    publish_state::note_pending(database_id, pending);
}

pub(crate) fn publish_row(database_id: &str, signed_log: bool) -> serde_json::Value {
    publish_state::row(database_id, signed_log)
}

fn outbox_len(home: &Path, database_id: &str) -> u64 {
    if let Some(n) = histories_for(home).peek_with(database_id, |history| history.outbox().len()) {
        return n as u64;
    }
    history_store::load_outbox_entries(home, database_id).map_or(0, |entries| entries.len() as u64)
}
