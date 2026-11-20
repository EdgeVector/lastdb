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
mod materialize;
mod proposal;
mod pull;
pub(crate) mod received;
mod relay;
mod restore;
mod spawn;
mod wake;

use cloud_backup_protocol::Digest;
use fold_db::schema::types::Schema;

pub use config::{relay_base_url, store_relay_base_url};
pub(crate) use history_store::histories_for;
pub(crate) use proposal::{
    status as share_write_status, submit as submit_share_write, ContributionError,
};
pub use restore::{restore_owned_database, OwnedStream, RestoreOwnedParams, RestoreOwnedReport};
pub use spawn::spawn;
pub use wake::{ensure_wake, publish_signed_history_intent, request_wake};

/// No-op: snapshot native recovery is retired. Restore uses signed history.
pub fn install_prepared_native_recovery(_home: &std::path::Path) -> Result<bool, String> {
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

pub(crate) fn database_cloud_state(home: &std::path::Path, _database_id: &str) -> CloudState {
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

/// Direct member writes are authorized only via AcceptEntry grant window.
pub(crate) fn direct_write_allowed(
    _host: &crate::host::Host,
    _share: &crate::registered_share::RegisteredShare,
) -> bool {
    // DESIGN: until local grant state is folded into share listing, allow the
    // write path to attempt AcceptEntry; unauthorized authors are rejected there.
    true
}

pub(crate) fn note_receiver(
    _database_id: &str,
    _store: &std::sync::Arc<fold_db::fold_db_core::FoldDB>,
) {
}

pub(crate) fn publish_row(_database_id: &str, signed_log: bool) -> serde_json::Value {
    serde_json::json!({
        "signed_log": signed_log,
        "unpublished": []
    })
}
