//! Signed-history sync: AcceptEntry locally, relay as blind transport.
//!
//! No snapshots, checkpoints, landed field, or manage/access side channel.
//! Grants exist only as signed chain entries. Relays stay grant-blind.

#[allow(dead_code)]
mod apply;
#[allow(dead_code)]
mod bootstrap;
mod config;
mod device;
mod history_store;
mod materialize;
mod pull;
mod relay;
#[allow(dead_code)]
mod restore;
mod spawn;
mod wake;

use cloud_backup_protocol::Digest;
use fold_db::schema::types::Schema;

pub use spawn::spawn;
pub use wake::{ensure_wake, publish_first_backup_intent, request_wake};

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

pub(crate) async fn wait_for_enrollment(host: &crate::host::Host, database_id: &str) -> CloudState {
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

pub(crate) mod received {
    use serde_json::{json, Value};

    /// Self-access view derived from chain grants (side channel removed).
    pub(crate) fn view(_home: &std::path::Path, _database_id: &str) -> Value {
        json!({
            "read": "chain",
            "write": "chain",
            "memberWrites": {},
            "stale": true
        })
    }
}

pub(crate) fn publish_row(_database_id: &str, signed_log: bool) -> serde_json::Value {
    serde_json::json!({
        "signed_log": signed_log,
        "unpublished": []
    })
}

/// Share-write contribution API retired with unsigned mutation log.
#[derive(Debug)]
pub(crate) enum ContributionError {
    Invalid,
    Forbidden,
    Unavailable,
}

impl std::fmt::Display for ContributionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid shared mutation",
            Self::Forbidden => "shared mutation forbidden",
            Self::Unavailable => "shared mutation unavailable",
        })
    }
}

pub(crate) fn submit_share_write(
    _host: &crate::host::Host,
    _share: &crate::registered_share::RegisteredShare,
    _id: &cloud_backup_protocol::Id,
    _body: &[u8],
) -> Result<serde_json::Value, ContributionError> {
    // ExeMem proposal side channel removed; members author signed proposal entries.
    Err(ContributionError::Unavailable)
}

pub(crate) fn share_write_status(
    _host: &crate::host::Host,
    _share: &crate::registered_share::RegisteredShare,
    _mutation_id: &cloud_backup_protocol::Id,
) -> Result<serde_json::Value, ContributionError> {
    Err(ContributionError::Unavailable)
}
