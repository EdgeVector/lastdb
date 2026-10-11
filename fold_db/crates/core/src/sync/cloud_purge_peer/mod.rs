//! Durable peer interlock for an account's permanent cloud generation.
//!
//! This interlock never invokes `cloud off`: local writes continue to enter
//! the old durable queue while cloud publication is held. The adoption handler
//! retains that queue and installs the exact replacement before an ack is sent.

mod adoption;
pub mod inbound_share;
mod retention;
pub mod scope;
pub mod scope_alias;
mod state;

pub use adoption::{effective_adoption_control, AdoptionHandler, AdoptionPlan, AdoptionReceipt};
pub use retention::RetainedHome;
pub use state::{CloudPurgePeer, PeerAccountStatus};

use crate::sync::error::{SyncError, SyncResult};

fn held(message: impl Into<String>) -> SyncError {
    SyncError::CloudPurgeHeld(message.into())
}

fn sha256(value: &serde_json::Value) -> SyncResult<String> {
    Ok(crate::hex::sha256_hex(&serde_json::to_vec(value)?))
}
