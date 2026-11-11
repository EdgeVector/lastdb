//! Host services used by the generic owner-socket handlers.
//!
//! Mini's `lastdb_node::Host` supplies core database access, identity,
//! and the id generator the daemon injected at boot. Handlers receive an
//! access context and use core's guarded query and mutation paths.

use std::sync::Arc;
use std::time::Duration;

use fold_db::fold_db_core::FoldDB;

use crate::ids::IdGen;

/// The framework-agnostic host surface the shared owner-socket handlers drive.
///
/// Mini holds its core behind `Arc<FoldDB>`.
#[async_trait::async_trait]
pub trait HostNode: Send + Sync {
    /// The live core database — query executor, mutation manager, schema
    /// manager, native index, db-ops. The single handle every shared handler
    /// executes against.
    fn fold_db(&self) -> &Arc<FoldDB>;

    /// The node's public key (base64). Used only by the auto-identity route;
    /// never echoed into a caller-visible error.
    fn public_key(&self) -> String;

    /// Ids this process assigns. Handlers ask this generator; they do not mint.
    fn ids(&self) -> &dyn IdGen;

    /// Wait for write-triggered background side effects that can affect
    /// immediate follow-up reads, bounded by `timeout`.
    async fn wait_for_background_tasks(&self, timeout: Duration) -> bool;
}
