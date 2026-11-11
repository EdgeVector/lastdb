//! Host services used by the generic owner-socket handlers.
//!
//! Mini's `lastdb_node::Host` supplies core database access, identity,
//! and the id generator the daemon injected at boot. Handlers receive an
//! access context and use core's guarded query and mutation paths.

use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;

use crate::ids::IdGen;

/// The framework-agnostic host surface the shared owner-socket handlers drive.
///
/// Mini holds its core behind `Arc<FoldDB>`.
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
}
