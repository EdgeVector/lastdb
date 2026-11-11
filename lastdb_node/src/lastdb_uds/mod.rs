//! Unix-domain-socket transport for the LastDB Mini daemon (`lastdb_node`).
//!
//! The daemon (`lastdbd`) uses these modules for its owner socket.
//! The former desktop node (`fold_db_node`) was removed in the Mini-only
//! cutover; it no longer re-exports this crate.
//!
//! What lives here is transport + routing only:
//! - [`worker_pool`] — **primary** concurrency control: bounded workers + queue
//!   with backpressure (CPU-shaped; not “max connections”).
//! - [`uds`] — socket bind, `0o600` perms, same-user peer-credential gate,
//!   EMFILE-aware accept classification.
//! - [`uds_http`] — the minimal HTTP/1.1 framing used on the
//!   socket (no actix, no TLS — kernel peer creds are the transport auth).
//! - [`uds_router`] — the control-socket route classifier and dispatcher with
//!   its hardcoded data-route allowlist ([`uds_router::DataRoute`]).
//!
//! The data-route executor lives in `lastdb_node`, which supplies
//! [`uds_router::dispatch`]'s `execute_data` callback.
#![warn(missing_docs)]

pub mod uds;
pub mod uds_http;
pub mod uds_router;
pub mod worker_pool;

pub use worker_pool::{SubmitError, UdsPoolSnapshot, UdsWorkerPool};

/// List databases on the current node.
pub const DATABASES_PATH: &str = "/api/databases";
/// Prefix that routes an operation to a selected database.
pub const DATABASE_SCOPE_PREFIX: &str = "/api/databases/";

/// Check the bounded literal alphabet before putting a selector in a path.
pub fn valid_selector(selector: &str) -> bool {
    !selector.is_empty()
        && selector.len() <= 64
        && selector
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Return the selector and ordinary API path suffix, if present.
pub fn split_database_path(target: &str) -> Option<(&str, &str)> {
    target
        .strip_prefix(DATABASE_SCOPE_PREFIX)?
        .split_once('/')
        .filter(|(selector, _)| valid_selector(selector))
}

/// Database-scoped targets on the owner socket.
pub mod database_path {
    pub use crate::lastdb_uds::{
        split_database_path as split, valid_selector, DATABASES_PATH, DATABASE_SCOPE_PREFIX,
    };
}
