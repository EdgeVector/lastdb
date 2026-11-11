//! Unix-domain-socket transport and routing for `lastdbd`.
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
