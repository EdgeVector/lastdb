//! Local caller identity primitives.
//!
//! Local access is intentionally coarse: the OS user (UDS peer-cred) plus
//! host-level app consent is the trust boundary. Historical trust-tier,
//! capability-quota, payment, namespace-ACL, audit-log, field-policy, and
//! code-signature *enforcement* layers are gone from query/mutation hot paths.
//!
//! What remains:
//! - [`peer_cred`] — live same-user gate for Unix sockets
//! - [`AccessContext`] / transport / verification — request identity plumbing
//! - [`db_handle`] — `X-LastDB-Db` locator → `storage_prefix` for multi-DB

pub mod db_handle;
pub mod peer_cred;
pub mod types;

pub use db_handle::{
    parse_db_locator, resolve_db_handle_header, storage_prefix_for, DbLocator, LASTDB_DB_HEADER,
};
pub use peer_cred::{process_name, AuditToken, CallerHandle, PeerCredVerdict, PeerCredential};
pub use types::{AccessContext, CallerTransport, CallerVerification};
