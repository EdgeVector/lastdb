//! Local caller identity primitives.

pub mod peer_cred;
pub mod types;

pub use peer_cred::{process_name, AuditToken, CallerHandle, PeerCredVerdict, PeerCredential};
pub use types::AccessContext;
