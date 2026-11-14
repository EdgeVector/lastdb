//! Canonical native cloud wire types. This crate performs no I/O and enables no capability.
//!
//! Both the client and service consume this source. The service binding follows
//! through its existing LastDB submodule after the canonical source lands.
#![warn(missing_docs)]

mod common;
mod digest;
mod error;
/// Request size bound re-exported from [`v2::limits`].
pub mod limits {
    /// Maximum canonical JSON bytes of a request.
    pub use crate::v2::limits::CONTROL_JSON_MAX_BYTES as REQUEST_JSON_MAX_BYTES;
}
mod primitives;
pub mod recovery;
pub mod v2;

pub use common::*;
pub use digest::{bytes_sha256, canonical_bytes, input_digest};
pub use error::{ErrorCode, PendingReason, ProtocolError, TerminalFailure, TerminalFailureCode};
pub use primitives::{AccountId, DecimalU64, Digest, Id, RelativePath, Text};
pub use recovery::*;
