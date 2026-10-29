//! Canonical native cloud wire types. This crate performs no I/O and enables no capability.
//!
//! Both the client and service consume this source. The service binding follows
//! through its existing LastDB submodule after the canonical source lands.
#![warn(missing_docs)]

mod common;
mod digest;
mod error;
pub mod limits;
pub mod manifest;
mod primitives;
pub mod recovery;
pub mod v2;

pub use common::*;
pub use digest::{bytes_sha256, canonical_bytes, input_digest};
pub use error::{ErrorCode, PendingReason, ProtocolError, TerminalFailure, TerminalFailureCode};
pub use manifest::*;
pub use primitives::{AccountId, DecimalU64, Digest, Id, RelativePath, Text, Version};
pub use recovery::*;
