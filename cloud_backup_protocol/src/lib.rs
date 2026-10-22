//! Canonical native cloud wire types. This crate performs no I/O and enables no capability.
//!
//! Both the client and service consume this source. The service binding follows
//! through its existing LastDB submodule after the canonical source lands.

pub mod access;
mod common;
mod digest;
pub mod discovery;
mod error;
pub mod holds;
pub mod limits;
pub mod logical;
pub mod manifest;
mod primitives;
pub mod publication;
pub mod recovery;
pub mod references;
pub mod transfer;

pub use common::*;
pub use digest::{bytes_sha256, canonical_bytes, input_digest};
pub use error::{ErrorCode, ProtocolError};
pub use manifest::*;
pub use primitives::{AccountId, DecimalU64, Digest, Id, RelativePath, Text, Version};
pub use recovery::*;

/// Types alone do not constitute an implemented client or service capability.
pub const NATIVE_BACKUP_AVAILABLE: bool = false;
pub const LOGICAL_SYNC_AVAILABLE: bool = false;
pub const CLOUD_GC_AVAILABLE: bool = false;
pub const JOURNAL_SUFFIX_AVAILABLE: bool = false;
