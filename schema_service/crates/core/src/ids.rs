//! Id port for the registry core.
//!
//! Core does not read OS entropy. The shell constructs an adapter and passes
//! it into the state constructors. Declaration ids, near-miss registration
//! ids, and mutation-gate challenge ids stay version-4 UUIDs.

use uuid::Uuid;

/// Random ids injected by the process shell.
pub trait IdGen: Send + Sync {
    /// Fresh version-4 UUID.
    fn fresh_uuid_v4(&self) -> Uuid;
}
