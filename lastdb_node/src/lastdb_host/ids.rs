//! Id port for owner-socket mutations.
//!
//! Same shape as the registry core's `IdGen`: one fresh version-4 UUID. The
//! host does not depend on the schema registry, and this crate does not read
//! OS entropy. The daemon constructs an adapter and passes it into the host.

use uuid::Uuid;

/// Random ids injected by the process that boots the host.
pub trait IdGen: Send + Sync {
    /// Fresh version-4 UUID.
    fn fresh_uuid_v4(&self) -> Uuid;
}
