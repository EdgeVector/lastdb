//! The store's narrow interface to the database memory it publishes into.
//!
//! The database owns the warm set, its complete key domains, and the typed
//! effects of each request. The store holds them only through these traits
//! and calls them at fixed points of a read and a commit. It does not read or
//! interpret what they hold.

use super::*;

/// The database memory that a committed root publishes into. The database
/// opens the store with its owner.
pub trait MemoryPublication: Send + Sync {
    /// Drop the memory records that depend on these changed coordinates.
    /// The store calls this under the view write lock, so no capture sees a
    /// new root until these raw records and their epochs agree with the
    /// newly visible complete mutation.
    fn invalidate(&self, changes: &[(String, String)]);

    /// Publish the changed membership keys of a committed root:
    /// `(collection, encoded key, deleted)`. The store calls this under the
    /// view write lock, before it installs the root.
    fn publish_key_changes(&self, changes: &[(String, String, bool)]);

    /// A new owner of one read call's memory holds. The read context keeps
    /// it and drops it after its other fields.
    fn call_holds(&self) -> Arc<dyn std::any::Any + Send + Sync>;

    /// The typed memory effects of one new complete request.
    fn open_batch(&self) -> Box<dyn BatchMemory>;

    /// The database owner, for database code that reads its own memory.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// The typed memory effects of one complete request. The batch keeps them
/// from preparation to completion and calls them in this order: stages,
/// `freeze`, `protect` before acceptance, `publish` under the view write
/// lock, then `release` after the publications finish.
pub trait BatchMemory: Send + Sync {
    /// A later physical stage supersedes the earlier typed facts that depend
    /// on its coordinates. The caller holds the pending data lock.
    fn forget_superseded(&self, operations: &[HomeMutation]);

    /// The retained bytes of the staged typed paths, for the memory quote.
    fn path_bytes(&self) -> u64;

    /// The bytes that the request's raw memory reservation covers, if any.
    fn reserved_bytes(&self) -> Option<u64>;

    /// True when no typed path or definition is staged.
    fn is_empty(&self) -> bool;

    /// Drop bounded preparation scratch when the batch freezes.
    fn freeze(&self);

    /// Reserve the staged typed effects before acceptance. `reserved` is
    /// false when the raw memory reservation does not cover the final quote.
    fn protect(&self, reserved: bool) -> StorageResult<()>;

    /// Publish the reserved clean effects. The caller holds the view write
    /// lock.
    fn publish(&self) -> StorageResult<()>;

    /// Release the reserved paths after the request's publications finish.
    fn release(&self);

    /// The database owner, for database code that stages its own effects.
    fn as_any(&self) -> &dyn std::any::Any;
}
