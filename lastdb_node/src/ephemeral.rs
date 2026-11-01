//! Detection and isolation for synthetic/ephemeral nodes (CoW clones).
//!
//! A synthetic or ephemeral node is a copy-on-write clone of an existing node
//! home, used for safe-upgrade probes and other temporary operations. Ephemeral
//! nodes must not write to the shared schema catalog, preserving the isolation
//! guarantee safe-upgrade probes and other ephemeral-node tooling depend on.
//!
//! Ephemeral status is signaled by the `LASTDB_EPHEMERAL` environment variable
//! (any non-empty value). When set, the node refuses catalog-mutating
//! operations (schema sync/registration and the DB catalog membership routes)
//! so the shared/primary catalog never sees a write from a throwaway clone.

/// Whether this node is running as an ephemeral/synthetic instance.
///
/// Ephemeral nodes are CoW clones or other temporary instances that must not
/// persist catalog writes to the shared catalog. This prevents isolation
/// violations when an ephemeral node boots and tries to register schemas.
pub fn is_ephemeral() -> bool {
    std::env::var_os("LASTDB_EPHEMERAL").is_some()
}
