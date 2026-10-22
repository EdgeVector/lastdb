//! Bounded same-home compaction and exact-reference reclaim under the writer lease.

mod compact;
mod quote;
mod reclaim;
mod space;
mod stream;
mod visit;

pub use space::{reserve_space, space_usage, SpacePolicy, SpaceReservation, SpaceUsage};
pub use visit::{visit_generation, StoredRecord};

/// Work performed by one complete maintenance call.
#[derive(Default, Debug)]
pub struct MaintenanceReceipt {
    /// Published generation after replacement, or unchanged CURRENT for reclaim only.
    pub generation: u64,
    /// Live records copied without a change to stored bytes or mutation versions.
    pub records_copied: u64,
    /// Tombstones retained at their original versions.
    pub tombstones_copied: u64,
    /// Source and replacement dependency part counts.
    pub parts_before: u64,
    /// Complete replacement part count, at most 65,536.
    pub parts_after: u64,
    /// Verified obsolete file count removed after protection and grace checks.
    pub files_removed: u64,
    /// Exact removed bytes from regular file metadata.
    pub bytes_removed: u64,
    /// Active readers cause deletion to defer, while replacement can still publish.
    pub reclaim_deferred: bool,
    /// Old generations remain after a reclaim that `SpacePolicy::reclaim_generations` bounded.
    pub reclaim_remaining: bool,
}

pub(super) use compact::compact_locked;
pub(super) use reclaim::{reclaim_at, reclaim_locked, sweep_orphans_at, sweep_orphans_locked};
