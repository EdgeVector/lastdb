//! # Last Store
//!
//! Multi-collection **local document store** for embedded / personal-cloud
//! apps (including [LastDB](https://thelastdb.com) Storage v2).
//!
//! - **Collections** of opaque documents (`put` / `get` / `delete` by id)
//! - **Segment files** on disk with real **`compact`** (space returns to the OS)
//! - **Group-commit** durability (batch writes, explicit [`LastStore::flush`])
//!
//! ```no_run
//! use laststore::{LastStore, collections};
//!
//! let store = LastStore::open("/tmp/my-last-store")?;
//! store.put(collections::ATOMS, "a1", br#"{"v":1}"#)?;
//! store.put(collections::TIPS, "t1", b"a1")?;
//! store.flush()?;
//! assert_eq!(store.get(collections::ATOMS, "a1")?.as_deref(), Some(br#"{"v":1}"#.as_slice()));
//! # Ok::<(), laststore::Error>(())
//! ```
//!
//! Crate name on crates.io / Cargo: **`laststore`**. Product name: **Last Store**.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod durability;
mod error;
pub mod frame;
mod keysidecar;
mod options;
mod placed_write;
mod read_activity;
mod segfmt;
mod sorted;
mod sorted_file;
mod sorted_merge;
mod store;

pub use error::{Error, Result};
pub use options::{
    CaptureHook, CollectionPolicy, HashAlgo, HashGroupKey, LastStoreOptions, LayoutMode,
    PackagingMode, SealHook,
};
pub use placed_write::{observe_placed_writes, PlacedWrite};
pub use read_activity::{observe_read_activity, ReadActivity};
/// Record limits the segment format enforces. Exported so callers that build
/// ids from their own data can check a write **before** taking a destructive
/// step that assumes it will succeed.
pub use segfmt::{MAX_BODY_LEN, MAX_ID_LEN};
pub use store::{
    describe_home, home_has_frame_aead_segments, read_retire_receipt, restore_chunk_cache_paths,
    set_pressure_shed_interactive_override, with_admit_code, with_schema_warm_owner,
    AllGroupsPurpose, CaptureEvent, CaptureOp, CollectionResidue, DroppedGroupReport,
    DurabilityToken, FrameCompressionStats, GroupResidue, HashGroupPlacement, HashGroupWarmStats,
    LastStore, LayoutDescriptor, LayoutMigrationReport, LoadedPoint, LoadedTip, MaintenanceReport,
    PhysicalRangeCursor, PhysicalRangePage, RetiredCompactGate, RetiredGroupId, SealedChunkMeta,
    ShardKey, Snapshot, TxnOp, VersionRetentionReport, WarmEvictionReport, WriteAck,
    COMPACT_REWRITE_PEAK_BUDGET_BYTES, RETIRED_GROUPS_RECEIPT_FILE,
    SUPERSEDED_VERSION_RETENTION_NANOS,
};

/// Convenience alias used by some internal callers.
pub type Store = LastStore;

/// Canonical LastDB Storage v2 collection names (optional; any string works).
pub mod collections {
    /// Schema / catalog documents.
    pub const SCHEMAS: &str = "schemas";
    /// Immutable value documents.
    pub const ATOMS: &str = "atoms";
    /// Current pointers (thin tips) → hop to atoms.
    pub const TIPS: &str = "tips";
}

/// Crate version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
