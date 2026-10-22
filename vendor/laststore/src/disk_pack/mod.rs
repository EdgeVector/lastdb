//! Immutable searchable snapshots with direct atomic commits and bounded reads.
//! Native homes and explicit artifacts retain their separate format fences.

mod artifact;
pub mod backup;
mod batch;
mod batch_error;
mod batch_io;
mod block_plan;
mod build;
mod bytes;
mod call;
mod call_bounded;
mod call_io;
pub mod conversion;
pub(crate) mod conversion_fence;
mod conversion_select;
mod current;
mod dependency;
mod frame;
mod generation;
mod hash_range;
pub mod home;
mod key_layout;
pub mod lifetime;
mod location;
mod locator_forest;
pub mod maintenance;
mod object;
mod pack_write;
mod packed_page;
mod page;
mod page_write;
mod private_files;
mod publish;
mod record;
mod selected_batch;
pub mod snapshot;
mod sparse_pages;
mod tree;
mod tree_cursor;
mod tree_edit;
mod tree_output;
mod tree_partition;
mod tree_write;
mod write_lock;

pub use build::{ArtifactBuilder, GroupWriter};
pub use call::{ReadBudget, ReadCall, ReadCosts};
pub use conversion_select::{resolve_conversion, ResolvedStore};
pub use current::Current;
pub use hash_range::{HashRangeCursor, HashScope, RangeBounds, RangePage, RangeRecord};
pub use location::{DirectoryEntry, Location, MutationVersion, Source};
pub use publish::PublishedGeneration;
pub use tree::{TreeKind, TreeRoot};

/// CURRENT feature bit: payload objects have complete immutable object metadata.
/// Older readers refuse this bit before they trust any payload extent.
pub const FEATURE_OBJECT_METADATA: u64 = 1;

/// CURRENT feature bit: a native home descriptor controls placement and key identity.
pub const FEATURE_NATIVE_HOME: u64 = 4;

/// Mandatory protocol: every active reader holds LIFETIME_LOCK or a writer-owned
/// read capability whose in-process gate excludes reclamation.
/// Homes without this bit retain all files because older readers have no such hold.
pub const FEATURE_FILE_LIFETIME: u64 = 8;

/// Native schema writers maintain runtime record-R reverse edges. Version-2
/// completeness is valid only after this bit is durably published in CURRENT.
pub const FEATURE_SCHEMA_RECORD_REFS: u64 = 16;

/// Native batches admit 16,384 ordered storage operations instead of 4,096.
/// The initial CURRENT fences older readers before any larger stored batch.
pub const FEATURE_LARGE_BATCHES: u64 = 32;

/// Locator pages pack complete ordinary keys beside their values.
/// Older readers reject this feature before they can open a packed-key tree.
pub const FEATURE_PACKED_KEYS: u64 = 64;

/// Every atom body is one record at `atom:{uuid}\0b` in the `atoms` collection.
/// A home without this bit used the older flat, partition-prefix, or locator
/// layouts, and a build that holds this bit refuses it. An older build refuses
/// a home with this bit.
pub const FEATURE_ATOM_RECORDS: u64 = 128;

/// Local recovery and reads use the complete immutable snapshot selected by CURRENT.
/// Old journal-backed homes require explicit conversion and are not opened.
pub const FEATURE_SNAPSHOT_COMMIT: u64 = 256;

const SUPPORTED_FEATURES: u64 = FEATURE_OBJECT_METADATA
    | FEATURE_NATIVE_HOME
    | FEATURE_FILE_LIFETIME
    | FEATURE_SCHEMA_RECORD_REFS
    | FEATURE_LARGE_BATCHES
    | FEATURE_PACKED_KEYS
    | FEATURE_ATOM_RECORDS
    | FEATURE_SNAPSHOT_COMMIT;

/// Most dependency parts one published generation can name. A writer that
/// reaches it refuses further writes until maintenance replaces the receipt.
pub const MAX_DEPENDENCY_PARTS: u64 = dependency::MAX_DEPENDENCIES as u64;

const LEGACY_BATCH_OPERATIONS: usize = 4096;
const LARGE_BATCH_OPERATIONS: usize = 16_384;

pub(crate) use lastdb_storage_policy::native_v1::{
    BLOCK_BYTES, MAP_ENTRIES, PAGES_PER_PART, PAGE_BYTES, PART_BYTES,
};
const HEADER_BYTES: usize = 128;
const PAGE_PAYLOAD: usize = PAGE_BYTES - HEADER_BYTES;
const KEY_BYTES: usize = 65_535;
const KEY_DESCRIPTOR: usize = 176;
const VALUE_BYTES: usize = 128;
const LEAF_WIDTH: usize = KEY_DESCRIPTOR + VALUE_BYTES;
const INTERNAL_WIDTH: usize = KEY_DESCRIPTOR + 8;
const LEAF_ENTRIES: usize = 53;
const INTERNAL_ENTRIES: usize = 88;

fn corrupt(reason: &'static str) -> crate::Error {
    crate::Error::Corrupt(format!("disk-pack v1: {reason}"))
}

fn resource() -> crate::Error {
    crate::Error::ReadBudgetExceeded
}

/// Fence artifact-only administrative commands away from native homes.
pub fn require_artifact_path(root: &std::path::Path) -> crate::Result<()> {
    if root.join(home::MARKER).try_exists()? {
        return Err(crate::Error::Config(
            "artifact-only command refuses a native pack home".into(),
        ));
    }
    artifact::require_marker(root)
}
