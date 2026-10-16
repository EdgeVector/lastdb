//! Immutable disk-pack v1 reader and construction primitives.
//!
//! These APIs address an explicitly selected artifact root. They do not activate
//! the format in a LastStore home or bypass the legacy home descriptor fence.

mod artifact;
mod batch;
mod batch_error;
mod batch_io;
mod block_plan;
mod build;
mod bytes;
mod call;
mod call_io;
mod current;
mod dependency;
mod dependency_walk;
mod frame;
mod generation;
mod hash_range;
pub mod home;
pub mod journal;
pub mod lifetime;
mod location;
pub mod maintenance;
mod object;
mod pack_write;
mod page;
mod page_write;
mod publish;
mod record;
mod sparse_pages;
mod stream_checkpoint;
mod tree;
mod tree_cursor;
mod tree_edit;
mod tree_output;
mod tree_write;
mod write_lock;

pub use build::{ArtifactBuilder, GroupWriter};
pub use call::{ReadBudget, ReadCall, ReadCosts};
pub use current::Current;
pub use hash_range::{HashRangeCursor, HashScope, RangeBounds, RangePage, RangeRecord};
pub use location::{DirectoryEntry, Location, MutationVersion, Source};
pub use publish::{publish_artifact_journal, PublishedGeneration};
pub use tree::{TreeKind, TreeRoot};

/// CURRENT feature bit: payload objects have complete immutable object metadata.
/// Older readers refuse this bit before they trust any payload extent.
pub const FEATURE_OBJECT_METADATA: u64 = 1;

/// CURRENT feature bit: no journal admission before a complete stream checkpoint.
/// A previous generation of zero marks initial, unadmitted journal state.
pub const FEATURE_JOURNAL_LIFECYCLE: u64 = 2;

/// CURRENT feature bit: a native home descriptor controls placement and key identity.
pub const FEATURE_NATIVE_HOME: u64 = 4;

/// Mandatory protocol: every active reader holds LIFETIME_LOCK against reclamation.
/// Homes without this bit retain all files because older readers have no such hold.
pub const FEATURE_FILE_LIFETIME: u64 = 8;

const SUPPORTED_FEATURES: u64 = FEATURE_OBJECT_METADATA
    | FEATURE_JOURNAL_LIFECYCLE
    | FEATURE_NATIVE_HOME
    | FEATURE_FILE_LIFETIME;

const PAGE_BYTES: usize = 16_384;
const HEADER_BYTES: usize = 128;
const PAGE_PAYLOAD: usize = PAGE_BYTES - HEADER_BYTES;
const BLOCK_BYTES: usize = 4_096;
const PART_BYTES: u64 = 8_388_608;
const KEY_BYTES: usize = 65_535;
const KEY_DESCRIPTOR: usize = 176;
const VALUE_BYTES: usize = 128;
const LEAF_WIDTH: usize = KEY_DESCRIPTOR + VALUE_BYTES;
const INTERNAL_WIDTH: usize = KEY_DESCRIPTOR + 8;
const LEAF_ENTRIES: usize = 53;
const INTERNAL_ENTRIES: usize = 88;
const MAP_ENTRIES: u64 = 677;

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
