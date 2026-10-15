//! Immutable disk-pack v1 reader and construction primitives.
//!
//! These APIs address an explicitly selected artifact root. They do not activate
//! the format in a LastStore home or bypass the legacy home descriptor fence.

mod artifact;
mod build;
mod bytes;
mod call;
mod current;
mod dependency;
mod dependency_walk;
mod frame;
mod generation;
pub mod journal;
mod location;
mod object;
mod pack_write;
mod page;
mod page_write;
mod publish;
mod record;
mod sparse_pages;
mod tree;
mod tree_edit;
mod tree_output;
mod tree_write;

pub use build::{ArtifactBuilder, GroupWriter};
pub use call::{ReadBudget, ReadCall, ReadCosts};
pub use current::Current;
pub use location::{DirectoryEntry, Location, MutationVersion, Source};
pub use publish::{publish_artifact_journal, PublishedGeneration};
pub use tree::{TreeKind, TreeRoot};

/// CURRENT feature bit: payload objects have complete immutable object metadata.
/// Older readers refuse this bit before they trust any payload extent.
pub const FEATURE_OBJECT_METADATA: u64 = 1;

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
