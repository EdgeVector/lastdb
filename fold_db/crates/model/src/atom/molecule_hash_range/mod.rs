//! MoleculeHashRange type for HashRange field semantics
//!
//! Provides a molecule type that combines hash and range functionality
//! for efficient indexing with complex fan-out operations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use super::{AtomEntry, KeyMetadata};

/// A hash-range-based collection of atom references stored in a nested HashMap<BTreeMap> structure.
///
/// This molecule type supports complex indexing where atoms are organized by:
/// - Hash field: Groups related atoms together
/// - Range field: Provides ordered access within each hash group
///
/// Structure: HashMap<hash_value, BTreeMap<range_value, AtomEntry>>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoleculeHashRange {
    /// Unique identifier for this molecule
    uuid: String,
    /// Atom entries organized by hash and range values
    /// Structure: HashMap<hash_value, BTreeMap<range_value, AtomEntry>>
    atom_uuids: HashMap<String, BTreeMap<String, AtomEntry>>,
    /// Timestamp when this molecule was last updated
    updated_at: DateTime<Utc>,
    /// Whether this in-memory molecule is a write-only tail.
    ///
    /// `false` (default) — a full snapshot. A snapshot takes the exclusive
    /// commit guard.
    ///
    /// `true` — hydrated for a write-only O(changed) update. It holds only
    /// the keys this write touched. A full rewrite of a tail molecule is
    /// refused. A tail takes the shared append guard.
    ///
    /// Skipped in serde — it describes this in-memory hydration, not molecule
    /// data. The full-rewrite guard reads this marker. It does not read the
    /// order log.
    #[serde(skip)]
    order_is_tail: bool,
    /// Monotonic version counter, bumped on each actual change
    #[serde(default)]
    version: u64,
    /// Per-key metadata organized by hash and range values
    /// Structure: HashMap<hash_value, BTreeMap<range_value, KeyMetadata>>
    #[serde(default)]
    key_metadata: HashMap<String, BTreeMap<String, KeyMetadata>>,
}

mod compact_key;
mod construct;
pub mod keys;
mod loaded;
mod ops;
mod verify;
