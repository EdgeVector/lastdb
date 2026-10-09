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
    /// Tip versions archived this in-memory session, to persist as `tv:{id}`.
    /// Each entry is `(version_id, tip_snapshot)` where the snapshot is the
    /// prior head (including its own `prev_tip_id` link). Not serialized.
    #[serde(skip)]
    pending_tip_versions: Vec<(String, AtomEntry)>,
    /// Prior tips replaced or removed in this in-memory session.
    ///
    /// The reverse-reference plane needs this transient delta even when the
    /// thin-tip policy does not retain a `tv:` history node. Each tuple is
    /// `(hash, range, old_tip, archived_version_id)`.
    #[serde(skip)]
    pending_replaced_tips: Vec<(String, String, AtomEntry, Option<String>)>,
    /// Whether overwrites archive prior heads into `tv:` history. Mini's
    /// settled thin-tip policy keeps this disabled by default.
    #[serde(skip)]
    tip_history_enabled: bool,
}

mod construct;
mod merge;
mod ops;
mod verify;
