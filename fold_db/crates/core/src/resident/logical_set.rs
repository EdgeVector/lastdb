//! Logical resident set — tips, tombstones, and catalog entries a call holds.
//!
//! This set sits beside [`crate::ResidentGraph`]. It does not install tips
//! into that graph, it does not charge `approx_bytes` or `resident_bytes`,
//! and it does not read `LASTDB_LOGICAL_RESIDENT_SET`.
//!
//! A call takes a hold on admit and releases the hold on return. A release
//! does not drop a clean record; the record stays warm. A dirty entry stores
//! a [`DurabilityToken`]. After that token is covered and the owning call
//! has returned, the entry becomes clean and stays warm. The laststore pin
//! is reaped separately when clean and unheld.
//!
//! Atom bodies and proteins are not hold-counted keys. An uncounted reverse
//! link from atom id to the tips that name it drops the body in O(1) when
//! the last tip leaves. An uncounted reverse link from member molecule id
//! to protein id drops the protein when its last resident member leaves.
//! [`RESIDENT_KEY_CAP`] caps used records. A fetch marks that record most
//! recent. Recency is ordered by tick. Over the cap, LRU purge removes the
//! cold end. It skips a held record and a dirty record, and a skip does not
//! touch the tick. [`Self::logical_record_count`] counts each used schema,
//! field, molecule tip, tombstone, and atom.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use super::metrics::ResidentMetrics;
use crate::atom::molecule_uuid::{encode_molecule_uuid_bytes, molecule_uuid_digest};
use crate::schema::types::{Schema, SchemaType};

mod bodies;
mod durability;
mod negative_barrier_cache;
mod purge;
mod records;
mod tips;
mod tombstones;
use negative_barrier_cache::NegativeBarrierCache;

/// Used-record cap for the logical resident set.
///
/// Decision `decision-2026-10-02-warm-set-exact-logical-key-budget`. One
/// fetched schema, field, molecule tip, tombstone, or atom is one used
/// record. The budget is this count, not a byte ledger.
pub const RESIDENT_KEY_CAP: usize = 10_000;

/// Lowers [`RESIDENT_KEY_CAP`] for a test node.
///
/// At 10000 records the purge never runs inside a short proof window on a
/// copy, so a gate cannot see it work. A probe sets this far lower (for
/// example 100) so the purge must run, then checks that
/// `resident_key_count` stays at or under `resident_key_budget` and that
/// `resident_purged_keys` rose. It only lowers: a value of 0 or above the
/// default is refused, and boot stops, so a mistyped limit cannot run a
/// proof on a cap nobody asked for.
pub const RESIDENT_KEY_CAP_ENV: &str = "LASTDB_RESIDENT_KEY_CAP";

static RESIDENT_KEY_CAP_IN_FORCE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Parse the cap override. `None` (unset) is [`RESIDENT_KEY_CAP`].
pub fn parse_resident_key_cap(raw: Option<&str>) -> Result<usize, String> {
    let Some(raw) = raw else {
        return Ok(RESIDENT_KEY_CAP);
    };
    let cap = raw
        .trim()
        .parse::<usize>()
        .map_err(|error| format!("invalid {RESIDENT_KEY_CAP_ENV}={raw:?}: {error}"))?;
    if !(1..=RESIDENT_KEY_CAP).contains(&cap) {
        return Err(format!(
            "{RESIDENT_KEY_CAP_ENV}={cap} is outside [1, {RESIDENT_KEY_CAP}] (it only lowers the cap)"
        ));
    }
    Ok(cap)
}

/// Read [`RESIDENT_KEY_CAP_ENV`] once, at boot. The daemon refuses to start
/// on a bad value.
pub fn init_resident_key_cap_from_env() -> Result<usize, String> {
    let cap = parse_resident_key_cap(std::env::var(RESIDENT_KEY_CAP_ENV).ok().as_deref())?;
    Ok(*RESIDENT_KEY_CAP_IN_FORCE.get_or_init(|| cap))
}

/// The used-record cap in force: [`RESIDENT_KEY_CAP`] until boot ran
/// [`init_resident_key_cap_from_env`] with an override.
#[must_use]
pub fn resident_key_cap() -> usize {
    RESIDENT_KEY_CAP_IN_FORCE
        .get()
        .copied()
        .unwrap_or(RESIDENT_KEY_CAP)
}

/// 32-byte molecule digest. Not a hex spelling and not a group id.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MoleculeId([u8; 32]);

impl MoleculeId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_schema_field(schema: &str, field: &str) -> Self {
        Self(molecule_uuid_digest(schema, field))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Unpadded base64url spelling used in `mk:` storage ids.
    pub fn storage_spelling(&self) -> String {
        encode_molecule_uuid_bytes(&self.0)
    }
}

impl std::fmt::Debug for MoleculeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MoleculeId({:02x?})", &self.0[..4])
    }
}

/// Content id and its storage scope. Hex from `Atom::generate_content_uuid`.
/// The same content id in two scopes names two distinct resident bodies.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AtomId(String, Option<String>);

impl AtomId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into(), None)
    }

    pub fn with_scope(mut self, scope: Option<&str>) -> Self {
        self.1 = scope.map(str::to_string);
        self
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for AtomId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomId")
            .field("uuid", &self.0)
            .field("scope", &self.1)
            .finish()
    }
}

/// Last Store durability token. One `u64`. Not a group id and not a [`ResidentKey`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DurabilityToken(u64);

impl DurabilityToken {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<laststore::DurabilityToken> for DurabilityToken {
    fn from(token: laststore::DurabilityToken) -> Self {
        Self::new(token.as_u64())
    }
}

/// Logical resident lookup. No directory, no shard number, no group id.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ResidentKey {
    Schema(String),
    Field {
        schema: String,
        field: String,
    },
    MoleculeTip {
        molecule: MoleculeId,
        hash: String,
        range: String,
    },
    /// Dirty delete. Not [`super::types::DirtyKey::MoleculeKeyTombstone`].
    Tombstone {
        molecule: MoleculeId,
        hash: String,
        range: String,
    },
    Atom(AtomId),
    Protein(String),
    /// One storage record a point read fetched by its exact id: a catalog
    /// row, a marker, or a known-absent key. It is one used record, like a
    /// tip. It is keyed by the record id, not by a structure type or a group.
    Record {
        collection: String,
        id: String,
    },
}

impl ResidentKey {
    /// False for every logical key. A group id does not fit this enum.
    pub fn names_group(&self) -> bool {
        match self {
            Self::Schema(_)
            | Self::Field { .. }
            | Self::MoleculeTip { .. }
            | Self::Tombstone { .. }
            | Self::Atom(_)
            | Self::Protein(_)
            | Self::Record { .. } => false,
        }
    }
}

impl std::fmt::Debug for ResidentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schema(name) => f.debug_tuple("Schema").field(name).finish(),
            Self::Field { schema, field } => f
                .debug_struct("Field")
                .field("schema", schema)
                .field("field", field)
                .finish(),
            Self::MoleculeTip {
                molecule,
                hash,
                range,
            } => f
                .debug_struct("MoleculeTip")
                .field("molecule", molecule)
                .field("hash", hash)
                .field("range", range)
                .finish(),
            Self::Tombstone {
                molecule,
                hash,
                range,
            } => f
                .debug_struct("Tombstone")
                .field("molecule", molecule)
                .field("hash", hash)
                .field("range", range)
                .finish(),
            Self::Atom(id) => f.debug_tuple("Atom").field(id).finish(),
            Self::Protein(id) => f.debug_tuple("Protein").field(id).finish(),
            Self::Record { collection, id } => f
                .debug_struct("Record")
                .field("collection", collection)
                .field("id", id)
                .finish(),
        }
    }
}

/// One owned molecule tip. The point map stores this record. No byte charge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tip {
    pub atom: AtomId,
    pub written_at: u64,
    pub logical_counter: u64,
    pub device_id: String,
    pub mutation_uuid: String,
}

/// Field catalog entry. Not a counted key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldEntry {
    pub molecule: MoleculeId,
    pub keying: &'static str,
    pub hash_field: Option<String>,
    pub range_field: Option<String>,
}

/// Completeness of one molecule hash's ordered view.
///
/// Not [`super::types::ResidentKeySetCompleteness`] and not
/// [`crate::ResidentGraph::mark_key_set_complete`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HashCompleteness {
    /// No fill and no admitted tip for this hash.
    #[default]
    Absent,
    /// At least one tip is resident. The order is not the full live set.
    Partial,
    /// Every live tip of this hash is in the ordered map, and dirty records
    /// were applied. Evicting one live tip demotes this hash only.
    Complete {
        /// Recency clock at the fill that marked this view complete.
        as_of: u64,
    },
}

/// The in-memory ordered view is not complete. Do not treat it as the order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeNotResident;

impl std::fmt::Display for RangeNotResident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "hash view is not complete")
    }
}

impl std::error::Error for RangeNotResident {}

#[derive(Debug, Default)]
struct HashView {
    completeness: HashCompleteness,
    /// Ranges of live tips. Tombstones are a separate index.
    order: BTreeMap<String, ()>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TipKey {
    molecule: MoleculeId,
    hash: String,
    range: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RecordKey {
    collection: String,
    id: String,
}

/// Stripe count for record write epochs. Fixed, so the epochs do not grow
/// with the number of keys a write touched.
const RECORD_EPOCH_STRIPES: usize = 32;

fn record_stripe(id: &str) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut hasher);
    (hasher.finish() as usize) % RECORD_EPOCH_STRIPES
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct FieldKey {
    schema: String,
    field: String,
}

/// Hold count plus optional dirty token. Not [`super::graph::ResidentGraph`]'s
/// `slot_readers`.
#[derive(Debug)]
struct Held<T> {
    value: T,
    hold: u32,
    token: Option<DurabilityToken>,
}

/// Token, hold, and atom body `delete_resident` takes with a tip.
#[derive(Debug)]
struct UndoDeleteSnapshot {
    hold: u32,
    token: Option<DurabilityToken>,
    atom_body: Option<Vec<u8>>,
    tip_body: Option<Vec<u8>>,
}

impl<T> Held<T> {
    fn admit(value: T) -> Self {
        Self {
            value,
            hold: 1,
            token: None,
        }
    }

    fn admit_dirty(value: T, token: DurabilityToken) -> Self {
        Self {
            value,
            hold: 1,
            token: Some(token),
        }
    }

    fn take_hold(&mut self) {
        self.hold = self.hold.saturating_add(1);
    }

    fn release_hold(&mut self) {
        self.hold = self.hold.saturating_sub(1);
    }

    fn can_leave(&self) -> bool {
        self.hold == 0 && self.token.is_none()
    }
}

/// The newest append for one storage key, and a digest of its stored bytes.
#[derive(Clone, Copy, Debug)]
struct PendingWrite {
    token: DurabilityToken,
    body_digest: u64,
}

/// Owned logical records. The old graph's byte ledger is not this map.
#[derive(Debug, Default)]
pub struct LogicalResidentSet {
    schemas: HashMap<String, Held<Schema>>,
    fields: HashMap<FieldKey, Held<FieldEntry>>,
    proteins: HashMap<String, Held<Vec<MoleculeId>>>,
    /// Uncounted reverse link so a protein drop is O(members), not a scan.
    molecule_proteins: HashMap<MoleculeId, HashSet<String>>,
    tips: HashMap<TipKey, Held<Tip>>,
    molecule_tips: HashMap<MoleculeId, usize>,
    /// Uncounted reverse link so the atom body drops with its last tip.
    atom_tips: HashMap<AtomId, HashSet<TipKey>>,
    atom_bodies: HashMap<AtomId, Vec<u8>>,
    /// Raw storage bytes for a resident tip. KvStore get returns these so a
    /// product point read stays byte-equal with the old LastStore get path.
    tip_bodies: HashMap<TipKey, Vec<u8>>,
    /// Newest append per storage key, for the encrypting layer after put.
    /// The digest names the stored bytes of that append, so only the put
    /// that wrote them can admit its plaintext.
    pending_write_tokens: HashMap<Vec<u8>, PendingWrite>,
    tombstones: HashMap<TipKey, Held<()>>,
    /// Tip token and atom body removed by [`Self::delete_resident`].
    /// [`Self::undo_delete`] restores them. A successful delete drops this
    /// when the tombstone leaves.
    undo_snapshots: HashMap<TipKey, UndoDeleteSnapshot>,
    /// Per-hash ordered molecule view. Completeness is this hash only.
    hash_views: HashMap<(MoleculeId, String), HashView>,
    recency: HashMap<ResidentKey, u64>,
    /// Recency order, least recently used first (tick → key).
    order: BTreeMap<u64, ResidentKey>,
    clock: u64,
    /// Full-flush seal. Tokens at or below this value are covered.
    covered_through: Option<DurabilityToken>,
    /// Exact tokens covered by `WriteAck.covered_by_file_len`.
    covered_exact: HashSet<DurabilityToken>,
    /// Point-fetched storage records that are not a tip or an atom body,
    /// and fetched ids that were absent (`None`). Keyed by the exact
    /// `(collection, id)` a loader read. Each entry is one used record.
    records: HashMap<RecordKey, Option<Vec<u8>>>,
    /// Exact absent Delete markers; separate from the logical-record budget.
    negative_barriers: NegativeBarrierCache,
    /// Write epochs for [`Self::records`], striped by id. A write bumps the
    /// stripe after the store write, so a read that loaded before that write
    /// cannot admit its older bytes afterwards.
    record_epochs: [u64; RECORD_EPOCH_STRIPES],
    /// Product gauges. Absent in unit tests that do not attach one.
    metrics: Option<Arc<ResidentMetrics>>,
    /// Used records a call still holds. O(1) occupancy, not a walk.
    held_used: usize,
    /// Used records with an uncovered durability token.
    dirty_used: usize,
}

impl LogicalResidentSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the process gauges the node reports through `ResidentGraph`.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<ResidentMetrics>) -> Self {
        self.metrics = Some(metrics);
        self.publish_occupancy();
        self
    }

    /// Gauges this set publishes, when a product constructor attached one.
    pub fn metrics(&self) -> Option<&Arc<ResidentMetrics>> {
        self.metrics.as_ref()
    }

    /// Instant occupancy of used, held, and dirty records. O(1).
    fn publish_occupancy(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        metrics.set_logical_occupancy(
            self.logical_record_count() as u64,
            self.held_used as u64,
            self.dirty_used as u64,
        );
        metrics.set_over_cap_keys(
            self.logical_record_count()
                .saturating_sub(resident_key_cap()) as u64,
        );
    }

    fn note_hold_transition(&mut self, before: u32, after: u32) {
        match (before > 0, after > 0) {
            (false, true) => self.held_used = self.held_used.saturating_add(1),
            (true, false) => self.held_used = self.held_used.saturating_sub(1),
            _ => {}
        }
    }

    fn note_dirty_transition(&mut self, before: bool, after: bool) {
        match (before, after) {
            (false, true) => self.dirty_used = self.dirty_used.saturating_add(1),
            (true, false) => self.dirty_used = self.dirty_used.saturating_sub(1),
            _ => {}
        }
    }

    fn note_used_insert(&mut self, hold: u32, dirty: bool) {
        if hold > 0 {
            self.held_used = self.held_used.saturating_add(1);
        }
        if dirty {
            self.dirty_used = self.dirty_used.saturating_add(1);
        }
    }

    fn note_used_remove(&mut self, hold: u32, dirty: bool) {
        if hold > 0 {
            self.held_used = self.held_used.saturating_sub(1);
        }
        if dirty {
            self.dirty_used = self.dirty_used.saturating_sub(1);
        }
    }
}

fn resident_tip_key(key: &TipKey) -> ResidentKey {
    ResidentKey::MoleculeTip {
        molecule: key.molecule,
        hash: key.hash.clone(),
        range: key.range.clone(),
    }
}

fn tip_key(molecule: MoleculeId, hash: &str, range: &str) -> TipKey {
    TipKey {
        molecule,
        hash: hash.to_string(),
        range: range.to_string(),
    }
}

fn field_key(schema: &str, field: &str) -> FieldKey {
    FieldKey {
        schema: schema.to_string(),
        field: field.to_string(),
    }
}

fn keying_of(schema_type: &SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Single => "Single",
        SchemaType::Hash => "Hash",
        SchemaType::Range => "Range",
        SchemaType::HashRange => "HashRange",
    }
}
