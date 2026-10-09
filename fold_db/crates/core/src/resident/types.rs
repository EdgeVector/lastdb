//! Shared types for the resident graph (full ladder).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Kind of object in the resident graph.
///
/// Covers schema → field → molecule → atom, plus protein metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResidentKind {
    Schema,
    /// Molecule index residency (header + tip slots for touched keys).
    /// Alias name: not a disposable request-local FieldVariant.
    Molecule,
    /// Back-compat alias for tip-focused call sites (same metric bucket as Molecule).
    MoleculeTip,
    Atom,
    /// Legacy metric bucket. CAS bytes no longer enter the resident graph.
    FileBlob,
    Protein,
}

impl ResidentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schema => RESIDENT_KIND_SCHEMA,
            Self::Molecule | Self::MoleculeTip => RESIDENT_KIND_MOLECULE,
            Self::Atom => RESIDENT_KIND_ATOM,
            Self::FileBlob => RESIDENT_KIND_FILE_BLOB,
            Self::Protein => RESIDENT_KIND_PROTEIN,
        }
    }

    /// Metric / dirty identity (MoleculeTip collapses into Molecule).
    pub const fn canonical(self) -> Self {
        match self {
            Self::MoleculeTip => Self::Molecule,
            other => other,
        }
    }
}

pub const RESIDENT_KIND_SCHEMA: &str = "schema";
pub const RESIDENT_KIND_MOLECULE: &str = "molecule";
/// Deprecated label kept for external docs; prefer `molecule`.
pub const RESIDENT_KIND_MOLECULE_TIP: &str = "molecule_tip";
pub const RESIDENT_KIND_ATOM: &str = "atom";
pub const RESIDENT_KIND_FILE_BLOB: &str = "file_blob";
pub const RESIDENT_KIND_PROTEIN: &str = "protein";

/// Whether a resolve was served from resident memory or faulted in from disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolveSource {
    /// Already in the resident graph — no disk load.
    ResidentHit,
    /// Loaded from durable store and installed into resident.
    Rehydrated,
}

/// Result of `resolve` / `rehydrate`.
#[derive(Debug, Clone)]
pub struct ResolveOutcome<T> {
    pub value: T,
    pub source: ResolveSource,
}

impl<T> ResolveOutcome<T> {
    pub fn hit(value: T) -> Self {
        Self {
            value,
            source: ResolveSource::ResidentHit,
        }
    }

    pub fn rehydrated(value: T) -> Self {
        Self {
            value,
            source: ResolveSource::Rehydrated,
        }
    }

    pub fn is_hit(&self) -> bool {
        self.source == ResolveSource::ResidentHit
    }

    pub fn is_rehydrated(&self) -> bool {
        self.source == ResolveSource::Rehydrated
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> ResolveOutcome<U> {
        ResolveOutcome {
            value: f(self.value),
            source: self.source,
        }
    }
}

/// Identity of a dirty (or resident) object — used for pin/evict/persist.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DirtyKey {
    Schema(String),
    /// molecule_uuid + storage hash + range (empty range for hash-only).
    MoleculeTip {
        molecule_uuid: String,
        hash: String,
        range: String,
    },
    /// Ordered resident key-set member for one molecule.
    MoleculeKeyIndex {
        molecule_uuid: String,
        hash: String,
        range: String,
    },
    /// Resident tombstone overlay for a deleted molecule key.
    MoleculeKeyTombstone {
        molecule_uuid: String,
        hash: String,
        range: String,
    },
    Atom(String),
    /// Reproducible exact-window query metadata; never part of a persist plan.
    QueryPage(u64),
    Protein(String),
}

impl DirtyKey {
    pub fn kind(&self) -> ResidentKind {
        match self {
            Self::Schema(_) => ResidentKind::Schema,
            Self::MoleculeTip { .. }
            | Self::MoleculeKeyIndex { .. }
            | Self::MoleculeKeyTombstone { .. }
            | Self::QueryPage(_) => ResidentKind::Molecule,
            Self::Atom(_) => ResidentKind::Atom,
            Self::Protein(_) => ResidentKind::Protein,
        }
    }
}

/// Ordered logical key under one molecule.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResidentMoleculeKey {
    pub hash: String,
    pub range: String,
}

impl ResidentMoleculeKey {
    pub fn new(hash: impl Into<String>, range: impl Into<String>) -> Self {
        Self {
            hash: hash.into(),
            range: range.into(),
        }
    }

    pub(crate) fn dirty_key(&self, molecule_uuid: &str) -> DirtyKey {
        DirtyKey::MoleculeKeyIndex {
            molecule_uuid: molecule_uuid.to_string(),
            hash: self.hash.clone(),
            range: self.range.clone(),
        }
    }

    pub(crate) fn tombstone_dirty_key(&self, molecule_uuid: &str) -> DirtyKey {
        DirtyKey::MoleculeKeyTombstone {
            molecule_uuid: molecule_uuid.to_string(),
            hash: self.hash.clone(),
            range: self.range.clone(),
        }
    }

    pub(crate) fn approx_bytes(&self, molecule_uuid: &str) -> u64 {
        PER_ENTRY_OVERHEAD_BYTES + (molecule_uuid.len() + self.hash.len() + self.range.len()) as u64
    }
}

/// Whether resident can answer a molecule key-set enumeration without storage.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ResidentKeySetCompleteness {
    #[default]
    Unknown,
    Complete {
        as_of: u64,
    },
    PartialOverlay {
        inserted: usize,
        deleted: usize,
    },
}

/// Snapshot returned by resident key-set range reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentKeySetSnapshot {
    pub molecule_uuid: String,
    pub completeness: ResidentKeySetCompleteness,
    pub keys: Vec<ResidentMoleculeKey>,
    pub tombstones: Vec<ResidentMoleculeKey>,
}

/// Tip record as it will land on disk (`mk:` logical form).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentTip {
    pub molecule_uuid: String,
    pub hash: String,
    pub range: String,
    pub atom_uuid: String,
    /// Physical part of the mutation author's clock. Legacy resident tips use zero.
    #[serde(default)]
    pub written_at: u64,
    /// Logical part of the mutation author's clock. Legacy resident tips use zero.
    #[serde(default)]
    pub logical_counter: u64,
    /// Device that authored the mutation. Empty on legacy resident tips.
    #[serde(default)]
    pub device_id: String,
    /// Stable mutation identity. Empty on legacy resident tips.
    #[serde(default)]
    pub mutation_uuid: String,
    /// Metadata belongs to this key-to-atom association, not the shared atom.
    #[serde(default)]
    pub key_metadata: Option<crate::atom::KeyMetadata>,
    #[serde(default)]
    pub writer_pubkey: String,
}

impl ResidentTip {
    pub fn dirty_key(&self) -> DirtyKey {
        DirtyKey::MoleculeTip {
            molecule_uuid: self.molecule_uuid.clone(),
            hash: self.hash.clone(),
            range: self.range.clone(),
        }
    }

    /// Approximate resident memory charge (see [`approx_json_bytes`]).
    pub(crate) fn approx_bytes(&self) -> u64 {
        PER_ENTRY_OVERHEAD_BYTES
            // The revision-control map retains a separate slot key and value
            // for this tip. It follows the tip's eviction lifetime.
            + PER_ENTRY_OVERHEAD_BYTES
            + std::mem::size_of::<ResidentSlotControl>() as u64
            + (self.molecule_uuid.len() + self.hash.len() + self.range.len() + 2) as u64
            + (self.molecule_uuid.len()
                + self.hash.len()
                + self.range.len()
                + self.atom_uuid.len()
                + self.device_id.len()
                + self.mutation_uuid.len()
                + self.writer_pubkey.len()) as u64
            + self.key_metadata.as_ref().map_or(0, |meta| {
                meta.source_file_name
                    .as_ref()
                    .map_or(0, |name| name.len() as u64)
                    + meta.metadata.as_ref().map_or(0, |values| {
                        values
                            .iter()
                            .map(|(key, value)| {
                                PER_ENTRY_OVERHEAD_BYTES + (key.len() + value.len()) as u64
                            })
                            .sum()
                    })
            })
            + 16
    }
}

/// Slot identity `(storage_prefix, molecule_uuid, disk_hash, disk_range)`.
///
/// One [`super::ResidentGraph`] is bound to a single LastStore prefix, so the
/// in-graph map key is still `molecule_uuid + hash + range`. The prefix is
/// carried here so later persist-lane work can name the full identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResidentSlotId {
    pub storage_prefix: String,
    pub molecule_uuid: String,
    pub disk_hash: String,
    pub disk_range: String,
}

/// Control state for one resident slot.
///
/// Missing map entry == [`ResidentSlotState::Absent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ResidentSlotState {
    #[default]
    Absent,
    RehydrateFlight,
    Ready,
}

/// Per-slot revision counters and flight/ready flag.
///
/// `resident_revision` advances on every resident apply to the slot.
/// `durable_revision` advances only after the matching persist succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResidentSlotControl {
    pub state: ResidentSlotState,
    pub resident_revision: u64,
    pub durable_revision: u64,
}

/// Atom body as held in resident (logical form for persist).
///
/// Carries full atom fidelity — metadata, source file name, creation time —
/// so a resident hit can reconstruct the domain [`crate::atom::Atom`] and
/// serve read paths without any storage load ([`Self::to_atom`]). Entries
/// installed before these fields existed (or built by write-apply paths that
/// have no timestamp) leave them `None`; `to_atom` then declines and the
/// read falls through to storage — fidelity-safe, never wrong.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResidentAtom {
    pub uuid: String,
    pub source_schema_name: String,
    pub content: Value,
    /// When set, atom points at a file-scale CAS blob (same ref persist will write).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_blob_ref: Option<String>,
    /// Full atom metadata map (superset of `file_blob_ref`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file_name: Option<String>,
    /// Creation time as recorded on disk. `None` = unknown → `to_atom` declines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Partition prefix of the owning slot (`mk:{M}:{esc(hash)}\0`, string
    /// form), when the writer knew it. Under acks-on-resident the T1 persist
    /// sink is the canonical atom writer, so without this the body degrades to
    /// the flat key with no locator — un-migrating the partition-prefix
    /// cutover one write at a time. `None` (entries installed before this
    /// field, or paths with no slot in scope) keeps today's flat placement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_prefix: Option<String>,
}

impl ResidentAtom {
    pub fn dirty_key(&self) -> DirtyKey {
        DirtyKey::Atom(self.uuid.clone())
    }

    /// Full-fidelity projection of a domain atom (the rehydrate direction).
    pub fn from_atom(atom: &crate::atom::Atom) -> Self {
        let metadata = atom.metadata().cloned();
        let file_blob_ref = metadata
            .as_ref()
            .and_then(|m| m.get("file_blob_ref").cloned());
        Self {
            uuid: atom.uuid().to_string(),
            source_schema_name: atom.source_schema_name().to_string(),
            content: atom.content().clone(),
            file_blob_ref,
            metadata,
            source_file_name: atom.source_file_name().cloned(),
            created_at: Some(atom.created_at()),
            partition_prefix: None,
        }
    }

    /// Attach the owning slot's partition so the persist sink can place the
    /// body partition-prefixed (+ locator) instead of at the flat key.
    #[must_use]
    pub fn with_partition(mut self, partition: Option<&crate::atom::AtomPartition>) -> Self {
        self.partition_prefix = partition.map(|p| p.as_str().to_string());
        self
    }

    /// Reconstruct the domain atom without JSON conversion.
    /// Missing creation time means partial fidelity; callers load from storage.
    pub fn to_atom(&self) -> Option<crate::atom::Atom> {
        self.clone().into_atom()
    }

    /// Move an owned resident hit into the domain atom without another content copy.
    pub fn into_atom(self) -> Option<crate::atom::Atom> {
        Some(crate::atom::Atom::from_stored_parts(
            self.uuid,
            self.source_schema_name,
            self.source_file_name,
            self.metadata,
            self.created_at?,
            self.content,
        ))
    }

    /// Approximate resident memory charge (see [`approx_json_bytes`]).
    pub(crate) fn approx_bytes(&self) -> u64 {
        let metadata_bytes: usize = self
            .metadata
            .iter()
            .flatten()
            .map(|(k, v)| k.len() + v.len())
            .sum();
        PER_ENTRY_OVERHEAD_BYTES
            + (self.uuid.len()
                + self.source_schema_name.len()
                + self.file_blob_ref.as_ref().map_or(0, String::len)
                + self.source_file_name.as_ref().map_or(0, String::len)
                + metadata_bytes) as u64
            + approx_json_bytes(&self.content)
    }
}

/// Fixed per-entry charge covering map slot, key strings, and allocator slack.
pub(crate) const PER_ENTRY_OVERHEAD_BYTES: u64 = 64;

/// Flat charge for a resident schema. The catalog is bounded by schema count
/// (dozens, not millions) and `Schema` has no cheap size probe; a flat charge
/// keeps the ledger honest enough without serializing the schema per install.
pub(crate) const APPROX_SCHEMA_BYTES: u64 = 4096;

/// Cheap recursive estimate of a `serde_json::Value`'s in-memory footprint.
///
/// Deliberately approximate: string/number payloads plus a fixed per-node
/// charge. The budget is a pressure valve, not an allocator — consistent
/// under-counting is acceptable, per-apply serialization is not.
pub(crate) fn approx_json_bytes(value: &Value) -> u64 {
    const PER_NODE: u64 = 16;
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => PER_NODE,
        Value::String(s) => PER_NODE + s.len() as u64,
        Value::Array(items) => PER_NODE + items.iter().map(approx_json_bytes).sum::<u64>(),
        Value::Object(map) => {
            PER_NODE
                + map
                    .iter()
                    .map(|(k, v)| PER_NODE + k.len() as u64 + approx_json_bytes(v))
                    .sum::<u64>()
        }
    }
}

/// Snapshot of dirty ladder objects ready to persist — **isomorphic** to resident.
///
/// Fidelity tests compare this plan to a rehydrate-from-plan graph.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistPlan {
    pub schemas: Vec<crate::schema::types::Schema>,
    pub tips: Vec<ResidentTip>,
    pub atoms: Vec<ResidentAtom>,
    // proteins: later
}
