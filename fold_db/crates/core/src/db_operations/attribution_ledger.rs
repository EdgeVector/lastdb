//! Durable, fixed-size attribution rows for owner-only copy migration.
//!
//! The ledger never puts a schema list in an atom. One object row records the
//! final class. Separate target-addressed path rows hold exact root evidence.
//! This keeps one normal atom write independent of the number of schemas that
//! later reach a shared atom.

use crate::hex::hex_lower;
use crate::schema::SchemaError;
use crate::storage::traits::KvStore;
use crate::storage::{StorageError, TypedKvStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

mod events;
pub use events::{AttributionEvent, AttributionPendingScope};

pub const ATTRIBUTION_RECORD_PREFIX: &str = "attr:v1:r:";
pub const ATTRIBUTION_PATH_PREFIX: &str = "attr:v1:p:";

/// The sentinel epoch a caller uses for attribution facts that describe the
/// node's current state rather than one resumable backfill generation: the
/// per-write inline path row (before a mutation response returns) and the
/// on-demand schema/system/retention root walk `GET /api/db/inventory` runs.
/// Distinct from a timestamped backfill `epoch_id` so a live fact never
/// collides with, or is mistaken for, a specific backfill generation's proof.
pub const LIVE_ATTRIBUTION_EPOCH_ID: &str = "attribution:live:v1";
const ATTRIBUTION_EVENT_TIP_KEY: &str = "event-tip:v1";
const ATTRIBUTION_EVENT_PREFIX: &str = "event:v1:";
const ATTRIBUTION_EVENT_MUTATION_PREFIX: &str = "event-mutation:v1:";
const ATTRIBUTION_PENDING_PREFIX: &str = "pending:v1:";
const ATTRIBUTION_VERSION: u8 = 1;
const ATTRIBUTION_OBJECT_DOMAIN: &[u8] = b"lastdb:attribution-object:v1\0";
const ATTRIBUTION_EPOCH_DOMAIN: &[u8] = b"lastdb:attribution-epoch:v1\0";
const ATTRIBUTION_ROOT_DOMAIN: &[u8] = b"lastdb:attribution-root:v1\0";

/// The physical object plane that an attribution row describes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttributionObjectKind {
    Atom,
    Blob,
    Molecule,
    Tip,
    DerivedIndex,
}

impl AttributionObjectKind {
    fn key(self) -> &'static str {
        match self {
            Self::Atom => "atom",
            Self::Blob => "blob",
            Self::Molecule => "molecule",
            Self::Tip => "tip",
            Self::DerivedIndex => "derived-index",
        }
    }
}

/// The final result for one inspected object in one attribution epoch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AttributionClass {
    SchemaAttributed,
    RetentionAttributed,
    SystemAttributed,
    DerivedAttributed,
    UnattributedResidue,
    Unknown,
}

impl AttributionClass {
    fn needs_root(self) -> bool {
        matches!(
            self,
            Self::SchemaAttributed
                | Self::RetentionAttributed
                | Self::SystemAttributed
                | Self::DerivedAttributed
        )
    }
}

/// The root family that supplies one exact path row.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum AttributionRootKind {
    Schema,
    Retention,
    System,
}

/// Decide one object's classification from every root kind that reaches it.
///
/// A live schema path proves the object is reachable from a current schema
/// binding, so it outranks a system or retention root: those exist only to
/// keep an object that a schema no longer reaches out of residue, and must
/// not downgrade an object a schema path already proves live.
#[must_use]
pub fn classification_for_roots<'a>(
    root_kinds: impl IntoIterator<Item = &'a AttributionRootKind>,
) -> AttributionClass {
    let mut has_schema = false;
    let mut has_system = false;
    let mut has_retention = false;
    for kind in root_kinds {
        match kind {
            AttributionRootKind::Schema => has_schema = true,
            AttributionRootKind::System => has_system = true,
            AttributionRootKind::Retention => has_retention = true,
        }
    }
    if has_schema {
        AttributionClass::SchemaAttributed
    } else if has_system {
        AttributionClass::SystemAttributed
    } else if has_retention {
        AttributionClass::RetentionAttributed
    } else {
        AttributionClass::Unknown
    }
}

impl AttributionRootKind {
    fn key(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Retention => "retention",
            Self::System => "system",
        }
    }
}

/// Fixed-size logical accounting attached to an attributed molecule.
///
/// These counters describe the schema-visible contribution of one molecule.
/// They do not claim exclusive physical atom ownership and never authorize
/// reclamation. Shared atoms can contribute to more than one molecule's
/// logical size while they remain one physical object.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionSize {
    pub logical_value_bytes: u64,
    pub structure_bytes: u64,
    pub retained_history_bytes: u64,
}

/// Object counts by classification, read from the durable ledger.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionClassCounts {
    pub schema_attributed: u64,
    pub retention_attributed: u64,
    pub system_attributed: u64,
    pub derived_attributed: u64,
    pub unattributed_residue: u64,
    pub unknown: u64,
}

/// The ledger's whole-namespace attribution summary: object counts by class
/// plus the total path-row count across every epoch.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionSummary {
    pub objects: AttributionClassCounts,
    pub path_rows: u64,
}

/// Fixed-size classification for one object.
///
/// `path_set_digest` commits to the separate path-row set. It is absent only
/// for residue and unknown rows, where no complete path set exists.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionRecord {
    pub version: u8,
    pub epoch_id: String,
    pub object_kind: AttributionObjectKind,
    pub object_id: String,
    pub classification: AttributionClass,
    pub root_count: u64,
    pub path_set_digest: Option<String>,
    /// Present only when this object has a complete logical-size projection.
    /// It is normally set for molecule roots during the catalog walk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<AttributionSize>,
    pub source_sequence: u64,
}

impl AttributionRecord {
    #[must_use]
    pub fn attributed(
        epoch_id: impl Into<String>,
        object_kind: AttributionObjectKind,
        object_id: impl Into<String>,
        classification: AttributionClass,
        root_count: u64,
        path_set_digest: impl Into<String>,
        source_sequence: u64,
    ) -> Self {
        Self {
            version: ATTRIBUTION_VERSION,
            epoch_id: epoch_id.into(),
            object_kind,
            object_id: object_id.into(),
            classification,
            root_count,
            path_set_digest: Some(path_set_digest.into()),
            size: None,
            source_sequence,
        }
    }

    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.version != ATTRIBUTION_VERSION {
            return Err(SchemaError::InvalidData(format!(
                "unsupported attribution record version {}",
                self.version
            )));
        }
        if self.epoch_id.trim().is_empty() || self.object_id.trim().is_empty() {
            return Err(SchemaError::InvalidData(
                "attribution record requires epoch and object identity".to_string(),
            ));
        }
        if self.classification.needs_root()
            && (self.root_count == 0 || self.path_set_digest.as_deref().is_none_or(str::is_empty))
        {
            return Err(SchemaError::InvalidData(
                "attributed object requires root evidence and a path-set digest".to_string(),
            ));
        }
        if matches!(
            self.classification,
            AttributionClass::UnattributedResidue | AttributionClass::Unknown
        ) && (self.root_count != 0 || self.path_set_digest.is_some())
        {
            return Err(SchemaError::InvalidData(
                "residue and unknown objects cannot claim attribution paths".to_string(),
            ));
        }
        Ok(())
    }

    fn storage_key(&self) -> String {
        format!(
            "{ATTRIBUTION_RECORD_PREFIX}{}\0{}\0{}",
            hash(ATTRIBUTION_EPOCH_DOMAIN, &self.epoch_id),
            self.object_kind.key(),
            hash(ATTRIBUTION_OBJECT_DOMAIN, &self.object_id),
        )
    }
}

fn hash(domain: &[u8], value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(value.as_bytes());
    hex_lower(hasher.finalize())
}

/// Owner-only persistent proof rows. This is not a product schema or atom
/// plane. Copy migration can discard and rebuild the namespace as one unit.
#[derive(Clone)]
pub struct AttributionLedger {
    store: Arc<TypedKvStore<dyn KvStore>>,
    event_tip: Arc<Mutex<u64>>,
}

mod path;
pub use path::*;
