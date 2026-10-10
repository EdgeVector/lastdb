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
use events::pending_key;
pub use events::{AttributionEvent, AttributionEventPage, AttributionPendingScope};

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
const ATTRIBUTION_EVENT_END: &str = "event:v1;";
const ATTRIBUTION_EVENT_MUTATION_PREFIX: &str = "event-mutation:v1:";
const ATTRIBUTION_PENDING_PREFIX: &str = "pending:v1:";
const ATTRIBUTION_VERSION: u8 = 1;
const ATTRIBUTION_OBJECT_DOMAIN: &[u8] = b"lastdb:attribution-object:v1\0";
const ATTRIBUTION_EPOCH_DOMAIN: &[u8] = b"lastdb:attribution-epoch:v1\0";
const ATTRIBUTION_ROOT_DOMAIN: &[u8] = b"lastdb:attribution-root:v1\0";
const ATTRIBUTION_PATH_SET_DOMAIN: &[u8] = b"lastdb:attribution-path-set:v1\0";

/// The physical object plane that an attribution row describes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttributionObjectKind {
    Atom,
    Blob,
    Molecule,
    Tip,
    Protein,
    DerivedIndex,
}

impl AttributionObjectKind {
    fn key(self) -> &'static str {
        match self {
            Self::Atom => "atom",
            Self::Blob => "blob",
            Self::Molecule => "molecule",
            Self::Tip => "tip",
            Self::Protein => "protein",
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

    #[must_use]
    pub fn residue(
        epoch_id: impl Into<String>,
        object_kind: AttributionObjectKind,
        object_id: impl Into<String>,
        source_sequence: u64,
    ) -> Self {
        Self {
            version: ATTRIBUTION_VERSION,
            epoch_id: epoch_id.into(),
            object_kind,
            object_id: object_id.into(),
            classification: AttributionClass::UnattributedResidue,
            root_count: 0,
            path_set_digest: None,
            size: None,
            source_sequence,
        }
    }

    #[must_use]
    pub fn unknown(
        epoch_id: impl Into<String>,
        object_kind: AttributionObjectKind,
        object_id: impl Into<String>,
        source_sequence: u64,
    ) -> Self {
        Self {
            version: ATTRIBUTION_VERSION,
            epoch_id: epoch_id.into(),
            object_kind,
            object_id: object_id.into(),
            classification: AttributionClass::Unknown,
            root_count: 0,
            path_set_digest: None,
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

/// One exact root path, stored outside its target object row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributionPath {
    pub version: u8,
    pub epoch_id: String,
    pub object_kind: AttributionObjectKind,
    pub object_id: String,
    pub root_kind: AttributionRootKind,
    pub root_id: String,
    pub edge_path_digest: String,
}

impl AttributionPath {
    #[must_use]
    pub fn new(
        epoch_id: impl Into<String>,
        object_kind: AttributionObjectKind,
        object_id: impl Into<String>,
        root_kind: AttributionRootKind,
        root_id: impl Into<String>,
        edge_path_digest: impl Into<String>,
    ) -> Self {
        Self {
            version: ATTRIBUTION_VERSION,
            epoch_id: epoch_id.into(),
            object_kind,
            object_id: object_id.into(),
            root_kind,
            root_id: root_id.into(),
            edge_path_digest: edge_path_digest.into(),
        }
    }

    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.version != ATTRIBUTION_VERSION {
            return Err(SchemaError::InvalidData(format!(
                "unsupported attribution path version {}",
                self.version
            )));
        }
        if self.epoch_id.trim().is_empty()
            || self.object_id.trim().is_empty()
            || self.root_id.trim().is_empty()
            || self.edge_path_digest.trim().is_empty()
        {
            return Err(SchemaError::InvalidData(
                "attribution path requires epoch, object, root, and digest".to_string(),
            ));
        }
        Ok(())
    }

    fn storage_key(&self) -> String {
        format!(
            "{ATTRIBUTION_PATH_PREFIX}{}\0{}\0{}\0{}\0{}",
            hash(ATTRIBUTION_EPOCH_DOMAIN, &self.epoch_id),
            self.object_kind.key(),
            hash(ATTRIBUTION_OBJECT_DOMAIN, &self.object_id),
            self.root_kind.key(),
            hash(ATTRIBUTION_ROOT_DOMAIN, &self.root_id),
        )
    }
}

/// Owner-only persistent proof rows. This is not a product schema or atom
/// plane. Copy migration can discard and rebuild the namespace as one unit.
#[derive(Clone)]
pub struct AttributionLedger {
    store: Arc<TypedKvStore<dyn KvStore>>,
    event_tip: Arc<Mutex<u64>>,
}

impl AttributionLedger {
    pub(crate) async fn new(store: Arc<dyn KvStore>) -> Result<Self, StorageError> {
        let store = Arc::new(TypedKvStore::new(store));
        let event_tip = store
            .get_item::<u64>(ATTRIBUTION_EVENT_TIP_KEY)
            .await?
            .unwrap_or(0);
        Ok(Self {
            store,
            event_tip: Arc::new(Mutex::new(event_tip)),
        })
    }

    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.store.inner().flush().await.map_err(Into::into)
    }

    /// Persist one class row. The caller writes root paths first, then this row.
    /// A crash can therefore leave extra path evidence but never classify an
    /// object as attributed without its paths.
    pub async fn put_attribution_record(
        &self,
        record: &AttributionRecord,
    ) -> Result<(), SchemaError> {
        record.validate()?;
        self.store
            .put_item(&record.storage_key(), record)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("put attribution record: {error}")))
    }

    /// Read one classification row by its exact epoch and object identity.
    pub async fn attribution_record(
        &self,
        epoch_id: &str,
        object_kind: AttributionObjectKind,
        object_id: &str,
    ) -> Result<Option<AttributionRecord>, SchemaError> {
        let probe = AttributionRecord::residue(epoch_id, object_kind, object_id, 0);
        self.store
            .get_item(&probe.storage_key())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("get attribution record: {error}")))
    }

    /// Persist one exact root path before its object's classification row.
    pub async fn put_attribution_path(&self, path: &AttributionPath) -> Result<(), SchemaError> {
        path.validate()?;
        self.store
            .put_item(&path.storage_key(), path)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("put attribution path: {error}")))
    }

    /// Add one or more paths, then rebuild the fixed-size object row from the
    /// durable path set. A shared atom therefore keeps every schema path even
    /// when separate molecule pages reach it at different times.
    pub async fn put_attributed_object_paths(
        &self,
        paths: &[AttributionPath],
        source_sequence: u64,
    ) -> Result<AttributionRecord, SchemaError> {
        let Some(first) = paths.first() else {
            return Err(SchemaError::InvalidData(
                "attribution object requires at least one path".to_string(),
            ));
        };
        for path in paths {
            path.validate()?;
            if path.epoch_id != first.epoch_id
                || path.object_kind != first.object_kind
                || path.object_id != first.object_id
            {
                return Err(SchemaError::InvalidData(
                    "attribution paths must target one object".to_string(),
                ));
            }
            self.put_attribution_path(path).await?;
        }

        let stored_paths = self
            .store
            .scan_items_with_prefix::<AttributionPath>(&attribution_path_prefix(
                &first.epoch_id,
                first.object_kind,
                &first.object_id,
            ))
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read attribution paths: {error}"))
            })?;
        let classification =
            classification_for_roots(stored_paths.iter().map(|(_, path)| &path.root_kind));
        let mut path_set: Vec<String> = stored_paths
            .into_iter()
            .map(|(_, path)| {
                format!(
                    "{}\0{}\0{}",
                    path.root_kind.key(),
                    path.root_id,
                    path.edge_path_digest
                )
            })
            .collect();
        path_set.sort();
        path_set.dedup();
        let path_set_digest = hash(ATTRIBUTION_PATH_SET_DOMAIN, &path_set.join("\0"));
        let existing = self
            .attribution_record(&first.epoch_id, first.object_kind, &first.object_id)
            .await?;
        let record = AttributionRecord {
            version: ATTRIBUTION_VERSION,
            epoch_id: first.epoch_id.clone(),
            object_kind: first.object_kind,
            object_id: first.object_id.clone(),
            classification,
            root_count: path_set.len() as u64,
            path_set_digest: Some(path_set_digest),
            size: existing.as_ref().and_then(|record| record.size),
            source_sequence: existing.as_ref().map_or(source_sequence, |record| {
                record.source_sequence.max(source_sequence)
            }),
        };
        self.put_attribution_record(&record).await?;
        Ok(record)
    }

    /// Preserve a missing target as unknown without downgrading an object that
    /// another root already proved reachable.
    pub async fn put_unknown_record_if_absent(
        &self,
        epoch_id: &str,
        object_kind: AttributionObjectKind,
        object_id: &str,
        source_sequence: u64,
    ) -> Result<bool, SchemaError> {
        if self
            .attribution_record(epoch_id, object_kind, object_id)
            .await?
            .is_some()
        {
            return Ok(false);
        }
        self.put_attribution_record(&AttributionRecord::unknown(
            epoch_id,
            object_kind,
            object_id,
            source_sequence,
        ))
        .await?;
        Ok(true)
    }

    /// Test one exact root path without materializing the object path set.
    pub async fn attribution_path_exists(
        &self,
        path: &AttributionPath,
    ) -> Result<bool, SchemaError> {
        path.validate()?;
        self.store
            .exists_item(&path.storage_key())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("probe attribution path: {error}")))
    }

    /// Count every durable classification row by class, plus the total path
    /// row count, across every epoch this ledger holds.
    ///
    /// This is the read surface behind `GET /api/db/inventory`'s attribution
    /// summary: an owner diagnostic over the ledger's own (bounded) namespace,
    /// not a walk of product data. It reports whatever the classification
    /// producers (the schema/system/retention root walk, and the inline
    /// per-write path) have already written; it does not run them itself.
    pub async fn summarize(&self) -> Result<AttributionSummary, SchemaError> {
        let records = self
            .store
            .scan_items_with_prefix::<AttributionRecord>(ATTRIBUTION_RECORD_PREFIX)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("scan attribution records: {error}"))
            })?;
        let mut objects = AttributionClassCounts::default();
        for (_, record) in records {
            match record.classification {
                AttributionClass::SchemaAttributed => objects.schema_attributed += 1,
                AttributionClass::RetentionAttributed => objects.retention_attributed += 1,
                AttributionClass::SystemAttributed => objects.system_attributed += 1,
                AttributionClass::DerivedAttributed => objects.derived_attributed += 1,
                AttributionClass::UnattributedResidue => objects.unattributed_residue += 1,
                AttributionClass::Unknown => objects.unknown += 1,
            }
        }
        let path_rows = self
            .store
            .scan_items_with_prefix::<AttributionPath>(ATTRIBUTION_PATH_PREFIX)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("scan attribution paths: {error}")))?
            .len() as u64;
        Ok(AttributionSummary { objects, path_rows })
    }
}

fn attribution_path_prefix(
    epoch_id: &str,
    object_kind: AttributionObjectKind,
    object_id: &str,
) -> String {
    format!(
        "{ATTRIBUTION_PATH_PREFIX}{}\0{}\0{}\0",
        hash(ATTRIBUTION_EPOCH_DOMAIN, epoch_id),
        object_kind.key(),
        hash(ATTRIBUTION_OBJECT_DOMAIN, object_id),
    )
}

fn hash(domain: &[u8], value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(value.as_bytes());
    hex_lower(hasher.finalize())
}
