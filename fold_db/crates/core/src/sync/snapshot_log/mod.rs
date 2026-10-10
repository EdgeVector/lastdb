//! Cloud sync v1 object model: mutation log + snapshot frontier F + CAS latest.
//!
//! Design (LOCKED): brain `design-lastdb-cloud-sync-snapshot-log`
//!
//! Pure types + pure helpers — no network I/O. Continuous publisher and
//! restore code map LastStore sealed files / backup manifests onto this model:
//!
//! - Snapshot body S ≈ backup manifest (content-addressed sealed chunks)
//! - Frontier F (v1) ≈ scalar `cut_csn` (or log through_id)
//! - CAS `latest` → `{ snapshot_id, F, counter }`
//! - GC of logs fully covered by F **only after** successful CAS
//!
//! Multi-writer door: log segment identity and frontier types leave room for
//! per-writer streams and vector F without rewriting the v1 shape.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Schema version for wire objects under this model.
pub const SNAPSHOT_LOG_MODEL_VERSION: u32 = 1;

/// Single-publisher (v1) or multi-writer (v2 door) incorporated frontier.
///
/// v1 uses [`Frontier::Scalar`]. v2 may use [`Frontier::Vector`] without
/// changing snapshot/latest field names — F remains an explicit field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Frontier {
    /// Last included seq / CSN / log id for the single authoritative stream.
    Scalar { through: u64 },
    /// Per-writer incorporated through-ids (multi-writer door; not required for v1 PASS).
    Vector { through: BTreeMap<String, u64> },
}

impl Frontier {
    pub fn scalar(through: u64) -> Self {
        Self::Scalar { through }
    }

    /// Multi-writer published / incorporated F: `{ writer_id → through_seq }`.
    pub fn vector(through: BTreeMap<String, u64>) -> Self {
        Self::Vector { through }
    }

    /// Build vector F from a per-writer HWM map (empty → scalar 0 for wire simplicity
    /// is **not** done here — callers that want scalar use [`Self::scalar`]).
    pub fn from_writer_hwm(through: impl IntoIterator<Item = (String, u64)>) -> Self {
        Self::Vector {
            through: through.into_iter().collect(),
        }
    }

    /// Scalar through-id when this is a v1 frontier; `None` for pure vector form.
    pub fn as_scalar_through(&self) -> Option<u64> {
        match self {
            Self::Scalar { through } => Some(*through),
            Self::Vector { through } if through.len() == 1 => through.values().next().copied(),
            Self::Vector { .. } => None,
        }
    }

    /// Max through across writers (scalar max of vector F; identity for scalar).
    pub fn max_through(&self) -> u64 {
        match self {
            Self::Scalar { through } => *through,
            Self::Vector { through } => through.values().copied().max().unwrap_or(0),
        }
    }

    /// Per-writer map view. Scalar becomes a single empty-key entry only when
    /// `writer_id` is not provided — prefer [`Self::as_writer_hwm`] with a known id.
    pub fn as_writer_hwm(&self, local_writer_id: Option<&str>) -> BTreeMap<String, u64> {
        match self {
            Self::Scalar { through } => {
                let mut m = BTreeMap::new();
                if *through > 0 || local_writer_id.is_some() {
                    let key = local_writer_id.unwrap_or("").to_string();
                    m.insert(key, *through);
                }
                m
            }
            Self::Vector { through } => through.clone(),
        }
    }

    /// Whether log id `log_through` on optional `writer_id` is fully covered by F.
    ///
    /// v1: compares against scalar through.  
    /// v2 door: if `writer_id` is set, uses that writer's vector entry; missing
    /// writer means not covered.
    pub fn covers_log(&self, writer_id: Option<&str>, log_through: u64) -> bool {
        match self {
            Self::Scalar { through } => log_through <= *through,
            Self::Vector { through } => {
                let key = writer_id.unwrap_or("");
                through.get(key).is_some_and(|t| log_through <= *t)
            }
        }
    }
}

/// Identity of one append-only mutation log segment object.
///
/// Legacy objects use flat or writer-scoped paths. Typed mutation objects use
/// `log/{writer_id}/{schema}/{utc_nanos}_{sequence}.enc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationLogSegmentId {
    /// Optional publisher/device id for multi-writer streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_id: Option<String>,
    /// Declared catalog schema for schema-folder mutation logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    /// T0 UTC nanosecond stamp carried in the object name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utc_nanos: Option<u64>,
    /// Sequence in this writer-schema stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Monotonic id within the stream (seq / CSN / log object id).
    pub through_id: u64,
    /// Object key relative to the account/org prefix (informational / routing).
    pub object_key: String,
}

impl MutationLogSegmentId {
    pub fn single_publisher(through_id: u64, object_key: impl Into<String>) -> Self {
        Self {
            writer_id: None,
            schema_name: None,
            utc_nanos: None,
            sequence: None,
            through_id,
            object_key: object_key.into(),
        }
    }

    /// Object-key path the Cloud PUT must mint.
    ///
    /// Writer-scoped legacy path. Flat `log/{through_id}.enc` is also readable.
    pub fn default_object_key(writer_id: Option<&str>, through_id: u64) -> String {
        match writer_id {
            Some(w) if !w.is_empty() => format!("log/{w}/{through_id}.enc"),
            _ => format!("log/{through_id}.enc"),
        }
    }

    pub fn schema_folder(
        writer_id: impl Into<String>,
        schema_name: impl Into<String>,
        utc_nanos: u64,
        sequence: u64,
        through_id: u64,
    ) -> Self {
        let writer_id = writer_id.into();
        let schema_name = schema_name.into();
        let object_key = Self::schema_object_key(&writer_id, &schema_name, utc_nanos, sequence);
        Self {
            writer_id: Some(writer_id),
            schema_name: Some(schema_name),
            utc_nanos: Some(utc_nanos),
            sequence: Some(sequence),
            through_id,
            object_key,
        }
    }

    pub fn schema_object_key(
        writer_id: &str,
        schema_name: &str,
        utc_nanos: u64,
        sequence: u64,
    ) -> String {
        format!("log/{writer_id}/{schema_name}/{utc_nanos:019}_{sequence}.enc")
    }

    pub fn expected_object_key(&self) -> String {
        match (
            self.writer_id.as_deref(),
            self.schema_name.as_deref(),
            self.utc_nanos,
            self.sequence,
        ) {
            (Some(writer), Some(schema), Some(utc_nanos), Some(sequence)) => {
                Self::schema_object_key(writer, schema, utc_nanos, sequence)
            }
            (writer, _, _, _) => Self::default_object_key(writer, self.through_id),
        }
    }
}

/// Snapshot record that pins an incorporated frontier F.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub model_version: u32,
    /// Content id of snapshot body S (e.g. backup manifest sha256).
    pub snapshot_id: String,
    /// Explicit incorporated frontier — never omitted.
    pub frontier: Frontier,
    /// Device that built S (single publisher in v1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher_id: Option<String>,
}

impl SnapshotRecord {
    pub fn v1(snapshot_id: impl Into<String>, through: u64) -> Self {
        Self {
            model_version: SNAPSHOT_LOG_MODEL_VERSION,
            snapshot_id: snapshot_id.into(),
            frontier: Frontier::scalar(through),
            publisher_id: None,
        }
    }
}

/// CAS payload for the sole atomic publish step: `latest` → (S, F, counter).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestCasPayload {
    pub model_version: u32,
    pub snapshot_id: String,
    pub frontier: Frontier,
    /// Monotonic publish counter (CAS reject if not strictly greater).
    pub counter: u64,
    /// Optional store identity for LastStore backup alignment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
}

impl LatestCasPayload {
    pub fn v1(snapshot_id: impl Into<String>, through: u64, counter: u64) -> Self {
        Self {
            model_version: SNAPSHOT_LOG_MODEL_VERSION,
            snapshot_id: snapshot_id.into(),
            frontier: Frontier::scalar(through),
            counter,
            store_uuid: None,
            epoch: None,
        }
    }

    /// Build from a LastStore backup cut (S = manifest sha, F = cut_csn).
    pub fn from_backup_cut(
        manifest_sha256: impl Into<String>,
        cut_csn: u64,
        counter: u64,
        store_uuid: impl Into<String>,
        epoch: u64,
    ) -> Self {
        Self {
            model_version: SNAPSHOT_LOG_MODEL_VERSION,
            snapshot_id: manifest_sha256.into(),
            frontier: Frontier::scalar(cut_csn),
            counter,
            store_uuid: Some(store_uuid.into()),
            epoch: Some(epoch),
        }
    }

    /// Whether `candidate` may replace `current` under single-publisher CAS rules.
    pub fn cas_allows_replace(current: Option<&Self>, candidate: &Self) -> bool {
        match current {
            None => candidate.counter >= 1,
            Some(cur) => candidate.counter > cur.counter,
        }
    }
}

/// Publish pipeline state for GC ordering: never delete logs before CAS succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishPhase {
    /// Building S / uploading sealed units; logs must not be GC'd.
    Building,
    /// CAS of latest in flight or failed; logs must not be GC'd.
    Publishing,
    /// CAS succeeded; logs fully ≤ F may become GC-eligible after grace.
    Published,
}

/// GC eligibility decision for one log segment relative to a published frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcEligibility {
    /// Safe to delete after grace (covered by F and CAS published).
    EligibleAfterGrace,
    /// Still needed for live = apply(logs > F) or CAS not done.
    Retain,
}

/// Pure GC rule: logs fully covered by F are GC-eligible **only after** CAS publish.
///
/// Never GC on CAS failure. Never GC logs that extend past F.
pub fn gc_eligibility_for_log(
    phase: PublishPhase,
    published_frontier: Option<&Frontier>,
    segment: &MutationLogSegmentId,
) -> GcEligibility {
    if phase != PublishPhase::Published {
        return GcEligibility::Retain;
    }
    let Some(f) = published_frontier else {
        return GcEligibility::Retain;
    };
    if f.covers_log(segment.writer_id.as_deref(), segment.through_id) {
        GcEligibility::EligibleAfterGrace
    } else {
        GcEligibility::Retain
    }
}
