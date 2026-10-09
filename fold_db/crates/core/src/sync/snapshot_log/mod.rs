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

use super::log::LogEntry;
use crate::clock::unix_millis;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

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

    /// Multi-writer head: CAS `latest` carries vector F (optional S pointer).
    ///
    /// Continuous log-first sync advances F without requiring a new full-home
    /// snapshot; `snapshot_id` may be empty when only F moves.
    pub fn with_vector_f(
        snapshot_id: impl Into<String>,
        writer_hwm: BTreeMap<String, u64>,
        counter: u64,
    ) -> Self {
        Self {
            model_version: SNAPSHOT_LOG_MODEL_VERSION,
            snapshot_id: snapshot_id.into(),
            frontier: Frontier::vector(writer_hwm),
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

/// Encode helpers (real serde path — used by unit tests and callers).
pub fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|e| format!("snapshot_log encode: {e}"))
}

pub fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("snapshot_log decode: {e}"))
}

const PIN_MODE_LOG_MAGIC: &[u8; 8] = b"LPMLOG1\n";
const PIN_MODE_LOG_HEADER_LEN: usize = PIN_MODE_LOG_MAGIC.len() + 4 + 32;

/// Sync-target identity stamped into each durable pin-mode mutation record.
///
/// The log file should be scoped per database / sync target. The target fields
/// are still stored in every record so restore/proof code can reject accidental
/// cross-target replay instead of treating one node-global file as valid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinModeLogTarget {
    /// Stable database / sync-target id (for example `personal` or an org id).
    pub target_id: String,
    /// Cloud/object prefix owned by this target. Org targets carry the org
    /// prefix, not a private personal user prefix.
    pub cloud_prefix: String,
    /// Non-secret identifier of the content key used to seal this target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_key_id: Option<String>,
}

impl PinModeLogTarget {
    pub fn new(target_id: impl Into<String>, cloud_prefix: impl Into<String>) -> Self {
        Self {
            target_id: target_id.into(),
            cloud_prefix: cloud_prefix.into(),
            content_key_id: None,
        }
    }

    pub fn with_content_key_id(mut self, content_key_id: impl Into<String>) -> Self {
        self.content_key_id = Some(content_key_id.into());
        self
    }
}

/// One post-F0 committed mutation persisted by pin mode before acknowledge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinModeMutationRecord {
    pub model_version: u32,
    pub target: PinModeLogTarget,
    /// Writer/device id. Required even in v1 so vector frontiers can be added
    /// without rewriting the durable format.
    pub writer_id: String,
    /// Monotonic per-writer durable sequence.
    pub seq: u64,
    /// Frontier covered after this record is durable.
    pub durable_frontier: Frontier,
    /// Commit timestamp supplied by the writer, milliseconds since epoch.
    pub committed_at_ms: u64,
    /// Existing cloud-sync log entry shape reused for replay into restore/proof
    /// code. Its `seq` remains the operation's logical sequence; `seq` above is
    /// the pin-mode durable stream sequence.
    pub entry: LogEntry,
}

impl PinModeMutationRecord {
    pub fn new(
        target: PinModeLogTarget,
        writer_id: impl Into<String>,
        seq: u64,
        entry: LogEntry,
    ) -> Self {
        Self {
            model_version: SNAPSHOT_LOG_MODEL_VERSION,
            target,
            writer_id: writer_id.into(),
            seq,
            durable_frontier: Frontier::scalar(seq),
            committed_at_ms: entry.timestamp_ms,
            entry,
        }
    }
}

/// Replay/status view over a target-scoped durable pin-mode log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinModeMutationLogStatus {
    pub target_id: String,
    pub records: usize,
    pub bytes: u64,
    pub trailing_partial_bytes: u64,
    pub last_durable_seq: Option<u64>,
    pub last_durable_frontier: Option<Frontier>,
    pub oldest_record_age_secs: Option<u64>,
}

impl PinModeMutationLogStatus {
    fn empty(target_id: impl Into<String>, bytes: u64, trailing_partial_bytes: u64) -> Self {
        Self {
            target_id: target_id.into(),
            records: 0,
            bytes,
            trailing_partial_bytes,
            last_durable_seq: None,
            last_durable_frontier: None,
            oldest_record_age_secs: None,
        }
    }
}

/// Append-only framed log for pin-mode post-F0 mutations.
///
/// Format per record:
/// `magic(8) | payload_len_be(u32) | sha256(payload)(32) | serde_json(payload)`.
/// Replay tolerates one torn trailing frame and reports its byte count in
/// status; checksum mismatch inside a complete frame is corruption.
pub struct PinModeMutationLog {
    file: File,
}

impl PinModeMutationLog {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self { file })
    }

    /// Append a group and issue a single durability barrier for the batch.
    pub fn append_group(&mut self, records: &[PinModeMutationRecord]) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        for record in records {
            let payload = serde_json::to_vec(record).map_err(invalid_data)?;
            let len: u32 = payload
                .len()
                .try_into()
                .map_err(|_| invalid_data("pin-mode log record exceeds u32 length"))?;
            let digest = Sha256::digest(&payload);
            self.file.write_all(PIN_MODE_LOG_MAGIC)?;
            self.file.write_all(&len.to_be_bytes())?;
            self.file.write_all(&digest)?;
            self.file.write_all(&payload)?;
        }
        self.file.sync_data()
    }

    pub fn replay_records(
        path: impl AsRef<Path>,
        target_id: &str,
        after: Option<&Frontier>,
    ) -> io::Result<Vec<PinModeMutationRecord>> {
        let read = read_pin_mode_log(path.as_ref())?;
        Ok(read
            .records
            .into_iter()
            .filter(|record| record.target.target_id == target_id)
            .filter(|record| {
                after.is_none_or(|frontier| {
                    !frontier.covers_log(Some(&record.writer_id), record.seq)
                })
            })
            .collect())
    }

    pub fn status(path: impl AsRef<Path>, target_id: &str) -> io::Result<PinModeMutationLogStatus> {
        let path = path.as_ref();
        let bytes = match std::fs::metadata(path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Ok(PinModeMutationLogStatus::empty(target_id, 0, 0));
            }
            Err(e) => return Err(e),
        };
        let read = read_pin_mode_log(path)?;
        let mut status =
            PinModeMutationLogStatus::empty(target_id, bytes, read.trailing_partial_bytes);
        let now_ms = unix_millis();
        for record in read
            .records
            .into_iter()
            .filter(|record| record.target.target_id == target_id)
        {
            status.records += 1;
            status.last_durable_seq = Some(
                status
                    .last_durable_seq
                    .map_or(record.seq, |s| s.max(record.seq)),
            );
            status.last_durable_frontier = Some(record.durable_frontier.clone());
            let age_secs = now_ms.saturating_sub(record.committed_at_ms) / 1000;
            status.oldest_record_age_secs = Some(
                status
                    .oldest_record_age_secs
                    .map_or(age_secs, |oldest| oldest.max(age_secs)),
            );
        }
        Ok(status)
    }
}

struct PinModeLogRead {
    records: Vec<PinModeMutationRecord>,
    trailing_partial_bytes: u64,
}

fn read_pin_mode_log(path: &Path) -> io::Result<PinModeLogRead> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Ok(PinModeLogRead {
                records: Vec::new(),
                trailing_partial_bytes: 0,
            });
        }
        Err(e) => return Err(e),
    };
    let file_len = file.metadata()?.len();
    let mut pos = 0u64;
    let mut records = Vec::new();

    loop {
        let remaining = file_len.saturating_sub(pos);
        if remaining == 0 {
            return Ok(PinModeLogRead {
                records,
                trailing_partial_bytes: 0,
            });
        }
        if remaining < PIN_MODE_LOG_HEADER_LEN as u64 {
            return Ok(PinModeLogRead {
                records,
                trailing_partial_bytes: remaining,
            });
        }

        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != PIN_MODE_LOG_MAGIC {
            return Err(invalid_data(format!(
                "pin-mode log bad magic at offset {pos}"
            )));
        }
        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes)?;
        let payload_len = u32::from_be_bytes(len_bytes) as usize;
        let mut expected_digest = [0u8; 32];
        file.read_exact(&mut expected_digest)?;
        pos += PIN_MODE_LOG_HEADER_LEN as u64;

        let remaining_payload = file_len.saturating_sub(pos);
        if remaining_payload < payload_len as u64 {
            return Ok(PinModeLogRead {
                records,
                trailing_partial_bytes: PIN_MODE_LOG_HEADER_LEN as u64 + remaining_payload,
            });
        }

        let mut payload = vec![0u8; payload_len];
        file.read_exact(&mut payload)?;
        let digest = Sha256::digest(&payload);
        if digest.as_slice() != expected_digest {
            return Err(invalid_data(format!(
                "pin-mode log checksum mismatch at offset {}",
                pos - PIN_MODE_LOG_HEADER_LEN as u64
            )));
        }
        let record: PinModeMutationRecord =
            serde_json::from_slice(&payload).map_err(invalid_data)?;
        records.push(record);
        pos += payload_len as u64;
    }
}

fn invalid_data<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, e.to_string())
}
