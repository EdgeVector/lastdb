use super::*;

pub const PIN_LOG_NAMESPACE: &str = "sync_pin_log";
pub(super) const PIN_LOG_MODEL_VERSION: u32 = 1;
pub(super) const PIN_LOG_ENTRY_PREFIX: &str = "target:";
/// Durable per-writer published high-water mark, keyed by target id.
///
/// Truncation is gated on "cloud confirmed this frontier", and that judgement
/// used to live only in [`PinLogRuntime::published_f_by_writer`] and the
/// process-local [`MutationLogLocalCloud`] — both constructed empty. Every
/// daemon restart therefore forgot every confirmation, re-classified already
/// uploaded records as pending, and left them on disk forever. That is how
/// `sync_pin_log` reached 21 GiB (51% of the store) on Tom's primary *after*
/// truncate-after-confirm had already shipped.
pub(super) const PIN_LOG_PUBLISHED_F_PREFIX: &str = "published_f:";
pub(super) const BACKUP_RESTORE_F_KEY: &[u8] = b"backup_restore_f:personal";

/// Highest locally appended frontier across every writer and target.
///
/// Pin-row keys omit the writer id, so one home-wide allocation floor prevents
/// any writer from reusing another writer's pending frontier after a restart.
/// The first upgraded append seeds this point row from all legacy pin rows.
pub(crate) const PIN_LOG_APPENDED_F_KEY: &[u8] = b"appended_f:global";
pub(super) const CAPTURE_MARKER_RECEIPT_PREFIX: &str = "capture_marker_receipt:";
pub(super) const CAPTURE_MARKER_RECEIPT_VERSION: u32 = 1;
// A receipt repeats each target row. Bound both the row copies in memory and
// the complete durable group before the store sees a batch_put.
pub(super) const CAPTURE_MARKER_BATCH_MAX_ROW_BYTES: usize = 16 * 1024 * 1024;
pub(super) const CAPTURE_MARKER_BATCH_MAX_DURABLE_BYTES: usize = 32 * 1024 * 1024;

/// SHA-256 length prefixed onto a sealed mutation-log segment's plaintext,
/// matching the `HASH_SIZE` contract in `sync::log`.
pub(super) const MUTATION_LOG_SEGMENT_HASH_SIZE: usize = 32;
/// Keep request count sublinear in mutation count without producing giant
/// retry units. The byte bound normally fills first for real records.
pub(super) const MUTATION_LOG_SEGMENT_MAX_RECORDS: usize = 1_000;
/// Target object size for the continuous plane. The cycle-wide byte budget can
/// lower this, but never raises it.
pub(super) const MUTATION_LOG_SEGMENT_TARGET_BYTES: usize = 4 * 1024 * 1024;
/// A cycle that carries more sealed objects than this is draining a backlog.
pub(super) const MUTATION_LOG_CATCH_UP_OBJECTS: usize = 16;
/// PUT fan-out floor for a catch-up cycle.
///
/// The adaptive upload policy pins concurrency to 1 under `interactive_busy`,
/// and on the primary that flag is effectively always set
/// (`foreground_p95_ms=17236` on 2026-10-05). A backlog of interleaved schemas
/// seals one small object per record, so at concurrency 1 the cycle drained
/// ~1,300 objects (~5 KB each) per 30-45 min and published only ~30 s of log
/// time: the cloud frontier fell behind at wall-clock rate. Small PUTs are
/// network-bound, not CPU-bound, so yielding them to foreground work bought
/// nothing and cost every safe-upgrade soak its cloud-progress check.
pub(crate) const MUTATION_LOG_CATCH_UP_PUT_CONCURRENCY: usize = 8;
/// Hard ceiling for the `LASTDB_MUTATION_LOG_UPLOAD_CONCURRENCY` override.
pub(super) const MUTATION_LOG_PUT_CONCURRENCY_MAX: usize = 32;
/// Pin-log records materialized (atom reads) at once before sealing. Results
/// are consumed in log order, so batching and F ordering are unchanged.
pub(super) const MUTATION_LOG_MATERIALIZE_CONCURRENCY: usize = 8;

pub(super) fn mutation_log_put_concurrency_env() -> Option<usize> {
    env_flag::var_parsed::<usize>("LASTDB_MUTATION_LOG_UPLOAD_CONCURRENCY")
        .map(|value| value.clamp(1, MUTATION_LOG_PUT_CONCURRENCY_MAX))
}

/// PUT fan-out for one mutation-log publish cycle.
///
/// `explicit env override > catch-up floor > adaptive policy`. A steady-state
/// cycle (few objects) keeps the adaptive policy unchanged. A catch-up cycle
/// gets at least [`MUTATION_LOG_CATCH_UP_PUT_CONCURRENCY`]. Fan-out does not
/// affect ordering: every PUT of a call lands before the call returns, the
/// manifest call runs after the body call, and published F advances only after
/// every call succeeds.
///
/// A mutation probe that drops the catch-up floor must make
/// `mutation_log_backlog_cycle_puts_in_parallel_under_interactive_busy` go
/// RED on `PUT-PARALLELISM`.
pub(crate) fn mutation_log_put_concurrency(
    policy_concurrency: usize,
    objects_in_cycle: usize,
    env_override: Option<usize>,
) -> usize {
    if let Some(explicit) = env_override {
        return explicit.clamp(1, MUTATION_LOG_PUT_CONCURRENCY_MAX);
    }
    let policy = policy_concurrency.max(1);
    if objects_in_cycle > MUTATION_LOG_CATCH_UP_OBJECTS {
        policy.max(MUTATION_LOG_CATCH_UP_PUT_CONCURRENCY)
    } else {
        policy
    }
}
/// One sealed base member frozen at F0 for a pin target.
///
/// Identity is content-addressed (`sha256`) plus a stable path string used by
/// no-rewrite checks. Paths are relative when possible so status is portable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedBaseMember {
    pub path: String,
    pub sha256: String,
    pub len: u64,
    pub mtime_secs: i64,
}

/// Publish descriptor for catch-up of frozen base S plus log for one target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinModePublishDescriptor {
    pub target_id: String,
    pub target_prefix: String,
    pub base_frontier: u64,
    pub log_from: u64,
    pub last_durable_frontier: u64,
    pub sealed_base_shas: Vec<String>,
    pub publish_counter: u64,
    /// Always true for pin-mode publish: base may be fuzzy relative to tips.
    pub base_may_be_fuzzy: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PinLogRuntime {
    pub(crate) target_id: String,
    pub(crate) target_label: String,
    pub(crate) target_prefix: String,
    /// Durable log append is on for this target (continuous MutationLog and/or pin freeze).
    pub(crate) active: bool,
    /// True only for pin-mode freeze (sealed-base no-rewrite + catch-up). Continuous
    /// mutation-log capture keeps this false so everyday multi-device progress is
    /// never frozen behind a full-home base cut.
    pub(crate) pin_freeze: bool,
    pub(crate) base_frontier: u64,
    pub(crate) last_durable_frontier: u64,
    pub(crate) entry_count: u64,
    pub(crate) byte_count: u64,
    pub(crate) last_durable_at_ms: Option<u64>,
    pub(crate) sealed_base: Vec<SealedBaseMember>,
    pub(crate) pin_entered_at_ms: Option<u64>,
    pub(crate) s_rewrite_attempts: u64,
    pub(crate) materialize_pending: bool,
    /// Highest frontier successfully sealed+uploaded across writers (status max).
    /// For multi-writer filtering, use `published_f_by_writer` (0 when absent).
    pub(crate) published_frontier: u64,
    /// Per-writer published through-seq (Phase B vector F). Empty ⇒ treat all
    /// writers as unpublished (0). Scalar `published_frontier` is always the
    /// max of these values after an upload cycle.
    pub(crate) published_f_by_writer: HashMap<String, u64>,
    /// Cloud-confirmed record timestamp for each writer's published frontier.
    /// Kept separate from local durable append time so RPO never advances on a
    /// local-only fact.
    pub(crate) published_at_ms_by_writer: HashMap<String, u64>,
    /// Segments sealed+uploaded this process for continuous MutationLog plane.
    pub(crate) segments_uploaded: u64,
    /// Pin-log MutationIntent rows skipped this process because they cannot
    /// be sealed (missing atom). One bad row must not fail-close later ones.
    pub(crate) records_quarantined: u64,
    /// Last unsealable-record reason (atom id / field), for `lastdb status`.
    pub(crate) last_quarantine_reason: Option<String>,
}

impl PinLogRuntime {
    /// Shared constructor for inactive/continuous runtime rows (enter / ensure / persist).
    pub(super) fn new(
        target_id: String,
        target_label: String,
        target_prefix: String,
        base_frontier: u64,
        last_durable_frontier: u64,
        active: bool,
    ) -> Self {
        Self {
            target_id,
            target_label,
            target_prefix,
            active,
            pin_freeze: false,
            base_frontier,
            last_durable_frontier,
            entry_count: 0,
            byte_count: 0,
            last_durable_at_ms: None,
            sealed_base: Vec::new(),
            pin_entered_at_ms: None,
            s_rewrite_attempts: 0,
            materialize_pending: false,
            published_frontier: 0,
            published_f_by_writer: HashMap::new(),
            published_at_ms_by_writer: HashMap::new(),
            segments_uploaded: 0,
            records_quarantined: 0,
            last_quarantine_reason: None,
        }
    }

    /// Advance per-writer F and refresh scalar max after a cloud-confirmed put.
    pub(super) fn advance_published_f(
        &mut self,
        writer_id: &str,
        through: u64,
        published_at_ms: u64,
    ) {
        let entry = self
            .published_f_by_writer
            .entry(writer_id.to_string())
            .or_insert(0);
        if through >= *entry {
            *entry = through;
            self.published_at_ms_by_writer
                .insert(writer_id.to_string(), published_at_ms);
        }
        self.published_frontier = self
            .published_f_by_writer
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            .max(self.published_frontier)
            .max(through);
    }

    pub(super) fn status(&self) -> PinLogTargetStatus {
        let recovery_point_age_secs = self
            .published_at_ms_by_writer
            .values()
            .copied()
            .min()
            .map(|published_at| unix_millis().saturating_sub(published_at) / 1000);
        let pin_age_secs = self
            .pin_entered_at_ms
            .map(|entered| unix_millis().saturating_sub(entered) / 1000);
        let upload_backlog = self
            .last_durable_frontier
            .saturating_sub(self.published_frontier);
        PinLogTargetStatus {
            target_id: self.target_id.clone(),
            target_label: self.target_label.clone(),
            target_prefix: self.target_prefix.clone(),
            // Surface pin freeze as `active` for operators (pin-mode status).
            // Continuous capture without freeze still shows in entry counters.
            active: self.pin_freeze,
            base_frontier: self.base_frontier,
            last_durable_frontier: self.last_durable_frontier,
            entry_count: self.entry_count,
            byte_count: self.byte_count,
            last_durable_at_ms: self.last_durable_at_ms,
            sealed_base_count: self.sealed_base.len() as u64,
            pin_age_secs,
            pin_age_known: self.pin_entered_at_ms.is_some(),
            s_rewrite_attempts: self.s_rewrite_attempts,
            materialize_pending: self.materialize_pending,
            // Defensive inconsistency signal: pin freeze without an entry
            // timestamp (should not happen on the enter path; exit clears both).
            degraded: self.pin_freeze && self.pin_entered_at_ms.is_none(),
            published_frontier: self.published_frontier,
            published_through: self.published_frontier,
            recovery_point_age_secs,
            published_f_by_writer: self
                .published_f_by_writer
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            upload_backlog,
            segments_uploaded: self.segments_uploaded,
            records_quarantined: self.records_quarantined,
            last_quarantine_reason: self.last_quarantine_reason.clone(),
        }
    }
}

/// One durable pin-mode log record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinLogRecord {
    pub model_version: u32,
    /// Stable id derived from the sync target prefix (`personal` for prefix "").
    pub target_id: String,
    pub target_label: String,
    pub target_prefix: String,
    /// Writer/device that minted the underlying [`LogEntry`].
    pub writer_id: String,
    /// F after applying this record in the target stream.
    pub frontier_after: u64,
    pub timestamp_ms: u64,
    pub entry: LogEntry,
}

/// One marker's durable append identity. The writer flushes this receipt
/// before its target row, so a retry can repair an incomplete append.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CaptureMarkerAppendReceipt {
    pub(super) version: u32,
    pub(super) marker_key: Vec<u8>,
    pub(super) op_sha256: String,
    pub(super) records: Vec<PinLogRecord>,
}

/// Exact cloud-publication coordinate for one required mutation-log target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutationLogTargetPosition {
    pub(crate) target_id: String,
    pub(crate) target_label: String,
    pub(crate) writer_id: String,
    pub(crate) frontier: u64,
}

/// Durable pin-log append receipt for one logical operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutationLogAppendReceipt {
    pub(crate) writer_id: String,
    pub(crate) frontier: u64,
    /// At least one local cloud-staging lane retained this operation durably.
    ///
    /// Legacy outbox capture has no exact mutation-log target coordinates, so
    /// `targets.is_empty()` alone cannot distinguish success from no capture.
    pub(crate) durable_capture_written: bool,
    pub(crate) targets: Vec<MutationLogTargetPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MutationPublicationWait {
    Published,
    Pending,
}

/// Operator-visible per-target pin log counters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinLogTargetStatus {
    pub target_id: String,
    pub target_label: String,
    pub target_prefix: String,
    pub active: bool,
    pub base_frontier: u64,
    pub last_durable_frontier: u64,
    pub entry_count: u64,
    pub byte_count: u64,
    pub last_durable_at_ms: Option<u64>,
    /// How many sealed base members were frozen for this target at F0.
    pub sealed_base_count: u64,
    /// Seconds since pin entry when known.
    pub pin_age_secs: Option<u64>,
    /// False when pin age cannot be reported honestly.
    pub pin_age_known: bool,
    /// Instrumentation: attempts to rewrite a member of S while pinned.
    pub s_rewrite_attempts: u64,
    /// True after publish until background materialize completes.
    pub materialize_pending: bool,
    /// Degraded when pin is active but age/status is unknown.
    pub degraded: bool,
    /// Published frontier F for continuous mutation-log upload (0 if none).
    /// Scalar max across writers; see [`Self::published_f_by_writer`].
    #[serde(default)]
    pub published_frontier: u64,
    /// Cloud-confirmed published frontier (operator-facing name for F).
    #[serde(default)]
    pub published_through: u64,
    /// Age in seconds of the oldest cloud-confirmed writer recovery point.
    /// `None` until this process observes a cloud-confirmed segment.
    #[serde(default)]
    pub recovery_point_age_secs: Option<u64>,
    /// Per-writer published through_seq (vector F / HWM map).
    /// Empty before any upload; single-writer is a one-entry map.
    #[serde(default)]
    pub published_f_by_writer: BTreeMap<String, u64>,
    /// Durable records not yet sealed/uploaded (`last_durable - published`).
    #[serde(default)]
    pub upload_backlog: u64,
    /// Segments sealed+uploaded this process under the continuous log plane.
    #[serde(default)]
    pub segments_uploaded: u64,
    /// MutationIntent rows skipped because they cannot be sealed.
    #[serde(default)]
    pub records_quarantined: u64,
    /// Last unsealable-record reason (includes the missing atom id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_quarantine_reason: Option<String>,
}

/// One sealed continuous mutation-log segment ready for (or already on) cloud.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationLogSegment {
    pub segment: MutationLogSegmentId,
    /// JSON-encoded [`PinLogRecord`]s covering (prev_F, through_id].
    pub payload: Vec<u8>,
}

/// Outcome of one continuous mutation-log segment seal/upload cycle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MutationLogUploadReport {
    pub target_id: String,
    pub writer_id: String,
    pub records_considered: usize,
    pub segments_uploaded: usize,
    pub bytes_uploaded: u64,
    pub published_frontier_before: u64,
    pub published_frontier_after: u64,
    /// Remaining durable work still above published F after this cycle, as a
    /// **frontier delta in nanoseconds** (`last_durable_frontier -
    /// published_frontier`) — not a record count. Same quantity and units as
    /// `MutationLogPlaneStatus::log_lag`.
    pub upload_backlog_after: u64,
    /// Object keys written this cycle (`log/{writer_id}/{seq}.enc`).
    pub object_keys: Vec<String>,
    /// Durable pin-log records deleted this cycle because cloud confirmed them.
    #[serde(default)]
    pub records_truncated: usize,
    /// Durable rows the cycle read off the pin-log plane to fill this batch.
    ///
    /// The cycle reads a bounded window, not the whole plane, so this is the
    /// cycle's actual read cost — not the plane's size.
    #[serde(default)]
    pub rows_scanned: usize,
    /// `true` when the bounded scan stopped before reaching the end of the
    /// plane, so [`Self::records_considered`] is a **floor**, not a total.
    ///
    /// A count that silently changed meaning from "all pending records" to
    /// "as many as this cycle happened to look at" would read as a shrinking
    /// backlog. Anything consuming `records_considered` must check this first.
    #[serde(default)]
    pub records_considered_is_lower_bound: bool,
    /// `true` when the scan stopped on the per-cycle row budget rather than on
    /// the batch target or the end of the plane. Distinct from the ordinary
    /// early stop: it means rows were skipped that this cycle never judged.
    #[serde(default)]
    pub scan_row_budget_exhausted: bool,
    /// MutationIntent rows skipped this cycle because atoms could not be loaded.
    #[serde(default)]
    pub records_quarantined: usize,
    /// Last skip reason this cycle (atom id / field).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_quarantine_reason: Option<String>,
    /// PUT fan-out used for this cycle. Zero when the cycle uploaded nothing.
    ///
    /// A catch-up cycle raises this above the adaptive policy (floor
    /// [`MUTATION_LOG_CATCH_UP_PUT_CONCURRENCY`]). The cycle log line carries
    /// the same field so a primary measurement can name the fan-out without
    /// correlating a second catch-up line.
    #[serde(default)]
    pub put_concurrency: usize,
}

/// Outcome of applying sealed mutation-log segments after restoring a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MutationLogReplayReport {
    pub segments_considered: usize,
    pub segments_applied: usize,
    pub records_applied: usize,
    pub records_skipped_at_or_below_frontier: usize,
    /// Per-writer frontier after replay. This is the cursor a restored home
    /// persists/advertises before fetching the next cloud page.
    pub frontier_after: BTreeMap<String, u64>,
}

/// Where a continuous mutation-log cycle publishes its sealed segments.
///
/// Chosen by the CALL SITE, deliberately not by config: a production node must
/// not be able to fall into the local-only path by misconfiguration. Before
/// this existed the cycle always wrote to [`MutationLogLocalCloud`] — an
/// in-process `HashMap` — so `segments_uploaded` and the published frontier
/// advanced while nothing left the machine (primary, 2026-08-08: 236
/// "uploads", 0 `log/` objects in R2, lag growing ~1 s/s forever).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationLogPublish {
    /// Production: presign -> PUT -> confirm under `{scope}/log/{seq}.enc`.
    /// The frontier advances only for segments cloud confirmed.
    Cloud,
    /// TEST DOUBLE ONLY: record segments in the in-process plane so unit tests
    /// and CoW harnesses can assert object geometry without a network. Provides
    /// **no** off-box durability and must never be used by a production cycle.
    ///
    /// Only ever constructed under `cfg(test)` — that is the point, and it is
    /// why the non-test build sees it as dead.
    #[allow(dead_code)]
    LocalPlaneForTests,
}

/// In-process continuous log plane: sealed segments under `log/{writer_id}/{seq}`
/// plus published scalar F. Production maps this onto the account prefix; unit
/// tests and CoW harnesses use the local plane as ground truth for geometry.
#[derive(Debug, Default)]
pub struct MutationLogLocalCloud {
    /// object_key → sealed segment
    pub(super) segments: std::collections::HashMap<String, MutationLogSegment>,
    /// writer_id → published through_seq (scalar F per writer; Phase A often one)
    pub(super) published_f: Arc<MutationLogFrontierSnapshot>,
    /// Monotonic CAS counter for latest pointer (optional rare compact later).
    pub(super) latest_counter: u64,
}

/// Read-only status projection with no dependency on the upload-plane mutex.
/// The production writer advances this only after cloud confirmation and the
/// durable published-F flush. Its lock never covers storage or network work.
#[derive(Debug, Default)]
pub(crate) struct MutationLogFrontierSnapshot {
    pub(super) frontiers: std::sync::RwLock<BTreeMap<String, u64>>,
}

impl MutationLogFrontierSnapshot {
    pub(crate) fn vector_frontier(&self) -> BTreeMap<String, u64> {
        self.frontiers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Clone for MutationLogLocalCloud {
    fn clone(&self) -> Self {
        // Preserve the old deep-copy geometry semantics. Only the explicit
        // status handle shares this plane's frontier; cloned planes do not.
        Self {
            segments: self.segments.clone(),
            published_f: Arc::new(MutationLogFrontierSnapshot {
                frontiers: std::sync::RwLock::new(self.vector_frontier()),
            }),
            latest_counter: self.latest_counter,
        }
    }
}

impl MutationLogLocalCloud {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn frontier_snapshot(&self) -> Arc<MutationLogFrontierSnapshot> {
        Arc::clone(&self.published_f)
    }

    pub fn put_segment(&mut self, segment: &MutationLogSegment) -> Result<(), String> {
        let key = segment.segment.object_key.clone();
        if key.is_empty() {
            return Err("mutation log segment object_key is empty".to_string());
        }
        if !key.starts_with("log/") {
            return Err(format!(
                "mutation log segment key must be under log/: got {key}"
            ));
        }
        self.segments.insert(key, segment.clone());
        Ok(())
    }

    /// Advance published F for a writer after successful segment put(s).
    pub fn advance_published_f(&mut self, writer_id: &str, through: u64) {
        let mut frontiers = self
            .published_f
            .frontiers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = frontiers.entry(writer_id.to_string()).or_insert(0);
        *entry = (*entry).max(through);
        self.latest_counter = self.latest_counter.saturating_add(1);
    }

    pub fn published_f(&self, writer_id: &str) -> u64 {
        self.published_f
            .frontiers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(writer_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn keys_under_writer(&self, writer_id: &str) -> Vec<String> {
        let prefix = format!("log/{writer_id}/");
        let mut keys: Vec<String> = self
            .segments
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        keys.sort();
        keys
    }

    /// Distinct writer_ids that have at least one sealed segment on the plane.
    pub fn writer_ids(&self) -> Vec<String> {
        self.vector_frontier().into_keys().collect()
    }

    /// Vector frontier F as `{ writer_id → through_seq }` (design Phase B).
    pub fn vector_frontier(&self) -> BTreeMap<String, u64> {
        self.published_f.vector_frontier()
    }

    pub fn latest_counter(&self) -> u64 {
        self.latest_counter
    }

    /// Return the encrypted segments not wholly incorporated by `frontier`.
    ///
    /// A returned segment may straddle the frontier. Replay must still filter
    /// individual records; dropping the whole object would lose its newer tail.
    pub fn segments_above(&self, frontier: &Frontier) -> Vec<MutationLogSegment> {
        let mut segments = self
            .segments
            .values()
            .filter(|segment| {
                !frontier.covers_log(
                    segment.segment.writer_id.as_deref(),
                    segment.segment.through_id,
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        segments.sort_by(|a, b| {
            a.segment
                .writer_id
                .cmp(&b.segment.writer_id)
                .then(a.segment.through_id.cmp(&b.segment.through_id))
        });
        segments
    }
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
