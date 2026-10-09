//! Themed module split from the parent.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcJobState {
    Queued,
    Active,
    Completed,
    PartialFailure,
    Superseded,
    Interrupted,
    Failed,
}

impl GcJobState {
    pub fn terminal(self) -> bool {
        !matches!(self, Self::Queued | Self::Active)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcJobReceipt {
    pub version: u32,
    pub job_id: String,
    pub trigger: String,
    pub dry_run: bool,
    pub state: GcJobState,
    pub phase: String,
    pub created_unix_ms: u64,
    pub progress_unix_ms: u64,
    pub stop_reason: Option<String>,
    /// Version 2. A typed code beside the redacted free-text stop reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_code: Option<GcStopCode>,
    pub report: Option<BackupOrphanGcReport>,
    /// Proof identity only, never a replayable keep set or a cloud capability.
    pub selection: Option<GcSelection>,
    /// Number of reconciled, numbered object receipts. Never a bucket cursor.
    pub objects_reconciled: u64,
    pub receipt_capacity: u64,
    pub delete_acknowledged: u64,
    pub acknowledged_bytes: u64,
    pub failed_before_dispatch: u64,
    pub failed_bytes: u64,
    pub unknown: u64,
    pub unknown_bytes: u64,
    /// Version 2. Present once any version 2 outcome reconciles. The version
    /// 1 counters above keep their meaning; this block adds the precise split.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispositions: Option<GcDispositionCounts>,
    /// Head of the in-flight dispatch set. Empty when no DELETE is in flight.
    pub pending: Option<GcObjectReceipt>,
    /// Remainder of a parallel dispatch batch. Absent on v1 receipts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_batch: Vec<GcObjectReceipt>,
}

impl GcJobReceipt {
    pub(super) fn in_flight_len(&self) -> u64 {
        u64::from(self.pending.is_some()) + self.pending_batch.len() as u64
    }

    pub(super) fn in_flight_objects(&self) -> Vec<GcObjectReceipt> {
        let mut objects = Vec::with_capacity(self.in_flight_len() as usize);
        if let Some(pending) = &self.pending {
            objects.push(pending.clone());
        }
        objects.extend(self.pending_batch.iter().cloned());
        objects.sort_by_key(|object| object.sequence);
        objects
    }

    pub(super) fn set_in_flight(&mut self, mut objects: Vec<GcObjectReceipt>) {
        objects.sort_by_key(|object| object.sequence);
        self.pending = objects.first().cloned();
        self.pending_batch = objects.into_iter().skip(1).collect();
    }
}

impl GcJobReceipt {
    /// Version 1 unless a version 2 field carries a value.
    pub fn format_version(&self) -> u32 {
        if self.stop_code.is_some() || self.dispositions.is_some() {
            GC_RECEIPT_VERSION
        } else {
            GC_RECEIPT_MIN_VERSION
        }
    }
}

/// Typed stop code. `stop_reason` stays the human-readable, redacted text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcStopCode {
    /// The held publication state changed after selection.
    PublicationStateChanged,
    /// A newer generation replaced this job before it finished.
    NewerGeneration,
    /// At least one object receipt is not an acknowledgement.
    ObjectFailures,
    /// The receipt archive reached its bounded capacity.
    MetadataCapacityRequired,
    /// The GC task panicked; no automatic replay.
    TaskPanic,
    /// Any other execution error; see `stop_reason`.
    ExecutionError,
    /// Recovery found the job unfinished after the daemon stopped.
    DaemonLost,
    /// Acceptance did not reach the index commit.
    AcceptanceInterrupted,
    /// The summary exists but no index entry claims it.
    UnindexedAcceptance,
}

impl GcStopCode {
    pub(super) fn from_failure_text(text: &str) -> Self {
        if text.contains("GC_METADATA_CAPACITY_REQUIRED") {
            Self::MetadataCapacityRequired
        } else if text.contains("GC task panic") {
            Self::TaskPanic
        } else {
            Self::ExecutionError
        }
    }
}

/// Version 2 per-disposition counts and exact listed bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcDispositionCounts {
    pub deleted_now: u64,
    pub deleted_now_bytes: u64,
    pub already_absent: u64,
    pub already_absent_bytes: u64,
    pub failed: u64,
    pub failed_bytes: u64,
    pub protected: u64,
    pub protected_bytes: u64,
    pub uncertain_write: u64,
    pub uncertain_write_bytes: u64,
}

/// Receipt encoding of the P0 `ProviderVersion` enum.
///
/// | P0 `ProviderVersion` | receipt `provider_version` |
/// |---|---|
/// | absent (version 1 receipt, or no version knowledge) | `null` / omitted |
/// | `Unversioned` | `"unversioned"` |
/// | `Exact(id)` | `"exact:<id>"` |
///
/// The prefix keeps a provider version id that is literally `unversioned`
/// distinct from the explicit unversioned locator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcProviderVersion {
    Unversioned,
    Exact(String),
}

impl GcProviderVersion {
    pub const UNVERSIONED: &'static str = "unversioned";
    pub const EXACT_PREFIX: &'static str = "exact:";

    pub fn to_receipt_string(&self) -> String {
        match self {
            Self::Unversioned => Self::UNVERSIONED.into(),
            Self::Exact(id) => format!("{}{id}", Self::EXACT_PREFIX),
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        if text == Self::UNVERSIONED {
            Some(Self::Unversioned)
        } else {
            text.strip_prefix(Self::EXACT_PREFIX)
                .filter(|id| !id.is_empty())
                .map(|id| Self::Exact(id.into()))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcSelection {
    pub cloud_db_hash: Option<String>,
    pub post_cas_generation: u64,
    pub published_manifest_sha256: Vec<String>,
    pub keep_sha256: String,
}

/// Object outcomes. The first four are version 1 and keep their meaning.
/// The version 2 variants mirror the P0 dispositions and write closures:
///
/// | receipt outcome | P0 source | meaning |
/// |---|---|---|
/// | `deleted_now` | `Disposition::DeletedNow` | the provider removed this exact instance |
/// | `already_absent` | `Disposition::AlreadyAbsent` | the exact instance was not present at dispatch |
/// | `protected` | `Disposition::Protected` | a hold or head change protected the instance; no DELETE effect |
/// | `failed` | `WriteOutcome::NotApplied` | terminal provider closure after dispatch; no effect |
/// | `uncertain_write` | accepted write, no `WriteOutcome` | dispatched, no terminal closure; may still complete |
///
/// `delete_acknowledged` (version 1) stays a bare 2xx acknowledgement that
/// does not split `deleted_now` from `already_absent`. `unknown` (version 1)
/// stays the local executor's crash-or-timeout case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcObjectOutcome {
    Intent,
    DeleteAcknowledged,
    FailedBeforeDispatch,
    Unknown,
    DeletedNow,
    AlreadyAbsent,
    Failed,
    Protected,
    UncertainWrite,
}

impl GcObjectOutcome {
    /// True for a variant that version 1 readers do not know.
    pub fn is_version_2(&self) -> bool {
        matches!(
            self,
            Self::DeletedNow
                | Self::AlreadyAbsent
                | Self::Failed
                | Self::Protected
                | Self::UncertainWrite
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcObjectReceipt {
    pub version: u32,
    pub job_id: String,
    pub sequence: u64,
    pub object_class: String,
    pub key: String,
    pub listed_bytes: u64,
    pub outcome: GcObjectOutcome,
    /// Version 2. The exact cloud instance this receipt addresses. `key` is
    /// digest-derived and can name a replacement with the same digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    /// Version 2. See [`GcProviderVersion`] for the P0 mapping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_version: Option<String>,
}

impl GcObjectReceipt {
    /// Version 1 unless a version 2 field or outcome carries a value.
    pub fn format_version(&self) -> u32 {
        if self.instance_id.is_some()
            || self.provider_version.is_some()
            || self.outcome.is_version_2()
        {
            GC_RECEIPT_VERSION
        } else {
            GC_RECEIPT_MIN_VERSION
        }
    }
}
