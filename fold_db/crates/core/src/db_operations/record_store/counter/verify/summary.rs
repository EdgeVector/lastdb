//! What `lastdb status` shows of the verifier: the last complete run and a run in
//! progress.

use super::report::{Mismatches, Phase, VerifyReport};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// What `lastdb status` shows of the verifier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifySummary {
    /// The last complete run. `None` since the daemon started, if none finished.
    pub last: Option<VerifyLast>,
    /// A run that is not complete.
    pub running: Option<VerifyProgress>,
}

/// The result of the last complete run. `lastdb db verify-refs --last` prints
/// the same lines as the run did, so it keeps the same numbers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyLast {
    pub run_id: String,
    pub finished_at: DateTime<Utc>,
    pub scope: Option<String>,
    pub pages: u64,
    pub steps: u64,
    pub elapsed_ms: u64,
    pub mismatches: Mismatches,
    pub mismatches_total: u64,
    pub queue_entries_read: u64,
    pub blob_counters_read: u64,
    pub blob_queue_read: u64,
    /// Pages read again because a read hit the read limit of one call.
    pub page_retries: u64,
}

/// A run in progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyProgress {
    pub run_id: String,
    pub phase: Phase,
    pub pages: u64,
    pub started_at: DateTime<Utc>,
}

impl VerifyReport {
    pub(crate) fn last(&self) -> Option<VerifyLast> {
        Some(VerifyLast {
            run_id: self.run_id.clone(),
            finished_at: self.finished_at?,
            scope: self.scope.clone(),
            pages: self.pages,
            steps: self.steps,
            elapsed_ms: self.elapsed_ms,
            mismatches: self.mismatches,
            mismatches_total: self.mismatches_total,
            queue_entries_read: self.queue_entries_read,
            blob_counters_read: self.blob_counters_read,
            blob_queue_read: self.blob_queue_read,
            page_retries: self.page_retries,
        })
    }
}
