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

/// The result of the last complete run.
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
    pub suspect_atoms: u64,
    pub tally_complete: bool,
    /// Tips the run read. Zero in a scope that holds data means a wrong scope.
    #[serde(default)]
    pub tips_read: u64,
    /// The rest of what the run read and counted. `lastdb db verify-refs --last`
    /// prints the same lines as the run did, so it needs the same numbers.
    #[serde(default)]
    pub records_read: u64,
    #[serde(default)]
    pub counters_read: u64,
    #[serde(default)]
    pub bodies_read: u64,
    /// Pages read again because a read hit the read limit of one call.
    #[serde(default)]
    pub page_retries: u64,
    /// Atoms the totals of the first two phases hold.
    #[serde(default)]
    pub tally_atoms: u64,
    /// Atoms that writers changed while the run read.
    #[serde(default)]
    pub touched_atoms: u64,
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
            suspect_atoms: self.suspect_atoms,
            tally_complete: self.tally_complete,
            tips_read: self.tips_read,
            records_read: self.records_read,
            counters_read: self.counters_read,
            bodies_read: self.bodies_read,
            page_retries: self.page_retries,
            tally_atoms: self.tally_atoms,
            touched_atoms: self.touched_atoms,
        })
    }
}
