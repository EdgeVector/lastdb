//! What `lastdb status` shows of the reference checks: the summary of the
//! verifier. The object keeps its `/api/status` name, `verify_refs`.

use serde::{Deserialize, Serialize};

/// The reference checks in `lastdb status`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifyRefsReport {
    /// The verifier: the last complete run and a run in progress. Memory only;
    /// it is empty after a restart. `None` when the daemon predates the field.
    #[serde(default)]
    pub verify_refs: Option<super::VerifySummary>,
}
