//! Safe replay fields contain only numeric values and closed enum codes.

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(super) struct RestoreReplayDiagnosis {
    seq: u64,
    #[serde(flatten)]
    diagnosis: fold_db::sync::ReplayApplyDiagnosis,
}

impl RestoreReplayDiagnosis {
    pub(super) fn from_sync(error: &fold_db::sync::SyncError) -> Option<Self> {
        match error {
            fold_db::sync::SyncError::ReplayApplyFailed { seq, diagnosis, .. } => Some(Self {
                seq: *seq,
                diagnosis: *diagnosis,
            }),
            _ => None,
        }
    }
}
