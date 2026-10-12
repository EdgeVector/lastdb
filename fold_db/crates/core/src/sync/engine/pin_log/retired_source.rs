//! Untouched authenticated source objects for exclusive restore preservation.

use super::{MutationLogSegment, PinLogRecord};
use crate::sync::snapshot_log::MutationLogSegmentId;
use std::sync::Arc;

/// Bytes remain exactly as authenticated by the normal mutation-log opener.
#[derive(Clone, Debug)]
pub struct AuthenticatedReplayObject {
    pub segment: MutationLogSegmentId,
    pub ciphertext: Arc<[u8]>,
}

/// The outer record precedes receiver-prefix and generation conversion.
#[derive(Clone, Debug)]
pub struct RetiredReplaySource {
    pub record: PinLogRecord,
    pub record_index: usize,
    pub transaction_group: bool,
    pub objects: Vec<AuthenticatedReplayObject>,
}

impl From<&MutationLogSegment> for AuthenticatedReplayObject {
    fn from(object: &MutationLogSegment) -> Self {
        Self {
            segment: object.segment.clone(),
            ciphertext: Arc::from(object.payload.as_slice()),
        }
    }
}

/// A preserved record advances the cursor only after its journal is durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplayEntryOutcome {
    Applied,
    RetiredAuthorityPreserved { mutations: usize },
}
