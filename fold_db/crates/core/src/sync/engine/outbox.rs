//! Durable outbox + local op recording path.

use super::super::log::{LogEntry, LogOp};
use super::*;
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) const OUTBOX_DROP_BATCH_SIZE: usize = 4096;
pub(crate) const CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES: usize = 4 * 1024 * 1024;

/// In-memory upload queue depth from adaptive policy fields.
///
/// `0` means unlimited. Prefer the tighter of `max_pending` and
/// `max_upload_entries` when both are set so a multi-thousand durable outbox
/// cannot deserialize thousands of full `LogEntry` payloads into RAM before
/// per-cycle seal/upload caps run.
pub(crate) fn upload_queue_cap(max_pending: usize, max_upload_entries: usize) -> usize {
    match (max_pending, max_upload_entries) {
        (0, 0) => 0,
        (0, u) => u,
        (m, 0) => m,
        (m, u) => m.min(u),
    }
}

mod backpressure;
mod record_kv;
mod record_op;
mod schedule;
mod storage;
