//! Retired side queue.
//!
//! `capture_mutations` ran only after cloud activation. `store_outbox` warned
//! and continued when a note could not be written after the local
//! acknowledgement. The record encoder dropped a file, a file write, an
//! inline write, a schema definition, and a contribution. Those notes are
//! not the capture path.

/// The JSON queue must not be written again.
pub(super) const RETIRED: &str = "json side queue is not the accepted-mutation capture";
