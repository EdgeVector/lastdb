//! The conflict state of one record field in a conflict metadata reply.

use serde::Serialize;

/// A clean result requires a complete authoritative read. Unknown is not clean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordConflictState {
    Clean,
    Conflict,
    Unknown,
}
