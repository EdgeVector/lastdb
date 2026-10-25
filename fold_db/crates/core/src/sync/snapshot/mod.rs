//! Frozen legacy snapshot metadata only. No capture, upload or restore operation.
mod model;
mod reports;
pub use model::{NamespaceData, Snapshot, SnapshotEntry, SnapshotValueEncoding};
pub use reports::{ScrubReport, SnapshotRestoreReport, SnapshotScrubReport, UndecryptableRow};
