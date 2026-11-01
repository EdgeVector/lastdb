//! The request and the result of a verify step.

use super::blob_report::BlobMismatches;
use crate::db_operations::record_store::WalkStats;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// What the verifier checks in each phase, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// The retention queue: every entry it holds, in the buckets of the
    /// directory and in the slices that the directory does not list. A bucket
    /// that holds entries and has no directory row is a fault.
    Queue,
    /// Blob counters (personal run only): each counter alone. See `blobs.rs`.
    Blobs,
    /// The queue of blobs at zero (personal run only): each entry against its
    /// counter.
    BlobQueue,
    /// The run is complete.
    Done,
}

/// The kinds of fault the verifier reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Kind {
    QueueEntryWithoutDirectory,
    BlobZeroWithoutDwell,
    BlobDwellWithRefs,
    ZeroBlobWithoutQueueEntry,
    BlobStaleQueueEntry,
    BlobSuspect,
    UnreadableBlobCounter,
    BlobCounterWithoutBlob,
}

impl Kind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::QueueEntryWithoutDirectory => "queue_entry_without_directory",
            Self::BlobZeroWithoutDwell => "blob_zero_without_dwell",
            Self::BlobDwellWithRefs => "blob_dwell_with_refs",
            Self::ZeroBlobWithoutQueueEntry => "zero_blob_without_queue_entry",
            Self::BlobStaleQueueEntry => "blob_stale_queue_entry",
            Self::BlobSuspect => "blob_suspect",
            Self::UnreadableBlobCounter => "unreadable_blob_counter",
            Self::BlobCounterWithoutBlob => "blob_counter_without_blob",
        }
    }
}

/// Faults by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mismatches {
    /// A queue entry sits in a bucket that the directory of the queue does not
    /// list, so the janitor or the dwell pass never finds it. The safety net,
    /// or `recount --execute`, writes the row again.
    #[serde(default)]
    pub queue_entry_without_directory: u64,
    /// The blob counter faults (counters, queue). See `blob_report.rs`.
    #[serde(flatten)]
    pub blob: BlobMismatches,
}

impl Mismatches {
    pub(crate) fn add_many(&mut self, kind: Kind, count: u64) {
        *self.slot(kind) += count;
    }

    fn slot(&mut self, kind: Kind) -> &mut u64 {
        match kind {
            Kind::QueueEntryWithoutDirectory => &mut self.queue_entry_without_directory,
            kind => self.blob.slot(kind),
        }
    }

    /// Every fault.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.queue_entry_without_directory + self.blob.total()
    }
}

/// One example of a fault, so an operator can look at it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    pub kind: String,
    /// The blob, the blob counter id or the queue key that the fault names.
    pub record: String,
    pub detail: String,
}

/// The request of one step. Every field is optional.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct VerifyRequest {
    /// The cursor a previous step returned. Without it the step starts a new run.
    #[serde(default)]
    pub cursor: Option<String>,
    /// The most pages one step reads (default 64, at most 1,024).
    #[serde(default)]
    pub max_pages: Option<u32>,
    /// The longest time one step runs, in milliseconds (default 3,000, at most 30,000).
    #[serde(default)]
    pub max_ms: Option<u64>,
    /// The pause after each page, in milliseconds (default 2, at most 1,000).
    #[serde(default)]
    pub pause_ms: Option<u64>,
    /// Rows asked for in each read (default 1,000, at most 4,000). A new run only.
    #[serde(default)]
    pub page_rows: Option<usize>,
    /// The storage scope prefix. Without it the run checks the personal scope. A new run only.
    #[serde(default)]
    pub scope: Option<String>,
}

/// The result of a step: the state of the run so far.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyReport {
    pub run_id: String,
    pub scope: Option<String>,
    pub phase: Phase,
    /// True when the run is complete. Then `next_cursor` is `None`.
    pub done: bool,
    /// Pass this cursor to the next step.
    pub next_cursor: Option<String>,
    pub started_at: DateTime<Utc>,
    /// When the run completed.
    pub finished_at: Option<DateTime<Utc>>,
    /// Time spent in steps, in milliseconds.
    pub elapsed_ms: u64,
    pub steps: u64,
    pub pages: u64,
    /// Entries the queue phase read.
    #[serde(default)]
    pub queue_entries_read: u64,
    /// Blob counters the blobs phase read.
    #[serde(default)]
    pub blob_counters_read: u64,
    /// Queue entries the blob queue phase read.
    #[serde(default)]
    pub blob_queue_read: u64,
    /// Pages read again because a read hit the read limit of one call.
    pub page_retries: u64,
    pub range_reads: WalkStats,
    pub mismatches: Mismatches,
    /// Every fault.
    pub mismatches_total: u64,
    pub samples: Vec<Sample>,
    pub notes: Vec<String>,
}

pub use super::summary::{VerifyLast, VerifyProgress, VerifySummary};
