//! The request and the result of a verify step.

use crate::db_operations::atom_store::WalkStats;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// What the verifier checks in each phase, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Tip records (`mk:`): the atom of each tip has a body, and the counter is
    /// not below the tips of the page.
    Tips,
    /// Conflict records: the same check for the atoms a record holds.
    Records,
    /// Body and counter records of atoms: pairing, zero, suspect, and the
    /// counter against the totals of the first two phases.
    Atoms,
    /// Blob counters (personal run only): each counter alone. See `blobs.rs`.
    Blobs,
    /// The queue of blobs at zero (personal run only): each entry against its
    /// counter.
    BlobQueue,
    /// The run is complete.
    Done,
}

/// The kinds of fault the verifier reports. A kind is a fact about one atom. An
/// atom can show more than one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Kind {
    CounterBelowTips,
    CounterAboveTips,
    UnconfirmedCountMismatch,
    BodyWithoutCounter,
    CounterWithoutBody,
    ZeroCounter,
    SuspectCounter,
    TipWithoutBody,
    RecordWithoutBody,
    UnreadableTip,
    UnreadableRecord,
    UnreadableCounter,
    UnsealedRows,
    BlobCounterMissing,
    BlobCounterBelowRefs,
    BlobZeroWithoutDwell,
    BlobDwellWithRefs,
    BlobUnqueued,
    BlobStaleQueueEntry,
    BlobSuspect,
    UnreadableBlobCounter,
}

impl Kind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::CounterBelowTips => "counter_below_tips",
            Self::CounterAboveTips => "counter_above_tips",
            Self::UnconfirmedCountMismatch => "unconfirmed_count_mismatch",
            Self::BodyWithoutCounter => "body_without_counter",
            Self::CounterWithoutBody => "counter_without_body",
            Self::ZeroCounter => "zero_counter",
            Self::SuspectCounter => "suspect_counter",
            Self::TipWithoutBody => "tip_without_body",
            Self::RecordWithoutBody => "record_without_body",
            Self::UnreadableTip => "unreadable_tip",
            Self::UnreadableRecord => "unreadable_record",
            Self::UnreadableCounter => "unreadable_counter",
            Self::UnsealedRows => "unsealed_rows",
            Self::BlobCounterMissing => "blob_counter_missing",
            Self::BlobCounterBelowRefs => "blob_counter_below_refs",
            Self::BlobZeroWithoutDwell => "blob_zero_without_dwell",
            Self::BlobDwellWithRefs => "blob_dwell_with_refs",
            Self::BlobUnqueued => "blob_unqueued",
            Self::BlobStaleQueueEntry => "blob_stale_queue_entry",
            Self::BlobSuspect => "blob_suspect",
            Self::UnreadableBlobCounter => "unreadable_blob_counter",
        }
    }
}

/// Faults by kind. `unconfirmed_count_mismatch` is not a fault: it names atoms
/// whose count differs from the totals of the pass while a writer changed
/// them. A second run with no writers settles each of them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mismatches {
    /// The counter holds fewer references than the tips or records that name the atom.
    pub counter_below_tips: u64,
    /// The counter holds more references than the tips and records that name the atom.
    pub counter_above_tips: u64,
    /// A count differs from the totals, and a writer changed the atom during the pass.
    pub unconfirmed_count_mismatch: u64,
    /// A body record has no counter record.
    pub body_without_counter: u64,
    /// A counter record has no body record.
    pub counter_without_body: u64,
    /// A counter holds zero and is not `suspect`.
    pub zero_counter: u64,
    /// A counter has the `suspect` flag.
    pub suspect_counter: u64,
    /// A tip names an atom that has no body.
    pub tip_without_body: u64,
    /// A conflict record names an atom that has no body.
    pub record_without_body: u64,
    pub unreadable_tip: u64,
    pub unreadable_record: u64,
    pub unreadable_counter: u64,
    /// Reads that dropped a row with no at-rest envelope. Such a row reads as
    /// absent, so the verifier cannot judge it. `lastdb db reap-unsealed`
    /// removes these rows.
    #[serde(default)]
    pub unsealed_rows: u64,
    /// An atom counter or a tip names a blob that has no counter.
    #[serde(default)]
    pub blob_counter_missing: u64,
    /// A blob counter holds fewer references than the atom counters or tips of
    /// one page that name the blob.
    #[serde(default)]
    pub blob_counter_below_refs: u64,
    /// A blob counter holds zero, is not `suspect`, and has no `zero_since`.
    #[serde(default)]
    pub blob_zero_without_dwell: u64,
    /// A blob counter holds references and has a `zero_since`.
    #[serde(default)]
    pub blob_dwell_with_refs: u64,
    /// A blob counter has a `zero_since` and the queue has no entry for it.
    #[serde(default)]
    pub blob_unqueued: u64,
    /// A queue entry names a counter that is absent, has references, is
    /// `suspect`, or has another `zero_since`.
    #[serde(default)]
    pub blob_stale_queue_entry: u64,
    /// A blob counter has the `suspect` flag.
    #[serde(default)]
    pub blob_suspect: u64,
    #[serde(default)]
    pub unreadable_blob_counter: u64,
}

impl Mismatches {
    pub(crate) fn add(&mut self, kind: Kind) {
        self.add_many(kind, 1);
    }

    pub(crate) fn add_many(&mut self, kind: Kind, count: u64) {
        *self.slot(kind) += count;
    }

    fn slot(&mut self, kind: Kind) -> &mut u64 {
        match kind {
            Kind::CounterBelowTips => &mut self.counter_below_tips,
            Kind::CounterAboveTips => &mut self.counter_above_tips,
            Kind::UnconfirmedCountMismatch => &mut self.unconfirmed_count_mismatch,
            Kind::BodyWithoutCounter => &mut self.body_without_counter,
            Kind::CounterWithoutBody => &mut self.counter_without_body,
            Kind::ZeroCounter => &mut self.zero_counter,
            Kind::SuspectCounter => &mut self.suspect_counter,
            Kind::TipWithoutBody => &mut self.tip_without_body,
            Kind::RecordWithoutBody => &mut self.record_without_body,
            Kind::UnreadableTip => &mut self.unreadable_tip,
            Kind::UnreadableRecord => &mut self.unreadable_record,
            Kind::UnreadableCounter => &mut self.unreadable_counter,
            Kind::UnsealedRows => &mut self.unsealed_rows,
            Kind::BlobCounterMissing => &mut self.blob_counter_missing,
            Kind::BlobCounterBelowRefs => &mut self.blob_counter_below_refs,
            Kind::BlobZeroWithoutDwell => &mut self.blob_zero_without_dwell,
            Kind::BlobDwellWithRefs => &mut self.blob_dwell_with_refs,
            Kind::BlobUnqueued => &mut self.blob_unqueued,
            Kind::BlobStaleQueueEntry => &mut self.blob_stale_queue_entry,
            Kind::BlobSuspect => &mut self.blob_suspect,
            Kind::UnreadableBlobCounter => &mut self.unreadable_blob_counter,
        }
    }

    /// The faults, without the unconfirmed candidates.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.counter_below_tips
            + self.counter_above_tips
            + self.body_without_counter
            + self.counter_without_body
            + self.zero_counter
            + self.suspect_counter
            + self.tip_without_body
            + self.record_without_body
            + self.unreadable_tip
            + self.unreadable_record
            + self.unreadable_counter
            + self.unsealed_rows
            + self.blob_counter_missing
            + self.blob_counter_below_refs
            + self.blob_zero_without_dwell
            + self.blob_dwell_with_refs
            + self.blob_unqueued
            + self.blob_stale_queue_entry
            + self.blob_suspect
            + self.unreadable_blob_counter
    }
}

/// One example of a fault, so an operator can look at the atom.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    pub kind: String,
    pub atom: String,
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
    /// The most atoms the totals hold (default 1,000,000). A new run only.
    #[serde(default)]
    pub max_atoms: Option<usize>,
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
    pub tips_read: u64,
    pub records_read: u64,
    pub counters_read: u64,
    pub bodies_read: u64,
    /// Blob counters the blobs phase read.
    #[serde(default)]
    pub blob_counters_read: u64,
    /// Queue entries the blob queue phase read.
    #[serde(default)]
    pub blob_queue_read: u64,
    /// Pages read again because a read hit the read limit of one call.
    pub page_retries: u64,
    pub range_reads: WalkStats,
    /// Atoms the totals of the first two phases hold.
    pub tally_atoms: u64,
    /// False when more atoms than the cap were referenced. Then the verifier
    /// does not compare counters with totals.
    pub tally_complete: bool,
    /// Atoms that writers changed while the run read.
    pub touched_atoms: u64,
    pub mismatches: Mismatches,
    /// The faults, without the unconfirmed candidates.
    pub mismatches_total: u64,
    /// Counters with the `suspect` flag that the run read.
    pub suspect_atoms: u64,
    pub samples: Vec<Sample>,
    pub notes: Vec<String>,
}

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
        })
    }
}
