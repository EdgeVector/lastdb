//! The blob checks of the verifier.
//!
//! The blob counters are global: one counter for each blob, whatever the scope
//! of the records that name it. Each page reads at one committed root, so the
//! invariants of a batch hold and no check here has a false positive.
//!
//! - **The counters** (phase `blobs`, personal run only). Each counter is
//!   checked alone: `suspect`, a count of zero with no dwell, a dwell with
//!   references, a dwell with no queue entry, references with no blob row.
//! - **The queue** (phase `blob_queue`, personal run only). Each entry must have
//!   a counter at zero with the same `zero_since`.
//!
//! A count that is too high or too low needs every record of every scope.
//! `lastdb db recount` computes it on a stopped home. Nothing here writes.

use super::report::Kind;
use super::state::Run;
use crate::db_operations::atom_store::blob_counter::{blob_id, BlobRead};

/// One fault that a check found.
pub(super) struct Finding {
    pub kind: Kind,
    pub detail: String,
}

/// One counter of the blobs phase.
pub(super) struct BlobEntry {
    pub id: String,
    pub counter: BlobRead,
    /// The queue holds the entry at the `zero_since` of the counter.
    pub queued: bool,
    /// The counter holds references and the `cas_blobs` plane has no row for
    /// the blob, read at the same root. Never set for a counter at zero: the
    /// dwell pass deletes the row and the counter in one batch, but a read of
    /// the row at another root could still race it.
    pub row_missing: bool,
}

/// A page of the blobs phase.
pub(super) struct BlobsPage {
    pub rows: usize,
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    pub entries: Vec<BlobEntry>,
}

/// One entry of the queue, with the counter it names.
pub(super) struct QueueEntry {
    pub zero_since: u64,
    pub id: String,
    pub counter: BlobRead,
}

/// A bucket of the blob queue that holds entries on a page and has no directory
/// row, read at the same root.
pub(super) struct UnlistedBucket {
    pub bucket: u64,
    /// Entries of the bucket on this page.
    pub entries: u64,
    /// The key of the first of them.
    pub sample: String,
}

/// A page of the queue phase.
pub(super) struct QueuePage {
    pub rows: usize,
    /// The last key the read examined, or `None` when the range ended.
    pub resume: Option<Vec<u8>>,
    pub entries: Vec<QueueEntry>,
    pub unlisted: Vec<UnlistedBucket>,
}

fn finding(kind: Kind, detail: String) -> Finding {
    Finding { kind, detail }
}

/// Check one counter alone. `queued` says that the queue holds its entry, and
/// `row_missing` that the blob has no row.
pub(super) fn judge_counter(counter: &BlobRead, queued: bool, row_missing: bool) -> Vec<Finding> {
    let counter = match counter {
        BlobRead::Absent => return Vec::new(),
        BlobRead::Unreadable => {
            return vec![finding(
                Kind::UnreadableBlobCounter,
                "blob counter does not decode".into(),
            )]
        }
        BlobRead::Present(counter) => counter,
    };
    let mut out = Vec::new();
    if counter.suspect {
        out.push(finding(
            Kind::BlobSuspect,
            format!("counter holds {} and is marked suspect", counter.refs),
        ));
    } else if counter.refs == 0 && counter.zero_since.is_none() {
        out.push(finding(
            Kind::BlobZeroWithoutDwell,
            "counter holds zero and has no zero_since".into(),
        ));
    }
    if counter.refs > 0 && counter.zero_since.is_some() {
        out.push(finding(
            Kind::BlobDwellWithRefs,
            format!("counter holds {} and has a zero_since", counter.refs),
        ));
    }
    if counter.zero_since.is_some() && !queued {
        out.push(finding(
            Kind::ZeroBlobWithoutQueueEntry,
            "counter has a zero_since and the queue has no entry for it".into(),
        ));
    }
    if row_missing {
        out.push(finding(
            Kind::BlobCounterWithoutBlob,
            format!(
                "counter holds {} and the store has no blob row: the records name a blob that is not there",
                counter.refs
            ),
        ));
    }
    out
}

/// Check one queue entry against the counter it names.
pub(super) fn judge_queue_entry(zero_since: u64, counter: &BlobRead) -> Option<Finding> {
    let detail = match counter {
        BlobRead::Absent => "the entry names a blob with no counter".to_string(),
        BlobRead::Unreadable => "the entry names a counter that does not decode".to_string(),
        BlobRead::Present(counter)
            if counter.zero_since == Some(zero_since) && counter.refs == 0 && !counter.suspect =>
        {
            return None
        }
        BlobRead::Present(counter) => format!(
            "the entry is at {zero_since} and the counter holds {} with zero_since {:?}{}",
            counter.refs,
            counter.zero_since,
            if counter.suspect { " (suspect)" } else { "" }
        ),
    };
    Some(finding(Kind::BlobStaleQueueEntry, detail))
}

/// Judge a page of blob counters.
pub(super) fn apply_blobs(run: &mut Run, page: BlobsPage) {
    run.report.pages += 1;
    for entry in page.entries {
        if !matches!(entry.counter, BlobRead::Absent) {
            run.report.blob_counters_read += 1;
        }
        if let BlobRead::Present(counter) = &entry.counter {
            if blob_id(&counter.blob) != entry.id {
                run.note_fault(
                    Kind::UnreadableBlobCounter,
                    &entry.id,
                    "the counter names a blob that does not match its key".into(),
                );
            }
        }
        let label = match &entry.counter {
            BlobRead::Present(counter) => counter.blob.clone(),
            _ => entry.id.clone(),
        };
        for finding in judge_counter(&entry.counter, entry.queued, entry.row_missing) {
            run.note_fault(finding.kind, &label, finding.detail);
        }
    }
}

/// Judge a page of queue entries.
pub(super) fn apply_queue(run: &mut Run, page: QueuePage) {
    run.report.pages += 1;
    for bucket in &page.unlisted {
        run.note_faults(
            Kind::QueueEntryWithoutDirectory,
            &bucket.sample.replace('\0', "\\0"),
            format!(
                "bucket {} of the blob queue holds {} entries on this page and the directory of the queue has no row for it: the dwell pass never lists it",
                bucket.bucket, bucket.entries
            ),
            bucket.entries,
        );
    }
    run.report.blob_queue_read += page.entries.len() as u64;
    for entry in page.entries {
        if let Some(finding) = judge_queue_entry(entry.zero_since, &entry.counter) {
            run.note_fault(finding.kind, &entry.id, finding.detail);
        }
    }
}
