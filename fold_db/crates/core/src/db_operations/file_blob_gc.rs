//! GC sweep for orphaned local file-blob rows (`gc-file-blobs`).
//!
//! # The gap this closes
//!
//! File bytes are content-addressed by the **plaintext** hash
//! (`blob_ref = sha256:<hex>`), so one blob can be referenced by atoms of any
//! record in any schema. Purge's atom guards prove an *atom* is unshared; they
//! cannot prove a *blob* is. The `bref:v1` reverse-edge set supplies that exact
//! target-partition proof. A missing completeness marker retains every blob.
//!
//! # What it covers — the two local planes
//!
//! - the `cas_blobs` namespace (sharing / delivery import / personal cache),
//!   keyed by bare `blob_ref`;
//! - resident-persist rows on the main store, keyed `cas_blob:{blob_ref}`.
//!
//! Cloud tiers (B2 CAS objects, R2 thumbnails) are deliberately **not**
//! touched: remote reachability must consider every device of the account, not
//! one node's store. That is a separate slice.
//!
//! # Reachability
//!
//! A blob is referenced when an active `bref:v1` edge names it. Atom writes
//! add that edge before they persist the atom source. Atom reclaim removes the
//! source first and the edge second. Source formats include content pointers
//! (`$lastdb_file.blob_ref` and legacy `$blob_ref`) and metadata references
//! (`file_blob_ref`, or `file_hash` as `sha256:{file_hash}`).
//!
//! Like `gc-atoms`, this is an admin sweep: the full scans here are the
//! explicit GC exception to the no-scan contract, never a single-key verb's
//! cost.
//!
//! # The write race
//!
//! Candidate enumeration can scan the two local blob planes. It never scans
//! atoms. For each candidate, the janitor holds the blob target gate, reads
//! the exact `bref:v1` partition, and rechecks the blob row's `stored_at`.
//! A concurrent writer either publishes its edge before the check or runs
//! after the gate and refreshes the immutable blob before it publishes the
//! atom source. The age gate remains a conservative extra guard.
//!
//! Rows from before `stored_at` existed cannot be aged, so the sweep **stamps**
//! them instead of deleting them: the first pass dates the legacy population,
//! and a later pass may reclaim what is still unreferenced. Two passes to
//! reclaim an undated orphan is the price of never deleting a row this run
//! could not reason about. (A row whose stamp is present but unparseable is
//! left alone forever — safe, visible in logs.)
//!
//! A `cas_blobs` row whose value cannot be opened (decrypt or inflate
//! failure, e.g. sealed over the at-rest inflate ceiling) is the same case at
//! the extreme: it is retained and counted in `file_blobs_unreadable_retained`,
//! and it never aborts the pass for the other rows.
//!
//! # Audit
//!
//! Every `--execute` pass writes one write-ahead `dellog:` row (verb
//! `gc-file-blobs`), counts only — a blob_ref is a hash of the file's
//! plaintext, so the ledger's no-uuid rule applies to it verbatim.

use crate::db_operations::DbOperations;
use crate::schema::SchemaError;
use chrono::Utc;
use serde::Serialize;
use sweep::{
    classify_candidates, collect_candidates, delete_candidates, execute_ledger_entry,
    record_empty_pass, stamp_candidates,
};

mod sweep;

/// Prefix of resident-persist blob rows on the main store.
const RESIDENT_BLOB_PREFIX: &str = "cas_blob:";

/// Minimum age (relative to `scan_started_at`) before an unreferenced row is
/// reclaimable — the margin that keeps a mid-scan blob-put→atom-write gap
/// from losing a live blob. See the module docs' race section.
pub(crate) const FRESH_WRITE_GRACE_SECS: i64 = 600;

/// Report for one [`gc_orphan_file_blobs`] pass.
#[derive(Debug, Clone, Serialize)]
pub struct FileBlobGcReport {
    pub dry_run: bool,
    /// When the reachability scan began (RFC3339) — the age-gate boundary.
    pub scan_started_at: String,
    /// Atom rows walked to build the reference set.
    pub atoms_scanned: u64,
    /// Distinct blob refs named by live atoms.
    pub blob_refs_referenced: u64,
    /// Blob rows walked across both local planes.
    pub file_blobs_scanned: u64,
    /// Rows kept because a live atom references them.
    pub file_blobs_referenced: u64,
    /// Rows retained because the `bref:v1` completeness marker is absent.
    pub file_blobs_retained_incomplete: u64,
    /// Unreferenced rows deleted (would-delete on dry run).
    pub file_blobs_deleted: u64,
    /// Unreferenced rows protected by the age gate.
    pub file_blobs_skipped_recent: u64,
    /// Undated rows stamped this pass instead of deleted (would-stamp on dry
    /// run).
    pub file_blobs_stamped: u64,
    /// `cas_blobs` rows whose value could not be opened (decrypt / inflate
    /// failure, e.g. sealed over the at-rest inflate ceiling). They cannot be
    /// aged, so they are always retained — never deleted, never stamped.
    pub file_blobs_unreadable_retained: u64,
    /// Approximate bytes freed by the deleted rows.
    pub bytes_freed_approx: u64,
}

/// Delete every local blob row no live atom references, oldest-first-safe.
/// See the module docs for the exact contract.
pub async fn gc_orphan_file_blobs(
    db_ops: &DbOperations,
    dry_run: bool,
) -> Result<FileBlobGcReport, SchemaError> {
    // Taken before the FIRST read, same as gc-atoms: everything below is a
    // snapshot of a store that keeps accepting writes, and this instant is
    // what the age gate compares against.
    let scan_started_at = Utc::now();

    let mut report = FileBlobGcReport {
        dry_run,
        scan_started_at: scan_started_at.to_rfc3339(),
        atoms_scanned: 0,
        blob_refs_referenced: 0,
        file_blobs_scanned: 0,
        file_blobs_referenced: 0,
        file_blobs_retained_incomplete: 0,
        file_blobs_deleted: 0,
        file_blobs_skipped_recent: 0,
        file_blobs_stamped: 0,
        file_blobs_unreadable_retained: 0,
        bytes_freed_approx: 0,
    };

    // ---- Candidates first, then references. A blob stored after this
    // snapshot is not a candidate at all; one stored before it is either seen
    // by the atom scan below or protected by the age gate.
    let candidates = collect_candidates(db_ops, &mut report).await?;
    report.file_blobs_scanned = candidates.len() as u64 + report.file_blobs_unreadable_retained;

    // Nothing to reason about — skip the atom scan entirely. An execute pass
    // still leaves its ledger row: "a sweep ran at T and removed 0" is the
    // evidence that rules the run out as the cause of a missing blob, exactly
    // as gc-atoms records its zero-delete passes.
    if candidates.is_empty() {
        if !dry_run {
            record_empty_pass(db_ops, &report).await?;
        }
        return Ok(report);
    }

    // ---- Classify candidates.
    let reclaim_before = scan_started_at - chrono::Duration::seconds(FRESH_WRITE_GRACE_SECS);
    let (to_delete, to_stamp) =
        classify_candidates(db_ops, candidates, reclaim_before, &mut report).await?;

    if dry_run {
        return Ok(report);
    }

    // ---- Execute. Ledger row first (write-ahead), including a zero-delete
    // pass: "a sweep ran at T and removed 0" is itself audit evidence.
    let ledger_entry = execute_ledger_entry(&report);
    let handle = db_ops
        .atoms()
        .begin_delete_ledger_row(None, ledger_entry)
        .await?;

    stamp_candidates(db_ops, &to_stamp, &report.scan_started_at).await?;

    delete_candidates(db_ops, &to_delete, reclaim_before, &mut report).await?;

    if !to_delete.is_empty() || !to_stamp.is_empty() {
        let _ = db_ops.flush().await;
    }
    db_ops
        .atoms()
        .commit_delete_ledger_row(handle, |_| {})
        .await;

    tracing::info!(
        file_blobs_scanned = report.file_blobs_scanned,
        file_blobs_referenced = report.file_blobs_referenced,
        file_blobs_deleted = report.file_blobs_deleted,
        file_blobs_skipped_recent = report.file_blobs_skipped_recent,
        file_blobs_stamped = report.file_blobs_stamped,
        file_blobs_unreadable_retained = report.file_blobs_unreadable_retained,
        bytes_freed_approx = report.bytes_freed_approx,
        "gc-file-blobs: reclaimed orphaned local file-blob rows"
    );

    Ok(report)
}
