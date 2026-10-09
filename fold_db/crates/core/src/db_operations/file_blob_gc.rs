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

use crate::db_operations::{AtomDeleteLedgerEntry, DbOperations};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::sharing::blob_cas;
use chrono::{DateTime, Utc};
use serde::Serialize;

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

/// One candidate row from either local plane.
struct BlobCandidate {
    blob_ref: String,
    /// Full main-store key for resident-plane rows; `None` for `cas_blobs`
    /// namespace rows (addressed by bare `blob_ref`).
    main_store_key: Option<String>,
    approx_bytes: u64,
    stored_at: Option<String>,
}

async fn candidate_still_predates(
    db_ops: &DbOperations,
    candidate: &BlobCandidate,
    reclaim_before: DateTime<Utc>,
) -> Result<bool, SchemaError> {
    let stored_at = if let Some(key) = &candidate.main_store_key {
        let Some(bytes) = db_ops
            .atoms()
            .raw()
            .inner()
            .get(key.as_bytes())
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "gc-file-blobs recheck resident row {}: {error}",
                    candidate.blob_ref
                ))
            })?
        else {
            return Ok(false);
        };
        serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("stored_at")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
    } else {
        blob_cas::get_blob_in_ops(db_ops, &candidate.blob_ref)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "gc-file-blobs recheck {}: {error}",
                    candidate.blob_ref
                ))
            })?
            .and_then(|blob| blob.stored_at)
    };
    Ok(stored_at
        .as_deref()
        .and_then(|stamp| DateTime::parse_from_rfc3339(stamp).ok())
        .is_some_and(|stamp| stamp.with_timezone(&Utc) < reclaim_before))
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
    let mut candidates: Vec<BlobCandidate> = Vec::new();
    let cas_scan = blob_cas::scan_blob_row_headers(db_ops)
        .await
        .map_err(|e| SchemaError::InvalidData(format!("gc-file-blobs scan cas_blobs: {e}")))?;
    // An unreadable row is never a candidate: retained, counted, and warned
    // (per blob_ref) by the scan. It must not abort the pass for every other
    // row.
    report.file_blobs_unreadable_retained = cas_scan.unreadable.len() as u64;
    for header in cas_scan.headers {
        candidates.push(BlobCandidate {
            blob_ref: header.blob_ref,
            main_store_key: None,
            approx_bytes: header.approx_bytes,
            stored_at: header.stored_at,
        });
    }

    let resident_prefix = build_storage_key(None, RESIDENT_BLOB_PREFIX);
    let resident_rows = db_ops
        .atoms()
        .raw()
        .inner()
        .scan_prefix(resident_prefix.as_bytes())
        .await
        .map_err(|e| SchemaError::InvalidData(format!("gc-file-blobs scan cas_blob:: {e}")))?;
    for (key, value) in resident_rows {
        let full_key = String::from_utf8_lossy(&key).into_owned();
        let Some(blob_ref) = full_key
            .strip_prefix(resident_prefix.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        let stored_at = serde_json::from_slice::<serde_json::Value>(&value)
            .ok()
            .and_then(|v| {
                v.get("stored_at")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
        candidates.push(BlobCandidate {
            blob_ref,
            main_store_key: Some(full_key),
            approx_bytes: (key.len() + value.len()) as u64,
            stored_at,
        });
    }
    report.file_blobs_scanned = candidates.len() as u64 + report.file_blobs_unreadable_retained;

    // Nothing to reason about — skip the atom scan entirely. An execute pass
    // still leaves its ledger row: "a sweep ran at T and removed 0" is the
    // evidence that rules the run out as the cause of a missing blob, exactly
    // as gc-atoms records its zero-delete passes.
    if candidates.is_empty() {
        if !dry_run {
            let ledger_entry =
                AtomDeleteLedgerEntry::gc_file_blobs("gc-file-blobs", &report.scan_started_at);
            let handle = db_ops
                .atoms()
                .begin_delete_ledger_row(None, ledger_entry)
                .await?;
            db_ops
                .atoms()
                .commit_delete_ledger_row(handle, |_| {})
                .await;
        }
        return Ok(report);
    }

    // ---- Classify candidates.
    let reclaim_before = scan_started_at - chrono::Duration::seconds(FRESH_WRITE_GRACE_SECS);
    let mut to_delete: Vec<BlobCandidate> = Vec::new();
    let mut to_stamp: Vec<BlobCandidate> = Vec::new();
    for candidate in candidates {
        let _target_gate = db_ops
            .atoms()
            .lock_liveness_blobs(std::slice::from_ref(&candidate.blob_ref))
            .await;
        if !db_ops.atoms().blob_ref_edges_complete(None).await? {
            report.file_blobs_retained_incomplete =
                report.file_blobs_retained_incomplete.saturating_add(1);
            report.file_blobs_referenced = report.file_blobs_referenced.saturating_add(1);
            continue;
        }
        if db_ops
            .atoms()
            .has_active_blob_refs(&candidate.blob_ref, None)
            .await?
        {
            report.file_blobs_referenced += 1;
            report.blob_refs_referenced = report.blob_refs_referenced.saturating_add(1);
            continue;
        }
        match candidate
            .stored_at
            .as_deref()
            .map(DateTime::parse_from_rfc3339)
        {
            None => {
                report.file_blobs_stamped += 1;
                to_stamp.push(candidate);
            }
            Some(Ok(at)) if at.with_timezone(&Utc) < reclaim_before => {
                report.file_blobs_deleted += 1;
                report.bytes_freed_approx += candidate.approx_bytes;
                to_delete.push(candidate);
            }
            // Inside the grace window — or a stamp this run cannot parse.
            // Either way the row cannot be reasoned about as old, so it stays.
            Some(_) => {
                report.file_blobs_skipped_recent += 1;
            }
        }
    }

    if dry_run {
        return Ok(report);
    }

    // ---- Execute. Ledger row first (write-ahead), including a zero-delete
    // pass: "a sweep ran at T and removed 0" is itself audit evidence.
    let mut ledger_entry =
        AtomDeleteLedgerEntry::gc_file_blobs("gc-file-blobs", &report.scan_started_at);
    ledger_entry.file_blobs_scanned = report.file_blobs_scanned;
    ledger_entry.file_blobs_referenced = report.file_blobs_referenced;
    ledger_entry.file_blobs_deleted = report.file_blobs_deleted;
    ledger_entry.file_blobs_skipped_recent = report.file_blobs_skipped_recent;
    ledger_entry.file_blobs_stamped = report.file_blobs_stamped;
    ledger_entry.file_blob_bytes_freed_approx = report.bytes_freed_approx;
    ledger_entry.storage_keys_deleted = report.file_blobs_deleted;
    let handle = db_ops
        .atoms()
        .begin_delete_ledger_row(None, ledger_entry)
        .await?;

    for candidate in &to_stamp {
        if candidate.main_store_key.is_some() {
            stamp_resident_row(db_ops, &candidate.blob_ref, &report.scan_started_at).await?;
        } else {
            blob_cas::stamp_blob_stored_at(db_ops, &candidate.blob_ref, &report.scan_started_at)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "gc-file-blobs stamp {}: {e}",
                        candidate.blob_ref
                    ))
                })?;
        }
    }

    let mut main_store_deletes: Vec<String> = Vec::new();
    for candidate in &to_delete {
        let _target_gate = db_ops
            .atoms()
            .lock_liveness_blobs(std::slice::from_ref(&candidate.blob_ref))
            .await;
        if !db_ops.atoms().blob_ref_edges_complete(None).await?
            || db_ops
                .atoms()
                .has_active_blob_refs(&candidate.blob_ref, None)
                .await?
        {
            report.file_blobs_deleted = report.file_blobs_deleted.saturating_sub(1);
            report.bytes_freed_approx = report
                .bytes_freed_approx
                .saturating_sub(candidate.approx_bytes);
            report.file_blobs_referenced = report.file_blobs_referenced.saturating_add(1);
            continue;
        }
        if !candidate_still_predates(db_ops, candidate, reclaim_before).await? {
            report.file_blobs_deleted = report.file_blobs_deleted.saturating_sub(1);
            report.bytes_freed_approx = report
                .bytes_freed_approx
                .saturating_sub(candidate.approx_bytes);
            report.file_blobs_skipped_recent = report.file_blobs_skipped_recent.saturating_add(1);
            continue;
        }
        match &candidate.main_store_key {
            Some(key) => main_store_deletes.push(key.clone()),
            None => {
                blob_cas::delete_blob_in_ops(db_ops, &candidate.blob_ref)
                    .await
                    .map_err(|e| {
                        SchemaError::InvalidData(format!(
                            "gc-file-blobs delete {}: {e}",
                            candidate.blob_ref
                        ))
                    })?;
            }
        }
    }
    const CHUNK: usize = 2000;
    for chunk in main_store_deletes.chunks(CHUNK) {
        db_ops
            .atoms()
            .raw()
            .batch_delete_keys(chunk.to_vec())
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("gc-file-blobs delete cas_blob rows: {e}"))
            })?;
    }

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

/// Stamp `stored_at` on a resident-plane `cas_blob:{ref}` row that lacks one,
/// leaving every other byte unchanged. Sibling of
/// `blob_cas::stamp_blob_stored_at` for the main-store plane.
async fn stamp_resident_row(
    db_ops: &DbOperations,
    blob_ref: &str,
    stored_at: &str,
) -> Result<(), SchemaError> {
    let key = build_storage_key(None, &format!("{RESIDENT_BLOB_PREFIX}{blob_ref}"));
    let store = db_ops.atoms().raw().inner();
    let Some(bytes) = store
        .get(key.as_bytes())
        .await
        .map_err(|e| SchemaError::InvalidData(format!("gc-file-blobs read {key}: {e}")))?
    else {
        return Ok(());
    };
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Ok(());
    };
    let Some(obj) = value.as_object_mut() else {
        return Ok(());
    };
    if obj.get("stored_at").is_some_and(|v| !v.is_null()) {
        return Ok(());
    }
    obj.insert(
        "stored_at".to_string(),
        serde_json::Value::String(stored_at.to_string()),
    );
    let encoded = serde_json::to_vec(&value)
        .map_err(|e| SchemaError::InvalidData(format!("gc-file-blobs stamp encode: {e}")))?;
    store
        .put(key.as_bytes(), encoded)
        .await
        .map_err(|e| SchemaError::InvalidData(format!("gc-file-blobs stamp write {key}: {e}")))?;
    Ok(())
}
