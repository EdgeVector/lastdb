//! Per-step helpers for the `gc-file-blobs` sweep: candidate collection,
//! classification, and the execute-phase stamp and delete steps.

use super::{FileBlobGcReport, RESIDENT_BLOB_PREFIX};
use crate::db_operations::{AtomDeleteLedgerEntry, DbOperations};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::sharing::blob_cas;
use chrono::{DateTime, Utc};

/// One candidate row from either local plane.
pub(super) struct BlobCandidate {
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

/// Walk both local planes. An unreadable `cas_blobs` row is never a candidate.
pub(super) async fn collect_candidates(
    db_ops: &DbOperations,
    report: &mut FileBlobGcReport,
) -> Result<Vec<BlobCandidate>, SchemaError> {
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
    Ok(candidates)
}

/// An execute pass with nothing to reason about still leaves its ledger row.
pub(super) async fn record_empty_pass(
    db_ops: &DbOperations,
    report: &FileBlobGcReport,
) -> Result<(), SchemaError> {
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
    Ok(())
}

/// Split candidates into (delete, stamp) lists, updating the report counts.
pub(super) async fn classify_candidates(
    db_ops: &DbOperations,
    candidates: Vec<BlobCandidate>,
    reclaim_before: DateTime<Utc>,
    report: &mut FileBlobGcReport,
) -> Result<(Vec<BlobCandidate>, Vec<BlobCandidate>), SchemaError> {
    let _retired_reclaim = db_ops.atoms().retired_intent_reclaim_guard().await;
    let blob_ids: Vec<String> = candidates
        .iter()
        .map(|candidate| candidate.blob_ref.clone())
        .collect();
    let retired_roots = db_ops.atoms().retired_intent_blob_roots(&blob_ids).await?;
    let mut to_delete: Vec<BlobCandidate> = Vec::new();
    let mut to_stamp: Vec<BlobCandidate> = Vec::new();
    for candidate in candidates {
        let _target_gate = db_ops
            .atoms()
            .lock_liveness_blobs(std::slice::from_ref(&candidate.blob_ref))
            .await;
        if retired_roots.contains(&candidate.blob_ref) {
            report.file_blobs_referenced += 1;
            report.blob_refs_referenced += 1;
            continue;
        }
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
    Ok((to_delete, to_stamp))
}

/// Write-ahead ledger entry for an execute pass (counts only).
pub(super) fn execute_ledger_entry(report: &FileBlobGcReport) -> AtomDeleteLedgerEntry {
    let mut ledger_entry =
        AtomDeleteLedgerEntry::gc_file_blobs("gc-file-blobs", &report.scan_started_at);
    ledger_entry.file_blobs_scanned = report.file_blobs_scanned;
    ledger_entry.file_blobs_referenced = report.file_blobs_referenced;
    ledger_entry.file_blobs_deleted = report.file_blobs_deleted;
    ledger_entry.file_blobs_skipped_recent = report.file_blobs_skipped_recent;
    ledger_entry.file_blobs_stamped = report.file_blobs_stamped;
    ledger_entry.file_blob_bytes_freed_approx = report.bytes_freed_approx;
    ledger_entry.storage_keys_deleted = report.file_blobs_deleted;
    ledger_entry
}

/// Stamp `stored_at` on every undated candidate.
pub(super) async fn stamp_candidates(
    db_ops: &DbOperations,
    to_stamp: &[BlobCandidate],
    scan_started_at: &str,
) -> Result<(), SchemaError> {
    for candidate in to_stamp {
        if candidate.main_store_key.is_some() {
            stamp_resident_row(db_ops, &candidate.blob_ref, scan_started_at).await?;
        } else {
            blob_cas::stamp_blob_stored_at(db_ops, &candidate.blob_ref, scan_started_at)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "gc-file-blobs stamp {}: {e}",
                        candidate.blob_ref
                    ))
                })?;
        }
    }
    Ok(())
}

/// Recheck and delete each candidate under its target gate.
pub(super) async fn delete_candidates(
    db_ops: &DbOperations,
    to_delete: &[BlobCandidate],
    reclaim_before: DateTime<Utc>,
    report: &mut FileBlobGcReport,
) -> Result<(), SchemaError> {
    let _retired_reclaim = db_ops.atoms().retired_intent_reclaim_guard().await;
    let blob_ids: Vec<String> = to_delete
        .iter()
        .map(|candidate| candidate.blob_ref.clone())
        .collect();
    let retired_roots = db_ops.atoms().retired_intent_blob_roots(&blob_ids).await?;
    let mut main_store_deletes: Vec<String> = Vec::new();
    for candidate in to_delete {
        let _target_gate = db_ops
            .atoms()
            .lock_liveness_blobs(std::slice::from_ref(&candidate.blob_ref))
            .await;
        if retired_roots.contains(&candidate.blob_ref)
            || !db_ops.atoms().blob_ref_edges_complete(None).await?
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
    Ok(())
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
