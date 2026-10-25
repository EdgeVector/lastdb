//! Liveness target and reply rules. Callers own scope checks and store reads.

use fold_db::db_operations::{AtomCounterView, BlobCounterView};
use serde_json::Value;

const MAX_EXPLAIN_IDS: usize = 10_000;

pub(in crate::exec) fn target_ids(id: &str, mut ids: Vec<String>) -> Vec<String> {
    if !id.is_empty() {
        ids.insert(0, id.to_string());
    }
    ids.truncate(MAX_EXPLAIN_IDS);
    ids
}

pub(super) fn atom_reply(views: &[AtomCounterView]) -> Value {
    let atoms: Vec<Value> = views
        .iter()
        .map(|view| {
            let state = match (view.refs, view.body_present, view.suspect) {
                (Some(_), _, true) => "suspect",
                (Some(_), true, false) => "live",
                (Some(_), false, false) | (None, true, _) => "inconsistent",
                (None, false, _) => "absent",
            };
            serde_json::json!({
                "atom": view.atom,
                "refs": view.refs,
                "suspect": view.suspect,
                "blobs": view.blobs,
                "body_present": view.body_present,
                "state": state,
            })
        })
        .collect();
    serde_json::json!({ "liveness": { "class": "atom", "atoms": atoms } })
}

pub(super) fn blob_reply(views: &[BlobCounterView], dwell_secs: u64) -> Value {
    let blobs: Vec<Value> = views
        .iter()
        .map(|view| {
            let state = match (view.refs, view.zero_since, view.suspect) {
                _ if view.unreadable => "unreadable",
                (Some(_), _, true) => "suspect",
                // Atoms name the blob and the store has no row for it: a
                // write named the blob after its dwell deleted it.
                (Some(refs), _, false) if refs > 0 && !view.row_present => "referenced_row_missing",
                (Some(refs), _, false) if refs > 0 => "referenced",
                (Some(_), Some(_), false) => "in_dwell",
                (Some(_), None, false) => "inconsistent",
                (None, _, _) if view.row_present => "row_without_counter",
                (None, _, _) => "absent",
            };
            serde_json::json!({
                "blob": view.blob,
                "refs": view.refs,
                "zero_since_unix_ms": view.zero_since,
                "suspect": view.suspect,
                "queued": view.queued,
                "row_present": view.row_present,
                "unreadable": view.unreadable,
                "state": state,
            })
        })
        .collect();
    serde_json::json!({
        "liveness": {
            "class": "blob",
            "dwell_secs": dwell_secs,
            "blobs": blobs,
        }
    })
}
