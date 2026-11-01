//! Liveness request, target, and reply rules. Callers own scope checks and store reads.
//! Only the file blob class is served: the format 7 record model has no record
//! or field key counters to explain.

use fold_db::db_operations::BlobCounterView;
use serde::Deserialize;
use serde_json::Value;

const MAX_EXPLAIN_IDS: usize = 10_000;

pub(super) struct ExplainRequest {
    pub id: String,
    pub ids: Vec<String>,
}

impl ExplainRequest {
    /// Keep body errors before class errors.
    pub(super) fn parse(body: &[u8]) -> Result<Self, String> {
        #[derive(Deserialize)]
        struct Request {
            class: String,
            #[serde(default)]
            id: String,
            /// Several ids in one call.
            #[serde(default)]
            ids: Vec<String>,
        }

        let body: Request = serde_json::from_slice(body)
            .map_err(|error| format!("invalid liveness explain request: {error}"))?;
        if body.class.trim().is_empty() || (body.id.trim().is_empty() && body.ids.is_empty()) {
            return Err("liveness explain requires class and id".to_string());
        }
        if !body.class.trim().eq_ignore_ascii_case("blob") {
            return Err("class must be blob".to_string());
        }
        Ok(Self {
            id: body.id,
            ids: body.ids,
        })
    }
}

pub(super) fn target_ids(id: &str, mut ids: Vec<String>) -> Vec<String> {
    if !id.is_empty() {
        ids.insert(0, id.to_string());
    }
    ids.truncate(MAX_EXPLAIN_IDS);
    ids
}

pub(super) fn blob_reply(views: &[BlobCounterView], dwell_secs: u64) -> Value {
    let blobs: Vec<Value> = views
        .iter()
        .map(|view| {
            let state = match (view.refs, view.zero_since, view.suspect) {
                _ if view.unreadable => "unreadable",
                (Some(_), _, true) => "suspect",
                // Records name the blob and the store has no row for it: a
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
