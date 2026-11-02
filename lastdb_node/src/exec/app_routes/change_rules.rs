//! App change-feed request and reply rules. No host, clocks, or storage access.

use fold_db::db_operations::ChangeFeedEvent;
use serde::Deserialize;
use serde_json::Value;

const APP_CHANGES_DEFAULT_LIMIT: usize = 100;
const APP_CHANGES_MAX_LIMIT: usize = 500;

pub(super) struct AppChangesRequest {
    pub after: u64,
    pub limit: usize,
    pub target: Option<String>,
}

impl AppChangesRequest {
    pub(super) fn parse(body: &[u8]) -> Result<Self, &'static str> {
        #[derive(Deserialize)]
        struct Body {
            #[serde(default)]
            since_cursor: Option<String>,
            #[serde(default)]
            limit: Option<usize>,
            #[serde(default)]
            target: Option<String>,
        }
        let body: Body = serde_json::from_slice(body).map_err(|_| "invalid app changes request")?;
        let after = match body.since_cursor.as_deref() {
            Some(cursor) => cursor
                .strip_prefix("v1-")
                .filter(|raw| raw.len() == 16)
                .and_then(|raw| u64::from_str_radix(raw, 16).ok())
                .ok_or("invalid app changes cursor")?,
            None => 0,
        };
        Ok(Self {
            after,
            limit: body
                .limit
                .unwrap_or(APP_CHANGES_DEFAULT_LIMIT)
                .clamp(1, APP_CHANGES_MAX_LIMIT),
            target: body.target,
        })
    }
}

/// The route supplies visible events and the cursor captured before access checks.
pub(super) fn reply(
    events: Vec<ChangeFeedEvent>,
    examined_cursor: u64,
    last_seq: u64,
    gap: bool,
) -> Value {
    let changes = events
        .into_iter()
        .map(|event| {
            serde_json::json!({
                "cursor": encode_change_cursor(event.seq),
                "mutation_id": event.mutation_id,
                "schema_name": event.schema,
                "key": {"hash": event.hash, "range": event.range},
                "operation": event.operation,
                "committed_at_ms": event.committed_at_ms,
                "background_tasks_drained": event.background_tasks_drained,
                "convergence_pending": event.convergence_pending,
            })
        })
        .collect();
    let mut response = serde_json::json!({
        "changes": [],
        "next_cursor": encode_change_cursor(examined_cursor),
        "has_more": examined_cursor < last_seq,
        "gap": gap,
    });
    // Move the complete array instead of serializing a second copy of its values.
    response["changes"] = Value::Array(changes);
    response
}

fn encode_change_cursor(seq: u64) -> String {
    format!("v1-{seq:016x}")
}
