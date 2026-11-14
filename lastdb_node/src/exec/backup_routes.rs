//! Owner-only signed cloud snapshot request and completion-poll route.

use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::json;

use super::reply::{render, HostError};
use super::wire;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

pub(super) async fn backup_snapshot(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        database: String,
    }
    let res = async {
        if !ctx.is_owner {
            return Err(HostError::new(403, "backup snapshot is owner-only"));
        }
        if req.method == "GET" {
            let db = wire::query_value(&req.target, "database").unwrap_or_default();
            let nonce = wire::query_value(&req.target, "nonce").unwrap_or_default();
            if db.is_empty() || nonce.is_empty() {
                return Err(HostError::new(
                    400,
                    "database and nonce query parameters are required",
                ));
            }
            cloud_backup_protocol::Id::try_from(db.clone())
                .map_err(|e| HostError::new(400, e.to_string()))?;
            if let Some((through, entries)) =
                crate::log_sync::snapshot_completed(&host.home, &db, &nonce)
            {
                return Ok(json!({
                    "ready": true,
                    "snapshot_through": through,
                    "log_head": through,
                    "log_entries": entries,
                }));
            }
            return Ok(match crate::log_sync::snapshot_error(&host.home, &db) {
                Some(error) => json!({ "ready": false, "error": error }),
                None => json!({ "ready": false }),
            });
        }
        let b: Body = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid /api/backup/snapshot body: {e}")))?;
        let (id, nonce) =
            crate::log_sync::request_snapshot(&host.home, &b.database).map_err(|e| {
                let status = if e == "database is not owned by this node" {
                    404
                } else if e.starts_with("cannot ") {
                    503
                } else {
                    400
                };
                HostError::new(status, e)
            })?;
        Ok(json!({ "database_id": id, "nonce": nonce }))
    }
    .await;
    render(res, ctx)
}
