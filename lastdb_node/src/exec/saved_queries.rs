//! Owner-only UDS routes for saving and reconciling keyed queries.

use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::json;

use super::reply::{render, HostError};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::saved_query::MAX_STORED_TEXT;
use crate::saved_query_run::{self, SaveRequest};

fn route_error(err: String) -> HostError {
    if let Some(rest) = err.strip_prefix("HTTP ") {
        if let Some((code, msg)) = rest.split_once(": ") {
            if let Ok(status) = code.parse::<u16>() {
                return HostError::new(status, msg.to_owned());
            }
        }
    }
    let status = if err == "database is not registered on this node" {
        404
    } else if err == "database registry is unavailable" {
        503
    } else {
        400
    };
    HostError::new(status, err)
}

pub(super) async fn save_route(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        id: String,
        app: String,
        source: String,
        destination: String,
        spec: String,
        spec_version: u32,
        definition_json: String,
    }
    let res = async {
        if !ctx.is_owner {
            return Err(HostError::new(403, "saved queries are owner-only"));
        }
        let b: Body = serde_json::from_slice(&req.body).map_err(|e| {
            HostError::new(400, format!("invalid /api/saved-queries/save body: {e}"))
        })?;
        if b.definition_json.len() > MAX_STORED_TEXT {
            return Err(HostError::new(400, "saved query definition is too large"));
        }
        let saved = saved_query_run::save(
            host,
            &SaveRequest {
                id: b.id,
                app: b.app,
                source: b.source,
                destination: b.destination,
                spec: b.spec,
                spec_version: b.spec_version,
                definition_json: b.definition_json,
            },
        )
        .await
        .map_err(route_error)?;
        Ok(json!({
            "saved": {
                "id": saved.id,
                "source": saved.source,
                "destination": saved.destination,
                "spec_name": saved.spec_name,
                "spec_version": saved.spec_version,
            }
        }))
    }
    .await;
    render(res, ctx)
}

pub(super) async fn reconcile_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        id: String,
        source: String,
    }
    let res = async {
        if !ctx.is_owner {
            return Err(HostError::new(403, "saved queries are owner-only"));
        }
        let b: Body = serde_json::from_slice(&req.body).map_err(|e| {
            HostError::new(
                400,
                format!("invalid /api/saved-queries/reconcile body: {e}"),
            )
        })?;
        let r = saved_query_run::reconcile(host, &b.id, &b.source)
            .await
            .map_err(route_error)?;
        Ok(json!({
            "receipt": {
                "added": r.added,
                "updated": r.updated,
                "removed": r.removed,
                "unchanged": r.unchanged,
            }
        }))
    }
    .await;
    render(res, ctx)
}
