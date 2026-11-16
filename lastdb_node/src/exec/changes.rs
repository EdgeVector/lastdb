//! `GET /api/changes`: the numbered list of changed record keys.

use fold_db::access::AccessContext;
use fold_db::db_operations::CHANGES_PAGE;
use serde_json::{json, Value};

use super::reply::{render, HostError};
use super::schema_routes::resolve_schema_name;
use super::wire::percent_decode;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

struct ChangesRequest {
    after: u64,
    limit: usize,
    schemas: Vec<String>,
}

impl ChangesRequest {
    fn parse(target: &str) -> Result<Self, &'static str> {
        let (mut after, mut limit, mut schemas) = (0, CHANGES_PAGE, Vec::new());
        let query = target.split_once('?').map_or("", |(_, query)| query);
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, raw) = pair.split_once('=').unwrap_or((pair, ""));
            let value = percent_decode(raw);
            match key {
                "after" => {
                    after = value
                        .parse()
                        .map_err(|_| "after must be a non-negative integer")?;
                }
                // Zero reads the head without entries.
                "limit" => {
                    limit = value
                        .parse::<usize>()
                        .map_err(|_| "limit must be a non-negative integer")?
                        .min(CHANGES_PAGE);
                }
                "schema" if value.is_empty() => return Err("schema must not be empty"),
                "schema" => schemas.push(value),
                _ => return Err("unknown changes query parameter"),
            }
        }
        Ok(Self {
            after,
            limit,
            schemas,
        })
    }
}

pub(in crate::exec) async fn changes(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let res = async {
        if !ctx.is_owner {
            return Err(HostError::new(403, "the changes list is owner-only"));
        }
        let r = ChangesRequest::parse(&req.target).map_err(|m| HostError::new(400, m))?;
        // Entries carry the runtime schema name. A name that resolves to no
        // schema stays as written and matches nothing.
        let mut schemas = Vec::with_capacity(r.schemas.len());
        for name in r.schemas {
            schemas.push(resolve_schema_name(host, &name)?.unwrap_or(name));
        }
        let page = host
            .records()
            .db_ops()
            .changes_after(r.after, r.limit, &schemas)
            .await?;
        let changes: Vec<Value> = page
            .changes
            .iter()
            .map(|c| {
                json!({
                    "seq": c.seq, "schema": c.schema, "hash": c.hash,
                    "range": c.range, "deleted": c.deleted,
                })
            })
            .collect();
        Ok(json!({
            "id": page.id, "head": page.head, "through": page.through,
            "gap": page.gap, "changes": changes,
        }))
    }
    .await;
    render(res, ctx)
}
