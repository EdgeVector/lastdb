//! `POST /v1/x/changes`: the app API extension that reads the numbered list of
//! changed record keys.

use fold_db::access::AccessContext;
use fold_db::db_operations::CHANGES_PAGE;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{bad, fail, in_database, ok};
use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangesBody {
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    after: u64,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    schemas: Vec<String>,
}

pub(in crate::exec) async fn changes_route(
    req: &UdsRequest,
    _ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match changes(host, &req.body).await {
        Ok(body) => ok(&body),
        Err(error) => fail(&error),
    }
}

async fn changes(host: &Host, body: &[u8]) -> Result<Value, HostError> {
    let request: ChangesBody = serde_json::from_slice(body)
        .map_err(|error| bad(format!("invalid /v1/x/changes body: {error}")))?;
    if request.schemas.iter().any(String::is_empty) {
        return Err(bad("a schema name must not be empty"));
    }
    // Zero reads the head without entries.
    let limit = request.limit.map_or(CHANGES_PAGE, |n| n.min(CHANGES_PAGE));
    in_database(request.database.as_deref(), host, async {
        // Entries carry the runtime schema name. A name that resolves to no
        // schema stays as written and matches nothing.
        let mut schemas = Vec::with_capacity(request.schemas.len());
        for name in &request.schemas {
            schemas.push(resolve_schema_name(host, name)?.unwrap_or_else(|| name.clone()));
        }
        let page = host
            .records()
            .db_ops()
            .changes_after(request.after, limit, &schemas)
            .await?;
        let entries: Vec<Value> = page
            .changes
            .iter()
            .map(|change| {
                json!({
                    "seq": change.seq, "schema": change.schema,
                    "key": { "hash": change.hash, "range": change.range },
                    "deleted": change.deleted,
                })
            })
            .collect();
        Ok(json!({
            "id": page.id, "head": page.head, "through": page.through,
            "gap": page.gap, "changes": entries,
        }))
    })
    .await
}
