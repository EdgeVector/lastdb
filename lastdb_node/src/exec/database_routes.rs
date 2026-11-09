//! Database selection at the owner socket. An explicit target never falls back.
use super::*;
use crate::registered_share::{self, RegisteredShare};
use crate::share_invitation::MAX_SEALED_INVITATION_BYTES;
use cloud_backup_protocol::Digest;
use lastdb_uds::database_path::{self, DATABASES_PATH};
use lastdb_uds::uds_router::{self, ControlRoute, SocketKind};
use serde_json::json;

const ACCEPT_PATH: &str = "/api/databases/accept";

/// Dispatch a database registry operation or route an ordinary record request
/// to the explicitly selected database on the owner socket.
pub async fn execute_database_request(
    req: &mut UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return content_free(404, "Not Found");
    }
    match (req.method.as_str(), req.target.as_str()) {
        ("GET", DATABASES_PATH) => return list(ctx, host),
        ("POST", ACCEPT_PATH) => return accept(req, ctx, host),
        _ => {}
    }
    let Some((selector, path)) = database_path::split(&req.target) else {
        return content_free(404, "Not Found");
    };
    let selector = selector.to_owned();
    req.target = format!("/api/{path}");
    if req.method == "POST" && req.target == "/api/contributors" {
        return attach_contributor(req, ctx, host, &selector);
    }
    let route = match uds_router::route(req, SocketKind::Owner) {
        ControlRoute::Data(route) if is_database_operation(route) => route,
        ControlRoute::MethodNotAllowed { allow } => {
            return uds_router::method_not_allowed_response(allow);
        }
        _ => return content_free(404, "Not Found"),
    };
    let Ok(id) = local_id(host) else {
        return error_response(503, "local database identity is unavailable", ctx);
    };
    if id == selector {
        return super::execute_data_route_with_timeout(route, req, ctx, host).await;
    }
    let share = match registered_share::resolve(&host.home, &selector) {
        Ok(Some(share)) => share,
        Ok(None) => return error_response(404, "database is not registered on this node", ctx),
        Err(crate::share_cache::ShareCacheError::Invalid("database name")) => {
            return error_response(400, "invalid database selector", ctx);
        }
        Err(_) => return error_response(503, "database registry is unavailable", ctx),
    };
    let work = async {
        match route {
            DataRoute::Mutation => super::database_mutation::mutate(req, ctx, &share).await,
            DataRoute::MutationBatch | DataRoute::DbPutBlob | DataRoute::DbDeleteBlob => {
                error_response(403, "operation is unavailable on a received database", ctx)
            }
            _ => super::database_read::read(route, req, ctx, &share),
        }
    };
    super::dispatch::execute_observed_route(route, req, ctx, host, work).await
}

fn is_database_operation(route: DataRoute) -> bool {
    matches!(
        route,
        DataRoute::Query
            | DataRoute::QueryBatch
            | DataRoute::GetSchema
            | DataRoute::ListSchemas
            | DataRoute::ListRecordKeys
            | DataRoute::DbFetchBlob
            | DataRoute::Mutation
            | DataRoute::MutationBatch
            | DataRoute::DbPutBlob
            | DataRoute::DbDeleteBlob
    )
}

fn local_id(host: &Host) -> Result<String, crate::share_cache::ShareCacheError> {
    Ok(registered_share::local_publication_id(&host.home)?
        .map_or_else(|| "personal".to_owned(), |id| id.as_str().to_owned()))
}

fn list(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let (Ok(id), Ok(shares)) = (local_id(host), registered_share::list(&host.home)) else {
        return error_response(503, "database registry is unavailable", ctx);
    };
    let mut databases =
        vec![json!({ "id": id, "name": null, "access": "owner", "state": "ready" })];
    databases.extend(shares.into_iter().map(|share| database(&share)));
    json_ok(&envelope(&json!({ "databases": databases }), &ctx.user_id))
}

fn database(share: &RegisteredShare) -> Value {
    let mut value = json!(share);
    value["state"] = json!(if share.snapshot.is_some() {
        "ready"
    } else {
        "restoring"
    });
    value["access"] = json!(if share.has_unexpired_contributor_grant() {
        "contributor"
    } else {
        "reader"
    });
    value
}

fn accept(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        invitation: String,
        publisher: Digest,
        name: Option<String>,
    }
    let Ok(body) = decode::<Request>(req, MAX_SEALED_INVITATION_BYTES) else {
        return error_response(400, "invalid database invitation request", ctx);
    };
    let Ok(sealed) = decode_sealed(&body.invitation, MAX_SEALED_INVITATION_BYTES) else {
        return error_response(400, "invalid encoded invitation", ctx);
    };
    let result =
        registered_share::accept(&host.home, &sealed, &body.publisher, body.name.as_deref());
    registration_reply(result, ctx)
}

fn attach_contributor(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    selector: &str,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        grant: String,
    }
    const LIMIT: usize = 64 * 1024;
    let Ok(body) = decode::<Request>(req, LIMIT) else {
        return error_response(400, "invalid contributor attachment request", ctx);
    };
    let Ok(sealed) = decode_sealed(&body.grant, LIMIT) else {
        return error_response(400, "invalid encoded contributor grant", ctx);
    };
    registration_reply(
        registered_share::attach_contributor(&host.home, selector, &sealed),
        ctx,
    )
}

fn decode<T: serde::de::DeserializeOwned>(req: &UdsRequest, limit: usize) -> Result<T, ()> {
    if req.body.len() > limit.div_ceil(3) * 4 + 512 {
        return Err(());
    }
    serde_json::from_slice(&req.body).map_err(|_| ())
}

fn decode_sealed(text: &str, limit: usize) -> Result<Vec<u8>, ()> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|_| ())?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err(());
    }
    Ok(bytes)
}

fn registration_reply(
    result: Result<RegisteredShare, crate::share_cache::ShareCacheError>,
    ctx: &AccessContext,
) -> UdsResponse {
    use crate::share_cache::ShareCacheError;
    match result {
        Ok(share) => json_ok(&envelope(
            &json!({ "database": database(&share) }),
            &ctx.user_id,
        )),
        Err(error @ (ShareCacheError::Invalid(_) | ShareCacheError::Invitation(_))) => {
            error_response(400, &error.to_string(), ctx)
        }
        Err(ShareCacheError::Unavailable) => error_response(404, "database is not registered", ctx),
        Err(_) => error_response(503, "database registration is unavailable", ctx),
    }
}
