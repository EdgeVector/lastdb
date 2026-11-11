//! Database selection at the owner socket. An explicit target never falls back.
use super::database_owned;
use super::*;
use crate::host::owned::{self, Resolved};
use crate::lastdb_uds::database_path::{self, DATABASES_PATH};
use crate::lastdb_uds::uds_router::{self, ControlRoute};
use crate::registered_share::{self, RegisteredShare};
use crate::share_invitation::MAX_SEALED_INVITATION_BYTES;
use cloud_backup_protocol::Digest;
use fold_db::fold_db_core::FoldDB;
use serde_json::json;
use std::sync::Arc;

const ACCEPT_PATH: &str = "/api/databases/accept";

enum Selection {
    Owner,
    Owned(Arc<FoldDB>),
    Share(Box<RegisteredShare>),
    Signed(Box<RegisteredShare>),
}

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
        ("POST", DATABASES_PATH) => return database_owned::create(req, ctx, host).await,
        ("POST", database_owned::SPEC_PATH) => return database_owned::bind(req, ctx, host),
        ("POST", ACCEPT_PATH) => return accept(req, ctx, host).await,
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
    let route = match uds_router::route(req) {
        ControlRoute::Data(route) if is_scoped_operation(route) => route,
        ControlRoute::MethodNotAllowed { allow } => {
            return uds_router::method_not_allowed_response(allow);
        }
        _ => return content_free(404, "Not Found"),
    };
    let selection = match resolve_database(&selector, ctx, host) {
        Ok(selection) => selection,
        Err(response) => return response,
    };
    if matches!(selection, Selection::Share(_) | Selection::Signed(_))
        && !is_database_operation(route)
    {
        return content_free(404, "Not Found");
    }
    dispatch(route, req, ctx, host, selection).await
}

async fn dispatch(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    selection: Selection,
) -> UdsResponse {
    match selection {
        Selection::Owner => super::execute_data_route_with_timeout(route, req, ctx, host).await,
        Selection::Owned(store) => {
            host.with_records(
                store,
                super::execute_data_route_with_timeout(route, req, ctx, host),
            )
            .await
        }
        Selection::Share(share) => received_share(route, req, ctx, share).await,
        Selection::Signed(share) => match route {
            DataRoute::Mutation | DataRoute::MutationBatch => {
                received_mutation(route, req, ctx, share.as_ref()).await
            }
            DataRoute::DbPutBlob | DataRoute::DbDeleteBlob => {
                error_response(403, "operation is unavailable on a received database", ctx)
            }
            _ => super::database_signed::read(route, req, ctx, host, share.as_ref()).await,
        },
    }
}

async fn received_share(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    share: Box<RegisteredShare>,
) -> UdsResponse {
    match route {
        DataRoute::Mutation | DataRoute::MutationBatch => {
            received_mutation(route, req, ctx, share.as_ref()).await
        }
        DataRoute::DbPutBlob | DataRoute::DbDeleteBlob => {
            error_response(403, "operation is unavailable on a received database", ctx)
        }
        _ => super::database_read::read(route, req, ctx, share.as_ref()),
    }
}

/// Only bounded, Send mutation futures may leave the socket worker.
pub fn is_deferred_database_mutation(req: &UdsRequest) -> bool {
    scoped_mutation(req).is_some()
}

pub fn execute_deferred_database_mutation<'a>(
    req: &'a mut UdsRequest,
    ctx: &'a AccessContext,
    host: &'a Host,
) -> ErasedRoute<'a> {
    Box::pin(async move {
        if !ctx.is_owner {
            return content_free(404, "Not Found");
        }
        let Some((selector, path, route)) = scoped_mutation(req) else {
            return content_free(404, "Not Found");
        };
        let selector = selector.to_owned();
        req.target = format!("/api/{path}");
        match resolve_database(&selector, ctx, host) {
            Ok(Selection::Owner) => execute_deferred_mutation(route, req, ctx, host).await,
            Ok(Selection::Owned(store)) => {
                host.with_records(store, execute_deferred_mutation(route, req, ctx, host))
                    .await
            }
            Ok(Selection::Share(share) | Selection::Signed(share)) => {
                with_handler_timeout_budget(
                    ctx,
                    data_route_budget(route),
                    received_mutation(route, req, ctx, share.as_ref()),
                )
                .await
            }
            Err(response) => response,
        }
    })
}

fn scoped_mutation(req: &UdsRequest) -> Option<(&str, &str, DataRoute)> {
    if req.method != "POST" {
        return None;
    }
    let (selector, path) = database_path::split(&req.target)?;
    let route = match path.split(['?', '#']).next()? {
        "mutation" => DataRoute::Mutation,
        "mutations/batch" => DataRoute::MutationBatch,
        _ => return None,
    };
    Some((selector, path, route))
}

fn resolve_database(
    selector: &str,
    ctx: &AccessContext,
    host: &Host,
) -> Result<Selection, UdsResponse> {
    match host.owned.resolve(selector) {
        Ok((Resolved::Owner, _)) => Ok(Selection::Owner),
        Ok((Resolved::Owned, Some(store))) => Ok(Selection::Owned(store)),
        Ok((Resolved::Owned, None) | (Resolved::Absent, Some(_))) => {
            Err(error_response(503, "database registry is unavailable", ctx))
        }
        Ok((Resolved::Absent, None)) => resolve_share(selector, ctx, host),
        Err(error) => Err(owned_response(error, ctx)),
    }
}

fn resolve_share(
    selector: &str,
    ctx: &AccessContext,
    host: &Host,
) -> Result<Selection, UdsResponse> {
    match registered_share::resolve(&host.home, selector) {
        Ok(Some(share)) if share.signed_log => Ok(Selection::Signed(Box::new(share))),
        Ok(Some(share)) => Ok(Selection::Share(Box::new(share))),
        Ok(None) => Err(error_response(
            404,
            "database is not registered on this node",
            ctx,
        )),
        Err(crate::share_cache::ShareCacheError::Invalid("database name")) => {
            Err(error_response(400, "invalid database selector", ctx))
        }
        Err(_) => Err(error_response(503, "database registry is unavailable", ctx)),
    }
}

fn owned_response(error: owned::OwnedError, ctx: &AccessContext) -> UdsResponse {
    let (status, message) = error.status();
    error_response(status, message, ctx)
}

async fn received_mutation(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    share: &RegisteredShare,
) -> UdsResponse {
    if route == DataRoute::Mutation {
        super::database_mutation::mutate(req, ctx, share).await
    } else {
        error_response(403, "operation is unavailable on a received database", ctx)
    }
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

fn is_scoped_operation(route: DataRoute) -> bool {
    is_database_operation(route)
        || matches!(
            route,
            DataRoute::SeedSystemSchema | DataRoute::DeclareSchema | DataRoute::DropSchema
        )
}

fn list(ctx: &AccessContext, host: &Host) -> UdsResponse {
    let owner = match host.owned.owner() {
        Ok(entry) => entry,
        Err(error) => return owned_response(error, ctx),
    };
    let owned = match host.owned.owned_entries() {
        Ok(entries) => entries,
        Err(error) => return owned_response(error, ctx),
    };
    let Ok(shares) = registered_share::list(&host.home) else {
        return error_response(503, "database registry is unavailable", ctx);
    };
    let mut databases = vec![database_owned::row(&owner)];
    databases.extend(owned.iter().map(database_owned::row));
    databases.extend(shares.iter().map(database));
    json_ok(&envelope(&json!({ "databases": databases }), &ctx.user_id))
}

fn database(share: &RegisteredShare) -> Value {
    let mut value = json!(share);
    value["state"] = json!(if share.signed_log || share.snapshot.is_some() {
        "ready"
    } else {
        "restoring"
    });
    value["access"] = json!(if share.signed_log {
        "reader"
    } else if share.has_unexpired_contributor_grant() {
        "contributor"
    } else {
        "reader"
    });
    value["kind"] = json!("received");
    value
}

async fn accept(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Request {
        invitation: String,
        publisher: Digest,
        name: Option<String>,
        #[serde(default)]
        database_id: Option<String>,
    }
    let Ok(body) = decode::<Request>(req, MAX_SEALED_INVITATION_BYTES) else {
        return error_response(400, "invalid database invitation request", ctx);
    };
    let Ok(sealed) = decode_sealed(&body.invitation, MAX_SEALED_INVITATION_BYTES) else {
        return error_response(400, "invalid encoded invitation", ctx);
    };
    let result = match body.database_id.as_deref() {
        Some(database_id) => registered_share::accept_signed_log(
            &host.home,
            &sealed,
            &body.publisher,
            body.name.as_deref(),
            database_id,
        ),
        None => {
            registered_share::accept(&host.home, &sealed, &body.publisher, body.name.as_deref())
        }
    };
    if let Ok(share) = &result {
        if share.signed_log
            && registered_share::receiver_store(host, &share.id)
                .await
                .is_err()
        {
            return error_response(503, "database store is unavailable", ctx);
        }
    }
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
        Err(ShareCacheError::Invalid("database selector")) => {
            error_response(400, "invalid database selector", ctx)
        }
        Err(ShareCacheError::Invalid("database id")) => {
            error_response(400, "database id does not match the invitation", ctx)
        }
        Err(error @ (ShareCacheError::Invalid(_) | ShareCacheError::Invitation(_))) => {
            error_response(400, &error.to_string(), ctx)
        }
        Err(ShareCacheError::Unavailable) => error_response(404, "database is not registered", ctx),
        Err(_) => error_response(503, "database registration is unavailable", ctx),
    }
}
