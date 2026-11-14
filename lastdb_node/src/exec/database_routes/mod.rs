//! Database selection and scoped request dispatch on the owner socket.

mod catalog;

use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;

use super::*;
use crate::host::owned::{self, Resolved};
use crate::lastdb_uds::uds_router::{self, ControlRoute};
use crate::lastdb_uds::{split_database_path, DATABASES_PATH};
use crate::registered_share::{self, RegisteredShare};

enum Selection {
    Owner,
    Owned(Arc<FoldDB>),
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
    if super::database_session::is_issue(req) {
        return super::database_session::issue(req, ctx, host);
    }
    match (req.method.as_str(), req.target.as_str()) {
        ("GET", DATABASES_PATH) => return catalog::list(ctx, host),
        ("POST", DATABASES_PATH) => return database_owned::create(req, ctx, host).await,
        ("POST", database_owned::SPEC_PATH) => return database_owned::bind(req, ctx, host),
        ("POST", catalog::ACCEPT_PATH) => return catalog::accept(req, ctx, host).await,
        _ => {}
    }
    let Some((selector, path)) = split_database_path(&req.target) else {
        return content_free(404, "Not Found");
    };
    if let Some(route) = super::database_share::Route::parse(path) {
        return super::database_share::execute(route, req, ctx, host, selector).await;
    }
    let selector = selector.to_owned();
    req.target = format!("/api/{path}");
    if req.method == "POST" && req.target == "/api/contributors" {
        return error_response(
            400,
            "contributor grants are managed on the signed database",
            ctx,
        );
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
    if matches!(selection, Selection::Signed(_)) && !is_database_operation(route) {
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
        Selection::Signed(share) => match route {
            DataRoute::Mutation => error_response(403, "this share is read-only", ctx),
            DataRoute::MutationBatch | DataRoute::DbPutBlob | DataRoute::DbDeleteBlob => {
                error_response(403, "operation is unavailable on a received database", ctx)
            }
            _ => super::database_signed::read(route, req, ctx, host, share.as_ref()).await,
        },
    }
}

/// Bounded mutation and sharing requests can leave the socket worker.
pub fn is_deferred_database_request(req: &UdsRequest) -> bool {
    scoped_mutation(req).is_some()
        || split_database_path(&req.target)
            .is_some_and(|(_, path)| super::database_share::Route::parse(path).is_some())
}

pub fn execute_deferred_database_request<'a>(
    req: &'a mut UdsRequest,
    ctx: &'a AccessContext,
    host: &'a Host,
) -> ErasedRoute<'a> {
    Box::pin(async move {
        if !ctx.is_owner {
            return content_free(404, "Not Found");
        }
        if let Some((selector, route)) =
            split_database_path(&req.target).and_then(|(selector, path)| {
                Some((selector, super::database_share::Route::parse(path)?))
            })
        {
            return with_handler_timeout_budget(
                ctx,
                handler_timeout(),
                super::database_share::execute(route, req, ctx, host, selector),
            )
            .await;
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
            Ok(Selection::Signed(_)) => match route {
                DataRoute::Mutation => error_response(403, "this share is read-only", ctx),
                _ => error_response(403, "operation is unavailable on a received database", ctx),
            },
            Err(response) => response,
        }
    })
}

fn scoped_mutation(req: &UdsRequest) -> Option<(&str, &str, DataRoute)> {
    if req.method != "POST" {
        return None;
    }
    let (selector, path) = split_database_path(&req.target)?;
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
        Ok(Some(_) | None) => Err(error_response(
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

pub(super) fn owned_response(error: owned::OwnedError, ctx: &AccessContext) -> UdsResponse {
    let (status, message) = error.status();
    error_response(status, message, ctx)
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
