//! Database selection and scoped request dispatch on the owner socket.

use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;

use super::*;
use crate::host::owned::ResolvedStore;
use crate::lastdb_uds::uds_router::{self, ControlRoute};
use crate::lastdb_uds::{split_database_path, DATABASES_PATH};
use crate::registered_share::{self, RegisteredShare};

enum Selection {
    Owner,
    Owned(Arc<FoldDB>),
    Signed(Box<RegisteredShare>),
}

/// Dispatch a database registry operation, or a schema read, seed, drop, or
/// installer placement, on the explicitly selected database. Record reads and
/// writes use `/v1`.
pub(in crate::exec) async fn execute_database_request(
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
        ("GET", DATABASES_PATH) => return database_owned::list(ctx, host),
        ("POST", DATABASES_PATH) => return database_owned::create(req, ctx, host).await,
        ("POST", database_owned::SPEC_PATH) => return database_owned::bind(req, ctx, host),
        ("POST", database_owned::ACCEPT_PATH) => {
            return database_owned::accept(req, ctx, host).await
        }
        _ => {}
    }
    if let Some(selector) = submission_status_selector(req) {
        return run_submission_status(selector, req, ctx, host).await;
    }
    if let Some(selector) = submission_selector(req) {
        return run_submission(selector, req, ctx, host).await;
    }
    let Some((selector, path)) = split_database_path(&req.target) else {
        return content_free(404, "Not Found");
    };
    if path == "submissions/status" || path == "submissions" {
        return uds_router::method_not_allowed_response("POST");
    }
    if let Some(route) = super::database_share::Route::parse(path) {
        return super::database_share::execute(route, req, ctx, host, selector).await;
    }
    let selector = selector.to_owned();
    req.target = format!("/api/{path}");
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
        Selection::Signed(share) => {
            super::database_session::read_signed(route, req, ctx, host, share.as_ref()).await
        }
    }
}

/// Bounded mutation and sharing requests can leave the socket worker.
pub(in crate::exec) fn is_deferred_database_request(req: &UdsRequest) -> bool {
    submission_status_selector(req).is_some()
        || submission_selector(req).is_some()
        || split_database_path(&req.target)
            .is_some_and(|(_, path)| super::database_share::Route::parse(path).is_some())
}

pub(in crate::exec) async fn execute_deferred_database_request(
    req: &mut UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return content_free(404, "Not Found");
    }
    if let Some(selector) = submission_status_selector(req) {
        return with_handler_timeout_budget(
            ctx,
            handler_timeout(),
            run_submission_status(selector, req, ctx, host),
        )
        .await;
    }
    if let Some(selector) = submission_selector(req) {
        return with_handler_timeout_budget(
            ctx,
            handler_timeout(),
            run_submission(selector, req, ctx, host),
        )
        .await;
    }
    if let Some((selector, route)) = split_database_path(&req.target)
        .and_then(|(selector, path)| Some((selector, super::database_share::Route::parse(path)?)))
    {
        return with_handler_timeout_budget(
            ctx,
            handler_timeout(),
            super::database_share::execute(route, req, ctx, host, selector),
        )
        .await;
    }
    content_free(404, "Not Found")
}

fn submission_status_selector(req: &UdsRequest) -> Option<&str> {
    let (selector, path) = split_database_path(&req.target)?;
    (req.method == "POST" && path == "submissions/status").then_some(selector)
}

fn submission_selector(req: &UdsRequest) -> Option<&str> {
    let (selector, path) = split_database_path(&req.target)?;
    (req.method == "POST" && path == "submissions").then_some(selector)
}

async fn run_submission_status(
    selector: &str,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match resolve_database(selector, ctx, host) {
        Ok(Selection::Signed(share)) => {
            super::database_session::submission_status(req, ctx, host, &share).await
        }
        Ok(_) => error_response(404, "this database has no received submissions", ctx),
        Err(response) => response,
    }
}

async fn run_submission(
    selector: &str,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match resolve_database(selector, ctx, host) {
        Ok(Selection::Signed(share)) => {
            super::database_session::submit_signed(req, ctx, host, &share).await
        }
        // An owned database is not a receiver. The route does not write a record.
        Ok(_) => error_response(404, "this database has no received submissions", ctx),
        Err(response) => response,
    }
}

fn resolve_database(
    selector: &str,
    ctx: &AccessContext,
    host: &Host,
) -> Result<Selection, UdsResponse> {
    match host.owned.resolve(selector) {
        Ok(ResolvedStore::Owner) => Ok(Selection::Owner),
        Ok(ResolvedStore::Owned(store)) => Ok(Selection::Owned(store)),
        Ok(ResolvedStore::Absent) => resolve_share(selector, ctx, host),
        Err(error) => Err(render(Err(error.into()), ctx)),
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

fn is_database_operation(route: DataRoute) -> bool {
    matches!(route, DataRoute::GetSchema | DataRoute::ListSchemas)
}

fn is_scoped_operation(route: DataRoute) -> bool {
    is_database_operation(route)
        || matches!(
            route,
            DataRoute::SeedSystemSchema | DataRoute::DropSchema | DataRoute::AppsDeclareSchema
        )
}
