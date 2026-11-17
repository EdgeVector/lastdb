//! App sessions. The owner issues one for an installed app on the owner
//! socket. The app socket admits only a request that carries one, serves it
//! only the app API, and holds it to what the app declared.

use std::collections::BTreeSet;
use std::sync::Arc;

use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::json;

use super::app_api;
use super::reply::{render, HostError};
use crate::host::app_sessions::{AppGrant, APP_SESSION_LIFETIME};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::{self, ControlRoute, DataRoute};

const HEADER: &str = "X-LastDB-App-Session";
const MAX_ISSUE_BODY: usize = 256 * 1024;

tokio::task_local! {
    static ACTIVE_APP: Arc<AppGrant>;
}

/// The app a request belongs to. `None` on the owner socket, which is not limited.
pub(in crate::exec) fn active_app() -> Option<Arc<AppGrant>> {
    ACTIVE_APP.try_with(Arc::clone).ok()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueBody {
    app: String,
    #[serde(default)]
    access: AccessBody,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccessBody {
    #[serde(default)]
    read: BTreeSet<String>,
    #[serde(default)]
    write: BTreeSet<String>,
    #[serde(default)]
    extensions: BTreeSet<String>,
}

/// `POST /api/apps/session` on the owner socket: a session for one app.
pub(in crate::exec) fn issue_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let result = (|| {
        if !ctx.is_owner {
            return Err(HostError::new(403, "an app session is issued by the owner"));
        }
        if req.body.len() > MAX_ISSUE_BODY {
            return Err(HostError::new(413, "app session request is too large"));
        }
        let body: IssueBody = serde_json::from_slice(&req.body).map_err(|error| {
            HostError::new(400, format!("invalid app session request: {error}"))
        })?;
        if !crate::profile::valid_label_64(&body.app) {
            return Err(HostError::new(400, "invalid app id"));
        }
        let AccessBody {
            read,
            write,
            extensions,
        } = body.access;
        let token = host
            .app_sessions
            .issue(AppGrant::new(body.app, read, write, extensions))
            .ok_or_else(|| HostError::new(503, "app session limit reached"))?;
        Ok(json!({
            "session": token,
            "expires_in_seconds": APP_SESSION_LIFETIME.as_secs(),
        }))
    })();
    render(result, ctx)
}

/// Serve one request on the app socket.
pub(in crate::exec) fn serve(
    handle: &tokio::runtime::Handle,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let Some(grant) = req
        .header(HEADER)
        .and_then(|token| host.app_sessions.get(token))
    else {
        return app_api::refuse(403, "the app socket needs a valid app session");
    };
    let route = match uds_router::route(req) {
        ControlRoute::Data(
            route @ (DataRoute::V1Read | DataRoute::V1Write | DataRoute::V1Changes),
        ) => route,
        _ => return app_api::refuse(404, "the app socket serves only the app API"),
    };
    handle.block_on(ACTIVE_APP.scope(
        grant,
        super::execute_data_route_with_timeout(route, req, ctx, host),
    ))
}
