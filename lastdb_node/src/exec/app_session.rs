//! App grants. The owner registers one app on the owner socket. The value in
//! `X-LastDB-App-Session` is the app id. The app socket admits only a request
//! that names a registered app, serves it only the app API, and holds it to
//! the grant.

use std::collections::BTreeSet;
use std::sync::Arc;

use fold_db::access::AccessContext;
use serde::Deserialize;
use serde_json::json;

use super::app_api;
use super::reply::{render, HostError};
use crate::host::app_sessions::{AppGrant, RegisterError};
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
    /// Absolute path of `<apps root>/<app>/data`. Absent keeps today's path reply.
    #[serde(default)]
    data: Option<String>,
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

/// `POST /api/apps/session` on the owner socket: register one app.
/// The reply's `session` is the app id. There is no expiry.
pub(in crate::exec) fn issue_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let result = (|| {
        if !ctx.is_owner {
            return Err(HostError::new(403, "the owner registers an app"));
        }
        if req.body.len() > MAX_ISSUE_BODY {
            return Err(HostError::new(413, "app registration request is too large"));
        }
        let body: IssueBody = serde_json::from_slice(&req.body).map_err(|error| {
            HostError::new(400, format!("invalid app registration request: {error}"))
        })?;
        if !app_identity_crypto::valid_app_id(&body.app) {
            return Err(HostError::new(400, "invalid app id"));
        }
        let data_dir = match body.data.as_deref() {
            None => None,
            Some(data) => Some(super::app_data::accept(&host.home, &body.app, data)?),
        };
        let app = body.app.clone();
        let AccessBody {
            read,
            write,
            extensions,
        } = body.access;
        host.app_sessions
            .register(AppGrant::new(body.app, read, write, extensions, data_dir))
            .map_err(register_error)?;
        Ok(json!({ "session": app }))
    })();
    render(result, ctx)
}

fn register_error(error: RegisterError) -> HostError {
    match error {
        RegisterError::Invalid => HostError::new(400, "invalid app grant"),
        RegisterError::Limit => HostError::new(503, "app grant limit reached"),
        RegisterError::Store => HostError::internal("could not store the app grant"),
    }
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
        .and_then(|app_id| host.app_sessions.get(app_id))
    else {
        return app_api::refuse(403, "the app socket needs a registered app");
    };
    let ControlRoute::Data(route @ (DataRoute::V1Read | DataRoute::V1Write | DataRoute::V1Changes)) =
        uds_router::route(req)
    else {
        return app_api::refuse(404, "the app socket serves only the app API");
    };
    handle.block_on(ACTIVE_APP.scope(
        grant,
        super::execute_data_route_with_timeout(route, req, ctx, host),
    ))
}
