//! Scoped database session tokens and received signed-log route handlers.

use cloud_backup_protocol::Id;

use super::*;
use crate::host::SESSION_LIFETIME as LIFETIME;
use crate::lastdb_uds::{split_database_path, uds_router};
use crate::registered_share::{self, RegisteredShare};

const HEADER: &str = "X-LastDB-Database-Session";
const PATH: &str = "/api/databases/session";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionRequest {
    database_id: Id,
    intent: Intent,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Intent {
    Read,
    Write,
}

pub(super) fn is_issue(req: &UdsRequest) -> bool {
    req.target == PATH
}

pub(super) fn issue(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if req.method != "POST" {
        return uds_router::method_not_allowed_response("POST");
    }
    let result = (|| {
        let body = decode(req)?;
        let write = matches!(body.intent, Intent::Write);
        let write_allowed = allows_write_session(host, &body.database_id)?;
        if write && !write_allowed {
            return Err(HostError::new(
                403,
                "this database permits local read sessions only",
            ));
        }
        let token = host
            .database_sessions
            .issue(body.database_id, write)
            .ok_or_else(|| HostError::new(503, "database session limit reached"))?;
        Ok(json!({"session": token, "expiresInSeconds": LIFETIME.as_secs()}))
    })();
    render(result, ctx)
}

fn decode(req: &UdsRequest) -> Result<SessionRequest, HostError> {
    if req.body.len() > 1024 {
        return Err(HostError::new(400, "invalid database session request"));
    }
    serde_json::from_slice(&req.body)
        .map_err(|_| HostError::new(400, "invalid database session request"))
}

fn allows_write_session(host: &Host, id: &Id) -> Result<bool, HostError> {
    use crate::host::owned::Resolved;

    let (resolved, _) = host.owned.resolve(id.as_str()).map_err(|error| {
        let (status, reason) = error.status();
        HostError::new(status, reason)
    })?;
    match resolved {
        Resolved::Owned => Ok(true),
        Resolved::Owner => Err(HostError::new(404, "a session requires a shared database")),
        Resolved::Absent => match registered_share::resolve(&host.home, id.as_str()) {
            Ok(Some(share)) => Ok(share.whole_database()
                && crate::log_sync::received_access::view(&host.home, id.as_str()).can_submit()),
            Ok(None) => Err(HostError::new(404, "the database is not registered")),
            Err(_) => Err(HostError::new(503, "database registry is unavailable")),
        },
    }
}

/// The owner socket remains an owner boundary. A request carrying a session
/// header can only reach the selected database and its allowlisted routes.
pub(in crate::exec) fn admit(req: &UdsRequest, host: &Host) -> bool {
    let Some(token) = req.header(HEADER) else {
        return true;
    };
    let Some((database, write)) = host.database_sessions.get(token) else {
        return false;
    };
    if req.method == "GET" && req.target == "/api/version" {
        return true;
    }
    let Some((id, path)) = split_database_path(&req.target) else {
        return false;
    };
    let path = path.split(['?', '#']).next().unwrap_or_default();
    database.as_str() == id && allowed(&req.method, path, write)
}

fn allowed(method: &str, path: &str, write: bool) -> bool {
    match (method, path) {
        ("POST", "query" | "queries/batch" | "db/fetch-blob" | "submissions/status")
        | ("GET", "schemas" | "schema" | "list") => true,
        ("GET", path) if path.starts_with("schema/") => true,
        ("POST", "mutation" | "mutations/batch" | "db/put-blob" | "db/delete-blob") => write,
        _ => false,
    }
}

pub(super) async fn submit_signed(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    use crate::log_sync::ContributionError;
    if !share.whole_database() {
        return error_response(403, "this share is read-only", ctx);
    }
    if !replay_durable(host, share) {
        return error_response(503, "database snapshot is not durable", ctx);
    }
    let Some(id) = req
        .header("Idempotency-Key")
        .and_then(|value| Id::try_from(value.to_owned()).ok())
    else {
        return error_response(400, "a shared write requires a UUID Idempotency-Key", ctx);
    };
    match crate::log_sync::submit_share_write(host, share, &id, &req.body).await {
        Ok(sequence) => {
            let mut response = json_ok(&json!({
                "ok": true, "status": "submitted", "mutation_id": id,
                "submission_sequence": sequence,
            }));
            if response.status == 200 {
                response.status = 202;
                response.reason = "Accepted";
            }
            response
        }
        Err(ContributionError::Invalid) => {
            error_response(400, "invalid shared mutation or schema definition", ctx)
        }
        Err(ContributionError::Forbidden) => {
            error_response(403, "this member cannot submit this shared mutation", ctx)
        }
        Err(ContributionError::Unavailable) => {
            error_response(503, "shared mutation was not confirmed", ctx)
        }
    }
}

pub(super) async fn read_signed(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    if !replay_durable(host, share) {
        return error_response(503, "database snapshot is not durable", ctx);
    }
    let Ok(store) = registered_share::receiver_store(host, &share.id).await else {
        return error_response(503, "database store is unavailable", ctx);
    };
    let response = host
        .with_records(
            store,
            super::execute_data_route_with_timeout(route, req, ctx, host),
        )
        .await;
    if !replay_durable(host, share) {
        return error_response(503, "database snapshot is not durable", ctx);
    }
    response
}

pub(super) async fn submission_status(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    share: &RegisteredShare,
) -> UdsResponse {
    use crate::log_sync::ContributionError;
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct StatusRequest {
        mutation_id: Id,
    }
    if req.body.len() > 256 {
        return error_response(400, "invalid submission status request", ctx);
    }
    let Ok(body) = serde_json::from_slice::<StatusRequest>(&req.body) else {
        return error_response(400, "invalid submission status request", ctx);
    };
    match crate::log_sync::share_write_status(host, share, &body.mutation_id).await {
        Ok(status) => {
            json_ok(&json!({"ok": true, "mutation_id": body.mutation_id, "status": status}))
        }
        Err(ContributionError::Forbidden) => {
            error_response(403, "submission status access refused", ctx)
        }
        Err(ContributionError::Invalid) => {
            error_response(400, "invalid submission status request", ctx)
        }
        Err(ContributionError::Unavailable) => {
            error_response(503, "submission status is unavailable", ctx)
        }
    }
}

fn replay_durable(host: &Host, share: &RegisteredShare) -> bool {
    crate::database_replay::prefix_durable(&host.home, share.id.as_str())
}
