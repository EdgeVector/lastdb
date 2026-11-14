//! Owner-approved, short-lived access for a single named content database.

use cloud_backup_protocol::Id;

use super::*;
use crate::host::database_sessions::LIFETIME;
use crate::lastdb_uds::{split_database_path, uds_router};

const HEADER: &str = "X-LastDB-Database-Session";
const PATH: &str = "/api/databases/session";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
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
        let owned = is_owned(host, &body.database_id)?;
        if write && !owned {
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

fn decode(req: &UdsRequest) -> Result<Request, HostError> {
    if req.body.len() > 1024 {
        return Err(HostError::new(400, "invalid database session request"));
    }
    serde_json::from_slice(&req.body)
        .map_err(|_| HostError::new(400, "invalid database session request"))
}

fn is_owned(host: &Host, id: &Id) -> Result<bool, HostError> {
    use crate::host::owned::Resolved;

    let (resolved, _) = host.owned.resolve(id.as_str()).map_err(|error| {
        let (status, reason) = error.status();
        HostError::new(status, reason)
    })?;
    match resolved {
        Resolved::Owned => Ok(true),
        Resolved::Owner => Err(HostError::new(404, "a session requires a shared database")),
        Resolved::Absent => match crate::registered_share::resolve(&host.home, id.as_str()) {
            Ok(Some(_)) => Ok(false),
            Ok(None) => Err(HostError::new(404, "the database is not registered")),
            Err(_) => Err(HostError::new(503, "database registry is unavailable")),
        },
    }
}

/// The owner socket remains an owner boundary. A request carrying a session
/// header can only reach the selected database and its allowlisted routes.
pub fn admit(req: &UdsRequest, host: &Host) -> bool {
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
        ("POST", "query" | "queries/batch" | "db/fetch-blob")
        | ("GET", "schemas" | "schema" | "list") => true,
        ("GET", path) if path.starts_with("schema/") => true,
        ("POST", "mutation" | "mutations/batch" | "db/put-blob" | "db/delete-blob") => write,
        _ => false,
    }
}
