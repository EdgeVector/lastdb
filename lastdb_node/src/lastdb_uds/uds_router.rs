//! Request router and database-scope path helpers for the LastDB Mini Unix-domain socket.

use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use fold_db::access::AccessContext;

use super::uds_http::{UdsRequest, UdsResponse};

pub const API_VERSION: u32 = 34;
pub const SOCKET_FILE_NAME: &str = "folddb.sock";
pub const DATABASES_PATH: &str = "/api/databases";
pub const DATABASE_SCOPE_PREFIX: &str = "/api/databases/";

const SOCKET_TMP_SUFFIX: &str = ".tmp";
const MAX_SOCKET_PATH_LEN: usize = 103;
const CAPABILITIES_JSON: &str = r#"{"owned_app_schemas_v1":true,"atomic_owned_mutations_v1":true,"compound_alternate_keys_v1":true,"independent_hash_range_v1":true}"#;

pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SOCKET_FILE_NAME)
}

fn check_len(path: &Path) -> io::Result<()> {
    let len = path.as_os_str().len();
    if len <= MAX_SOCKET_PATH_LEN {
        return Ok(());
    }
    let msg = format!(
        "socket path is {len} bytes, exceeds the {MAX_SOCKET_PATH_LEN}-byte sockaddr_un limit"
    );
    Err(io::Error::new(io::ErrorKind::InvalidInput, msg))
}

fn tmp_socket_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map_or_else(|| SOCKET_FILE_NAME.into(), |s| s.to_string_lossy());
    path.with_file_name(format!("{name}{SOCKET_TMP_SUFFIX}"))
}

pub fn preflight_data_dir(data_dir: &Path) -> io::Result<()> {
    let worst = socket_path(data_dir);
    if tmp_socket_path(&worst).as_os_str().len() <= MAX_SOCKET_PATH_LEN {
        return Ok(());
    }
    let max_dir = MAX_SOCKET_PATH_LEN - (SOCKET_FILE_NAME.len() + SOCKET_TMP_SUFFIX.len() + 1);
    let msg = format!(
        "data dir {} is too deep to host a Unix control socket: its socket path {} is {} bytes, and binding needs {} more for the atomic temp sibling, over the {MAX_SOCKET_PATH_LEN}-byte sockaddr_un limit. Use a data dir at most {max_dir} bytes long.",
        data_dir.display(), worst.display(), worst.as_os_str().len(), SOCKET_TMP_SUFFIX.len()
    );
    Err(io::Error::new(io::ErrorKind::InvalidInput, msg))
}

fn remove_stale_socket(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_socket() => std::fs::remove_file(path),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("refusing to remove non-socket file at {}", path.display()),
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub(super) fn bind_socket_listener(path: PathBuf) -> io::Result<(PathBuf, UnixListener)> {
    let tmp = tmp_socket_path(&path);
    check_len(&path)?;
    check_len(&tmp)?;
    if UnixStream::connect(&path).is_ok() {
        let msg = format!(
            "a live node is already listening on {} — refusing to take over its socket",
            path.display()
        );
        return Err(io::Error::new(io::ErrorKind::AddrInUse, msg));
    }
    remove_stale_socket(&tmp)?;
    remove_stale_socket(&path)?;
    let listener = UnixListener::bind(&tmp)?;
    let perm = std::fs::Permissions::from_mode(0o600);
    if let Err(e) = std::fs::set_permissions(&tmp, perm).and_then(|()| std::fs::rename(&tmp, &path))
    {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok((path, listener))
}

pub(super) fn record_fd_exhaustion(
    ep: &mut Option<(Instant, Instant, u64)>,
    err: &io::Error,
) -> Duration {
    let now = Instant::now();
    let (_, last, n) = ep.get_or_insert((now, now, 0));
    *n = n.saturating_add(1);
    let shift = u32::try_from(*n - 1).unwrap_or(31).min(31);
    if *n == 1 {
        tracing::error!(error = %err, "control-socket accept hit fd exhaustion; backing off");
    } else if now.duration_since(*last) >= Duration::from_secs(30) {
        *last = now;
        tracing::warn!(error = %err, failed_accepts = *n, "control-socket accept still out of fds");
    }
    Duration::from_millis(50)
        .saturating_mul(1_u32 << shift)
        .min(Duration::from_secs(1))
}

pub fn valid_selector(selector: &str) -> bool {
    !selector.is_empty()
        && selector.len() <= 64
        && selector
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

pub fn split_database_path(target: &str) -> Option<(&str, &str)> {
    target
        .strip_prefix(DATABASE_SCOPE_PREFIX)?
        .split_once('/')
        .filter(|(s, _)| valid_selector(s))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRoute {
    Query,
    Mutation,
    MutationBatch,
    QueryBatch,
    ListSchemas,
    ListRecordKeys,
    GetSchema,
    DeclareSchema,
    SeedSystemSchema,
    DropSchema,
    Status,
    AdminShed,
    AutoIdentity,
    BootIdentity,
    DbSchemas,
    DbFlush,
    DbCleanup,
    DbNativeStatus,
    DbDeleteLedger,
    DbSchemaRetention,
    DbPutBlob,
    DbFetchBlob,
    DbDeleteBlob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRoute {
    Health,
    Version,
    Data(DataRoute),
    MethodNotAllowed { allow: &'static str },
    NotFound,
}

pub fn route(req: &UdsRequest) -> ControlRoute {
    use DataRoute::*;
    let path = req.target.split(['?', '#']).next().unwrap_or(&req.target);
    let m = req.method.as_str();
    let check = |allow: &'static str, r: ControlRoute| {
        if m == allow {
            r
        } else {
            ControlRoute::MethodNotAllowed { allow }
        }
    };
    match path {
        "/health" => return check("GET", ControlRoute::Health),
        "/api/version" => return check("GET", ControlRoute::Version),
        "/api/db/native-status" | "/api/db/cleanup" => {
            let d = if path == "/api/db/cleanup" {
                DbCleanup
            } else {
                DbNativeStatus
            };
            return if matches!(m, "GET" | "POST") {
                ControlRoute::Data(d)
            } else {
                ControlRoute::MethodNotAllowed { allow: "GET, POST" }
            };
        }
        _ => {}
    }
    let exact = match path {
        "/api/schemas" => Some(("GET", ListSchemas)),
        "/api/list" => Some(("GET", ListRecordKeys)),
        "/api/schemas/declare" => Some(("POST", DeclareSchema)),
        "/api/schemas/seed-system" => Some(("POST", SeedSystemSchema)),
        "/api/schemas/drop" => Some(("POST", DropSchema)),
        "/api/status" => Some(("GET", Status)),
        "/api/admin/shed" => Some(("POST", AdminShed)),
        "/api/db/schemas" => Some(("GET", DbSchemas)),
        "/api/db/flush" => Some(("POST", DbFlush)),
        "/api/db/delete-ledger" => Some(("GET", DbDeleteLedger)),
        "/api/db/schema-retention" => Some(("POST", DbSchemaRetention)),
        "/api/db/put-blob" => Some(("POST", DbPutBlob)),
        "/api/db/fetch-blob" => Some(("POST", DbFetchBlob)),
        "/api/db/delete-blob" => Some(("POST", DbDeleteBlob)),
        "/api/query" => Some(("POST", Query)),
        "/api/mutation" => Some(("POST", Mutation)),
        "/api/mutations/batch" => Some(("POST", MutationBatch)),
        "/api/queries/batch" => Some(("POST", QueryBatch)),
        "/api/system/auto-identity" => Some(("GET", AutoIdentity)),
        "/api/system/boot-identity" => Some(("GET", BootIdentity)),
        _ if path
            .strip_prefix("/api/schema/")
            .is_some_and(|r| !r.is_empty() && !r.contains('/')) =>
        {
            Some(("GET", GetSchema))
        }
        _ => None,
    };
    exact.map_or(ControlRoute::NotFound, |(allow, d)| {
        check(allow, ControlRoute::Data(d))
    })
}

fn instance_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        format!(
            "{}-{}",
            std::process::id(),
            fold_db::clock::unix_nanos_wide()
        )
    })
}

fn json_200(body: String) -> UdsResponse {
    UdsResponse::new(200, "OK", body.into_bytes()).with_header("Content-Type", "application/json")
}

pub fn health_response() -> UdsResponse {
    json_200(format!("{{\"status\":\"ok\",\"api_version\":{API_VERSION},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{}\"}}", instance_id()))
}

pub fn method_not_allowed_response(allow: &str) -> UdsResponse {
    UdsResponse::new(405, "Method Not Allowed", b"Method Not Allowed".to_vec())
        .with_header("Allow", allow.to_string())
}

pub fn dispatch<F: FnOnce(DataRoute, &UdsRequest, &AccessContext) -> UdsResponse>(
    req: &UdsRequest,
    ctx: &AccessContext,
    exec: F,
) -> UdsResponse {
    match route(req) {
        ControlRoute::Health => health_response(),
        ControlRoute::Version => {
            let build = serde_json::Value::String(crate::version::build_version().into());
            let (min_api, out, min_out) = (
                crate::version::MIN_SUPPORTED_API_VERSION,
                crate::version::CURRENT_OUTBOX_FORMAT_VERSION,
                crate::version::MIN_SUPPORTED_OUTBOX_FORMAT_VERSION,
            );
            json_200(format!("{{\"ok\":true,\"api_version\":{API_VERSION},\"build\":{build},\"capabilities\":{CAPABILITIES_JSON},\"instance_id\":\"{}\",\"min_supported_api_version\":{min_api},\"current_outbox_format_version\":{out},\"min_supported_outbox_format_version\":{min_out}}}", instance_id()))
        }
        ControlRoute::Data(data) => exec(data, req, ctx),
        ControlRoute::NotFound => UdsResponse::new(404, "Not Found", b"Not Found".to_vec()),
        ControlRoute::MethodNotAllowed { allow } => method_not_allowed_response(allow),
    }
}
