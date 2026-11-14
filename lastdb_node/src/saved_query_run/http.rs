//! Owner-socket calls for one source database and one destination database.
//!
//! This is the same route client the manual command uses. The daemon task
//! calls it too, so publication is not implemented a second time.

use std::borrow::Cow;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Value};

use crate::lastdb_uds::{socket_path, valid_selector, DATABASES_PATH, DATABASE_SCOPE_PREFIX};
use crate::profile::paths::{expand_tilde_path, folddb_home};
use crate::saved_query::MAX_RECORDS;
use crate::service_home::{configured_home, explicit_home_override_present};

const GET_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_TIMEOUT_SECS: u64 = 600;

pub(super) struct Store {
    socket: PathBuf,
}

impl Store {
    pub(super) fn open(data_dir: Option<PathBuf>) -> Result<Self, String> {
        let socket = resolve_socket(data_dir)?;
        Ok(Self { socket })
    }

    pub(super) fn at(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    /// `personal` stays on the unscoped owner routes. Every other selector
    /// uses the scoped database routes.
    pub(super) fn database(&self, selector: &str) -> Result<DatabaseTarget, String> {
        let selector = (selector != "personal").then_some(selector);
        DatabaseTarget::open(&self.socket, selector)
    }

    pub(super) fn list(&self) -> Result<Vec<Value>, String> {
        let target = DatabaseTarget::open(&self.socket, None)?;
        let raw = get_path(&target.socket, DATABASES_PATH, GET_TIMEOUT)?;
        let response = parse_json_response(&raw, "database list")?;
        response
            .get("databases")
            .and_then(Value::as_array)
            .cloned()
            .ok_or("missing database list".to_owned())
    }

    pub(super) fn post(
        &self,
        database: &DatabaseTarget,
        path: &str,
        body: &Value,
    ) -> Result<Value, String> {
        let raw = post_json(&database.socket, &database.path(path), body)?;
        parse_json_response(&raw, path)
    }

    pub(super) fn schema(&self, database: &DatabaseTarget, name: &str) -> Result<Value, String> {
        if probe_health(&database.socket).is_err() {
            return Err(format!(
                "lastdbd not reachable at {} — start the daemon first",
                database.socket.display()
            ));
        }
        let path = format!("/api/schema/{}", percent_encode(name));
        let response = get_path(&database.socket, &database.path(&path), GET_TIMEOUT)
            .and_then(|raw| parse_json_response(&raw, "schema"));
        match response {
            Ok(body) => body
                .get("schema")
                .cloned()
                .ok_or_else(|| "response missing schema field".to_owned()),
            Err(error) if error.contains("404") => {
                Err(format!("source schema {name} is not installed"))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn query(
        &self,
        database: &DatabaseTarget,
        schema: &str,
        filter: &Value,
    ) -> Result<Vec<Value>, String> {
        let body = json!({
            "schema_name": schema,
            "filter": filter,
            "limit": MAX_RECORDS,
        });
        let response = self.post(database, "/api/query", &body)?;
        if response.get("has_more").and_then(Value::as_bool) == Some(true) {
            return Err("saved query read exceeds 256 records".into());
        }
        response
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .ok_or("query returned no results".to_owned())
    }
}

pub(super) struct DatabaseTarget {
    pub(super) socket: PathBuf,
    selector: Option<String>,
}

impl DatabaseTarget {
    fn open(socket: &Path, selector: Option<&str>) -> Result<Self, String> {
        let selector = selector
            .map(|value| resolve_selector(socket, value))
            .transpose()?;
        Ok(Self {
            socket: socket.to_path_buf(),
            selector,
        })
    }

    pub(super) fn path<'p>(&self, path: &'p str) -> Cow<'p, str> {
        if let Some(selector) = self.selector.as_deref() {
            Cow::Owned(format!(
                "{DATABASE_SCOPE_PREFIX}{selector}{}",
                path.strip_prefix("/api")
                    .expect("database path starts with /api")
            ))
        } else {
            Cow::Borrowed(path)
        }
    }
}

pub(super) struct ListedDatabase {
    pub(super) id: String,
    pub(super) name: Option<String>,
    pub(super) kind: Option<String>,
    pub(super) access: Option<String>,
    pub(super) state: Option<String>,
    pub(super) spec_name: Option<String>,
    pub(super) spec_version: Option<u32>,
}

pub(super) fn find_database(rows: &[Value], selector: &str) -> Result<ListedDatabase, String> {
    let row = rows
        .iter()
        .find(|row| {
            row.get("id").and_then(Value::as_str) == Some(selector)
                || row.get("name").and_then(Value::as_str) == Some(selector)
        })
        .ok_or("database is not registered on this node")?;
    listed(row)
}

fn listed(row: &Value) -> Result<ListedDatabase, String> {
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or("database has no ID")?
        .to_owned();
    let (spec_name, spec_version) = listed_spec(row.get("spec"))?;
    Ok(ListedDatabase {
        id,
        name: row.get("name").and_then(Value::as_str).map(str::to_owned),
        kind: row.get("kind").and_then(Value::as_str).map(str::to_owned),
        access: row.get("access").and_then(Value::as_str).map(str::to_owned),
        state: row.get("state").and_then(Value::as_str).map(str::to_owned),
        spec_name,
        spec_version,
    })
}

fn listed_spec(spec: Option<&Value>) -> Result<(Option<String>, Option<u32>), String> {
    match spec {
        None | Some(Value::Null) => Ok((None, None)),
        Some(spec) => {
            let name = spec
                .get("name")
                .and_then(Value::as_str)
                .ok_or("destination database has no spec binding")?;
            let version = spec
                .get("version")
                .and_then(Value::as_u64)
                .ok_or("destination database has no spec binding")?;
            let version =
                u32::try_from(version).map_err(|_| "unsupported database spec version")?;
            Ok((Some(name.to_owned()), Some(version)))
        }
    }
}

fn resolve_socket(data_dir: Option<PathBuf>) -> Result<PathBuf, String> {
    let explicit = data_dir.is_some();
    let home = node_home(data_dir)?;
    match std::env::var_os("LASTDB_SOCKET_PATH").filter(|value| !value.is_empty()) {
        Some(path) if !explicit => expand_tilde_path(path),
        _ => Ok(socket_path(&home.join("data"))),
    }
}

/// Same home rule as the daemon. Kept here so this leaf client does not call the host.
fn node_home(data_dir: Option<PathBuf>) -> Result<PathBuf, String> {
    match data_dir {
        Some(dir) => expand_tilde_path(dir),
        None if explicit_home_override_present() => folddb_home(),
        None => configured_home()?.map_or_else(folddb_home, Ok),
    }
}

fn resolve_selector(socket: &Path, selector: &str) -> Result<String, String> {
    if !valid_selector(selector) {
        return Err("invalid database ID or name".into());
    }
    if selector == "personal" || cloud_backup_protocol::Id::try_from(selector.to_owned()).is_ok() {
        return Ok(selector.to_owned());
    }
    let raw = get_path(socket, DATABASES_PATH, GET_TIMEOUT)?;
    let response = parse_json_response(&raw, "database list")?;
    let list = response["databases"]
        .as_array()
        .ok_or("missing database list")?;
    let id = list
        .iter()
        .find(|db| db["name"].as_str() == Some(selector))
        .and_then(|db| db["id"].as_str())
        .filter(|id| cloud_backup_protocol::Id::try_from((*id).to_owned()).is_ok())
        .ok_or("database is not registered on this node")?;
    Ok(id.to_owned())
}

fn percent_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn admin_timeout() -> Duration {
    std::env::var("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map_or(Duration::from_secs(ADMIN_TIMEOUT_SECS), Duration::from_secs)
}

fn post_json(socket: &Path, path: &str, body: &Value) -> Result<String, String> {
    let body =
        serde_json::to_vec(body).map_err(|error| format!("failed to encode JSON: {error}"))?;
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        client_headers(),
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    request_text(socket, &request, admin_timeout())
}

fn get_path(socket: &Path, path: &str, timeout: Duration) -> Result<String, String> {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    request_text(socket, request.as_bytes(), timeout)
}

fn probe_health(socket: &Path) -> Result<(), String> {
    let raw = get_path(socket, "/health", Duration::from_secs(2))?;
    let first = raw.lines().next().unwrap_or("");
    if first.starts_with("HTTP/1.1 200") || first.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        Err(format!("unexpected /health status: {first}"))
    }
}

fn client_headers() -> String {
    format!(
        "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
        uuid::Uuid::new_v4()
    )
}

fn request_text(socket: &Path, bytes: &[u8], timeout: Duration) -> Result<String, String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|error| format_socket_error("connect to daemon socket", &error, timeout))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format_socket_error("set socket read timeout", &error, timeout))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|error| format_socket_error("set socket write timeout", &error, timeout))?;
    stream
        .write_all(bytes)
        .map_err(|error| format_socket_error("write request", &error, timeout))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| format_socket_error("read response", &error, timeout))?;
    String::from_utf8(response).map_err(|error| format!("failed to decode response: {error}"))
}

fn format_socket_error(op: &str, error: &std::io::Error, timeout: Duration) -> String {
    if matches!(
        error.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    ) {
        let secs = timeout.as_secs().max(1);
        format!(
            "client timeout after {secs}s while waiting to {op} (daemon may still be working). This is the CLI socket deadline, not node backpressure. Raise it with LASTDB_UDS_ADMIN_TIMEOUT_SECS=<secs> in the environment — the CLI and the daemon both read it, so server and client stay matched. Some subcommands also accept --timeout <secs>; not all define it."
        )
    } else {
        format!("failed to {op}: {error}")
    }
}

fn parse_json_response(response: &str, label: &str) -> Result<Value, String> {
    let status_line = response.lines().next().unwrap_or("<empty response>");
    if !response.starts_with("HTTP/1.1 200 ") {
        let body = response
            .split_once("\r\n\r\n")
            .map_or("", |(_, body)| body.trim());
        return Err(format!("{label} returned {status_line}: {body}"));
    }
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("{label} response had no body"))?;
    serde_json::from_str(body).map_err(|error| format!("invalid {label} JSON: {error}"))
}
