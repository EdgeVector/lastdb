//! Tiny socket-only LastDB operator CLI.

#[path = "lastdb/app_cmds.rs"]
mod app_cmds;
#[path = "lastdb/cli_defs.rs"]
mod cli_defs;
#[path = "lastdb/cloud_cmds.rs"]
mod cloud_cmds;
#[path = "lastdb/database_cmds.rs"]
mod database_cmds;
#[path = "lastdb/mutate_cmds.rs"]
mod mutate_cmds;
#[path = "lastdb/record_get_cmds.rs"]
mod record_get_cmds;
#[path = "lastdb/share_cmds.rs"]
mod share_cmds;

use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use app_cmds::*;
use clap::{Args, Parser, Subcommand};
use cli_defs::*;
use cloud_cmds::*;
use database_cmds::*;
use mutate_cmds::*;
use record_get_cmds::*;
use serde_json::Value;
use uuid::Uuid;

pub(crate) mod socket_client {
    pub(crate) use super::post_json_admin;
}

type RawErr = (&'static str, std::io::Error, Duration);

pub(crate) fn client_headers() -> String {
    format!(
        "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
        Uuid::new_v4()
    )
}

fn get_req(path: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    )
}

pub(crate) fn get_path(socket: &Path, path: &str, timeout: Duration) -> Result<String, String> {
    request_with_timeout(socket, get_req(path).as_bytes(), timeout)
}

pub(crate) fn get_path_retry(
    socket: &Path,
    path: &str,
    timeout: Duration,
) -> Result<String, String> {
    let (req, started) = (get_req(path), Instant::now());
    for attempt in 1..=3usize {
        let budget = timeout
            .saturating_sub(started.elapsed())
            .max(Duration::from_millis(1));
        match raw_io(socket, req.as_bytes(), budget) {
            Ok(resp) => {
                return String::from_utf8(resp)
                    .map_err(|e| format!("failed to decode response: {e}"));
            }
            Err(("read response", err, el))
                if err.kind() == ErrorKind::WouldBlock
                    && el < budget
                    && el < Duration::from_secs(1) =>
            {
                if attempt == 3 {
                    return Err("transient socket read remained unavailable after 3 attempts; retry this command. The node may be healthy but busy; this is not an outage.".into());
                }
                std::thread::sleep(Duration::from_millis(25 * attempt as u64));
            }
            Err((op, err, _)) => return Err(format_socket_io_error(op, &err, timeout)),
        }
    }
    unreachable!()
}

pub(crate) fn build_post_request(path: &str, extra_headers: &str, body: &[u8]) -> Vec<u8> {
    let (hdr, len) = (client_headers(), body.len());
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{hdr}{extra_headers}Content-Length: {len}\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    req.extend_from_slice(body);
    req
}

pub(crate) fn post_json_admin(socket: &Path, path: &str, body: &Value) -> Result<String, String> {
    post_json_with_id(
        socket,
        path,
        body,
        lastdb_node::exec::admin_handler_timeout(),
        None,
    )
}

pub(crate) fn post_json_with_timeout(
    socket: &Path,
    path: &str,
    body: &Value,
    timeout: Duration,
) -> Result<String, String> {
    post_json_with_id(socket, path, body, timeout, None)
}

pub(crate) fn post_json_with_id(
    socket: &Path,
    path: &str,
    body: &Value,
    timeout: Duration,
    id: Option<Uuid>,
) -> Result<String, String> {
    let body = serde_json::to_vec(body).map_err(|e| format!("failed to encode JSON: {e}"))?;
    let extra = if let Some(id) = id {
        format!("Idempotency-Key: {id}\r\nContent-Type: application/json\r\n")
    } else {
        "Content-Type: application/json\r\n".into()
    };
    request_with_timeout(socket, &build_post_request(path, &extra, &body), timeout)
}

pub(crate) fn request_with_timeout_bytes(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, String> {
    raw_io(socket, bytes, timeout).map_err(|(op, err, _)| format_socket_io_error(op, &err, timeout))
}

pub(crate) fn request_with_timeout(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<String, String> {
    String::from_utf8(request_with_timeout_bytes(socket, bytes, timeout)?)
        .map_err(|e| format!("failed to decode response: {e}"))
}

fn raw_io(socket: &Path, bytes: &[u8], timeout: Duration) -> Result<Vec<u8>, RawErr> {
    let zero = |op| move |e| (op, e, Duration::ZERO);
    let mut s = UnixStream::connect(socket).map_err(zero("connect to daemon socket"))?;
    s.set_read_timeout(Some(timeout))
        .map_err(zero("set socket read timeout"))?;
    s.set_write_timeout(Some(timeout))
        .map_err(zero("set socket write timeout"))?;
    s.write_all(bytes).map_err(zero("write request"))?;
    let (mut resp, started) = (Vec::new(), Instant::now());
    s.read_to_end(&mut resp)
        .map_err(|e| ("read response", e, started.elapsed()))?;
    Ok(resp)
}

fn format_socket_io_error(op: &str, err: &std::io::Error, timeout: Duration) -> String {
    if matches!(
        err.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    ) {
        let secs = timeout.as_secs().max(1);
        format!("client timeout after {secs}s while waiting to {op} (daemon may still be working). This is the CLI socket deadline, not node backpressure. Raise it with LASTDB_UDS_ADMIN_TIMEOUT_SECS=<secs> in the environment — the CLI and the daemon both read it, so server and client stay matched. Some subcommands also accept --timeout <secs>; not all define it.")
    } else {
        format!("failed to {op}: {err}")
    }
}

pub(crate) fn probe_health(socket: &Path) -> Result<(), String> {
    let raw = get_path_retry(socket, "/health", Duration::from_secs(2))?;
    let line = raw.lines().next().unwrap_or("");
    (line.starts_with("HTTP/1.1 200") || line.starts_with("HTTP/1.0 200"))
        .then_some(())
        .ok_or_else(|| format!("unexpected /health status: {line}"))
}

pub(crate) fn require_reachable(socket: &Path) -> Result<(), String> {
    probe_health(socket).map_err(|_| {
        format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        )
    })
}

pub(crate) fn split_http_200<'a>(resp: &'a [u8], label: &str) -> Result<&'a [u8], String> {
    let end = resp.windows(4).position(|w| w == b"\r\n\r\n");
    let head = std::str::from_utf8(end.map_or(resp, |i| &resp[..i]))
        .map_err(|e| format!("invalid {label} response headers: {e}"))?;
    let line = head.lines().next().unwrap_or("<empty response>");
    if !line.starts_with("HTTP/1.1 200 ") {
        let body = end.map_or_else(String::new, |i| {
            String::from_utf8_lossy(&resp[i + 4..]).trim().to_owned()
        });
        return Err(format!("{label} returned {line}: {body}"));
    }
    Ok(&resp[end.ok_or_else(|| format!("{label} response had no body"))? + 4..])
}

pub(crate) fn parse_json_response(response: &str, label: &str) -> Result<Value, String> {
    serde_json::from_slice(split_http_200(response.as_bytes(), label)?)
        .map_err(|e| format!("invalid {label} JSON: {e}"))
}

fn main() {
    std::panic::set_hook(Box::new(|info| {
        let p = info.payload();
        let m = p
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| p.downcast_ref::<&str>().copied());
        if m.is_some_and(|m| {
            (m.starts_with("failed printing to stdout")
                || m.starts_with("failed printing to stderr"))
                && (m.contains("os error 32") || m.contains("Broken pipe"))
        }) {
            std::process::exit(141);
        }
        observability::crash::write_panic_marker();
    }));
    if let Err(error) = run(Cli::parse()) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

pub(crate) fn resolve_client_home_and_socket(
    data_dir: Option<PathBuf>,
) -> Result<(PathBuf, PathBuf), String> {
    let env_sock = data_dir
        .is_none()
        .then(|| std::env::var_os("LASTDB_SOCKET_PATH"))
        .flatten()
        .filter(|v| !v.is_empty());
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let sock = if let Some(p) = env_sock {
        lastdb_node::profile::paths::expand_tilde_path(p)?
    } else {
        lastdb_node::lastdb_uds::uds::socket_path(&home.join("data"))
    };
    Ok((home, sock))
}

fn run(cli: Cli) -> Result<(), String> {
    let cmd = cli.command.unwrap_or(Command::Status {
        json: false,
        timeout: None,
    });
    let db_scoped = matches!(
        &cmd,
        Command::Get { .. }
            | Command::GetKeys(_)
            | Command::Mutate { .. }
            | Command::Share(
                ShareCommand::Invite { .. }
                    | ShareCommand::Accept { .. }
                    | ShareCommand::Contributors(
                        ShareContributorCommand::Grant { .. }
                            | ShareContributorCommand::Revoke { .. }
                    )
                    | ShareCommand::Files(_)
            )
    );
    if cli.db.is_some() && !db_scoped {
        return Err("--db requires a record command or a database-scoped share command".into());
    }
    match cmd {
        Command::Status { json, timeout } => {
            if !status(cli.data_dir, json, timeout)? {
                std::process::exit(1);
            }
            Ok(())
        }
        Command::Backup(b) => cloud_command(
            cli.data_dir,
            CloudCommand::BackupV2 {
                action: match b {
                    BackupCommand::Connect(a) => ByteBackupCommand::Connect(a),
                    BackupCommand::Restore(a) => ByteBackupCommand::Restore(a),
                    BackupCommand::Snapshot { database, wait } => ByteBackupCommand::Snapshot {
                        database: database.or(cli.db).unwrap_or_else(|| "personal".to_owned()),
                        wait,
                    },
                },
            },
        ),
        Command::Share(a) => share_cmds::run(cli.data_dir, cli.db.as_deref(), a),
        Command::Db(DatabaseCommand::List { json }) => list_databases(cli.data_dir, json),
        Command::Db(DatabaseCommand::Select { selector, json }) => {
            select_database(cli.data_dir, &selector, json)
        }
        Command::Db(DatabaseCommand::Create {
            name,
            private,
            spec_name,
            spec_version,
            json,
        }) => create_database(
            cli.data_dir,
            &name,
            private,
            spec_name.as_deref(),
            spec_version,
            json,
        ),
        Command::Admin(a) => run_admin(cli.data_dir, a),
        Command::App(a) => app_command(cli.data_dir, a),
        c => {
            let t = DatabaseTarget::open(cli.data_dir, cli.db.as_deref())?;
            match c {
                Command::Mutate {
                    request_file,
                    durable,
                    mutation_id,
                } => mutate_command(&t, &request_file, durable, mutation_id),
                Command::GetKeys(a) => list_record_keys(&t, &a),
                Command::Get {
                    schema,
                    key_hash,
                    key_range,
                    json,
                    hydrate,
                    hydrate_inline,
                } => get_record(
                    &t,
                    &schema,
                    (key_hash.as_deref(), key_range.as_deref()),
                    json,
                    (hydrate.as_deref(), hydrate_inline.as_deref()),
                ),
                _ => unreachable!(),
            }
        }
    }
}
