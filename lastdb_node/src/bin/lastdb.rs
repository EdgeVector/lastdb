//! Tiny socket-only LastDB operator CLI.

#[path = "lastdb/admin_cmds.rs"]
mod admin_cmds;
#[path = "lastdb/app_cmds.rs"]
mod app_cmds;
use app_cmds::*;
#[path = "lastdb/schema_record_cmds.rs"]
mod schema_record_cmds;
use schema_record_cmds::*;
#[path = "lastdb/record_get_cmds.rs"]
mod record_get_cmds;
use record_get_cmds::*;
#[path = "lastdb/cloud_cmds.rs"]
mod cloud_cmds;
#[path = "lastdb/share_cmds.rs"]
mod share_cmds;
use cloud_cmds::*;
#[path = "lastdb/status_ops_cmds.rs"]
mod status_ops_cmds;
use status_ops_cmds::*;
#[path = "lastdb/mutate_cmds.rs"]
mod mutate_cmds;
use mutate_cmds::*;
#[path = "lastdb/database_cmds.rs"]
mod database_cmds;
use database_cmds::*;

use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};

#[path = "lastdb/cli_defs.rs"]
mod cli_defs;
use cli_defs::*;

mod socket_client {
    use super::*;

    type SocketRequestResult = Result<String, (&'static str, std::io::Error, Duration)>;

    pub(crate) fn client_headers() -> String {
        format!(
            "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
            uuid::Uuid::new_v4()
        )
    }

    pub(crate) fn post_json_admin(
        socket: &Path,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<String, String> {
        post_json_with_timeout(socket, path, body, admin_scan_client_timeout())
    }

    pub(crate) fn post_json_with_timeout(
        socket: &Path,
        path: &str,
        body: &serde_json::Value,
        timeout: Duration,
    ) -> Result<String, String> {
        post_json_with_id(socket, path, body, timeout, None)
    }

    pub(crate) fn post_json_with_id(
        socket: &Path,
        path: &str,
        body: &serde_json::Value,
        timeout: Duration,
        id: Option<uuid::Uuid>,
    ) -> Result<String, String> {
        let body = serde_json::to_vec(body).map_err(|e| format!("failed to encode JSON: {e}"))?;
        let id_header = id.map_or_else(String::new, |id| format!("Idempotency-Key: {id}\r\n"));
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\n{}{id_header}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            client_headers(),
            body.len()
        )
        .into_bytes();
        request.extend_from_slice(&body);
        request_with_timeout(socket, &request, timeout)
    }

    pub(crate) fn request_with_timeout(
        socket: &Path,
        bytes: &[u8],
        timeout: Duration,
    ) -> Result<String, String> {
        request_with_timeout_raw(socket, bytes, timeout)
            .map_err(|(op, err, _)| format_socket_io_error(op, &err, timeout))
    }

    fn request_with_timeout_raw(
        socket: &Path,
        bytes: &[u8],
        timeout: Duration,
    ) -> SocketRequestResult {
        let response = request_with_timeout_raw_bytes(socket, bytes, timeout)?;
        let started = Instant::now();
        String::from_utf8(response).map_err(|e| {
            (
                "decode response",
                std::io::Error::new(ErrorKind::InvalidData, e),
                started.elapsed(),
            )
        })
    }

    pub(crate) fn request_with_timeout_raw_bytes(
        socket: &Path,
        bytes: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, (&'static str, std::io::Error, Duration)> {
        let mut stream = UnixStream::connect(socket)
            .map_err(|e| ("connect to daemon socket", e, Duration::ZERO))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| ("set socket read timeout", e, Duration::ZERO))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|e| ("set socket write timeout", e, Duration::ZERO))?;
        stream
            .write_all(bytes)
            .map_err(|e| ("write request", e, Duration::ZERO))?;
        let mut response = Vec::new();
        let started = Instant::now();
        stream
            .read_to_end(&mut response)
            .map_err(|e| ("read response", e, started.elapsed()))?;
        Ok(response)
    }

    pub(crate) fn request_readonly_with_retry(
        socket: &Path,
        bytes: &[u8],
        timeout: Duration,
    ) -> Result<String, String> {
        let started = Instant::now();
        for attempt in 1..=3usize {
            let attempt_timeout = timeout
                .saturating_sub(started.elapsed())
                .max(Duration::from_millis(1));
            match request_with_timeout_raw(socket, bytes, attempt_timeout) {
                Ok(response) => return Ok(response),
                Err(("read response", err, elapsed))
                    if err.kind() == ErrorKind::WouldBlock
                        && elapsed < attempt_timeout
                        && elapsed < Duration::from_secs(1) =>
                {
                    if attempt == 3 {
                        return Err(
                            "transient socket read remained unavailable after 3 attempts; retry this command. The node may be healthy but busy; this is not an outage.".into(),
                        );
                    }
                    std::thread::sleep(Duration::from_millis(25 * attempt as u64));
                }
                Err((op, err, _)) => return Err(format_socket_io_error(op, &err, timeout)),
            }
        }
        unreachable!()
    }

    pub(crate) fn format_socket_io_error(
        op: &str,
        err: &std::io::Error,
        timeout: Duration,
    ) -> String {
        if matches!(
            err.kind(),
            ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
        ) {
            let secs = timeout.as_secs().max(1);
            format!(
                "client timeout after {secs}s while waiting to {op} (daemon may still be working). This is the CLI socket deadline, not node backpressure. Raise it with LASTDB_UDS_ADMIN_TIMEOUT_SECS=<secs> in the environment — the CLI and the daemon both read it, so server and client stay matched. Some subcommands also accept --timeout <secs>; not all define it."
            )
        } else {
            format!("failed to {op}: {err}")
        }
    }

    pub(crate) fn probe_health(socket: &Path) -> Result<(), String> {
        let raw = request_readonly_with_retry(
            socket,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            Duration::from_secs(2),
        )?;
        let first_line = raw.lines().next().unwrap_or("");
        if first_line.starts_with("HTTP/1.1 200") || first_line.starts_with("HTTP/1.0 200") {
            Ok(())
        } else {
            Err(format!("unexpected /health status: {first_line}"))
        }
    }

    pub(crate) fn parse_json_response(
        response: &str,
        label: &str,
    ) -> Result<serde_json::Value, String> {
        let status_line = response.lines().next().unwrap_or("<empty response>");
        if !response.starts_with("HTTP/1.1 200 ") {
            let body = response
                .split_once("\r\n\r\n")
                .map_or("", |(_, b)| b.trim());
            return Err(format!("{label} returned {status_line}: {body}"));
        }
        let (_, body) = response
            .split_once("\r\n\r\n")
            .ok_or_else(|| format!("{label} response had no body"))?;
        serde_json::from_str(body).map_err(|e| format!("invalid {label} JSON: {e}"))
    }

    pub(crate) fn parse_binary_response(response: &[u8], label: &str) -> Result<Vec<u8>, String> {
        let header_end = response
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| format!("{label} response had no headers"))?;
        let header = std::str::from_utf8(&response[..header_end])
            .map_err(|e| format!("invalid {label} response headers: {e}"))?;
        let status_line = header.lines().next().unwrap_or("<empty response>");
        if !status_line.starts_with("HTTP/1.1 200 ") {
            let body = String::from_utf8_lossy(&response[header_end + 4..]);
            return Err(format!("{label} returned {status_line}: {}", body.trim()));
        }
        Ok(response[header_end + 4..].to_vec())
    }
}
use socket_client::*;

fn main() {
    std::panic::set_hook(Box::new(|info| {
        let payload = info.payload();
        let msg = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied());
        if msg.is_some_and(|m| {
            (m.starts_with("failed printing to stdout")
                || m.starts_with("failed printing to stderr"))
                && (m.contains("os error 32") || m.contains("Broken pipe"))
        }) {
            std::process::exit(141);
        }
        observability::crash::write_panic_marker();
    }));
    let cli = Cli::parse();
    if let Err(error) = run(cli) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn resolve_client_home_and_socket(data_dir: Option<PathBuf>) -> Result<(PathBuf, PathBuf), String> {
    let explicit_data_dir = data_dir.is_some();
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = match std::env::var_os("LASTDB_SOCKET_PATH").filter(|v| !v.is_empty()) {
        Some(path) if !explicit_data_dir => lastdb_node::profile::paths::expand_tilde_path(path)?,
        _ => lastdb_node::lastdb_uds::uds::socket_path(&home.join("data")),
    };
    Ok((home, socket))
}

fn run(cli: Cli) -> Result<(), String> {
    let command = cli.command.unwrap_or(Command::Status {
        json: false,
        timeout: None,
    });
    if cli.db.is_some()
        && !matches!(
            &command,
            Command::Get { .. }
                | Command::GetKeys(_)
                | Command::Mutate { .. }
                | Command::Share(ShareCommand::Files(
                    ShareFilesCommand::List(_) | ShareFilesCommand::Save { .. },
                ))
        )
    {
        return Err("--db requires get, get-keys, mutate, or Files list/save".into());
    }
    match command {
        Command::Status { json, timeout } => match status(cli.data_dir, json, timeout)? {
            StatusRun::Serving => Ok(()),
            StatusRun::Unreachable => std::process::exit(STATUS_UNREACHABLE_EXIT),
        },
        Command::Backup { action } => cloud_command(cli.data_dir, action.into_cloud()?),
        Command::Share(action) => share_cmds::run(cli.data_dir, cli.db.as_deref(), action),
        Command::Db { action } => match action {
            DatabaseCommand::List { json } => list_databases(cli.data_dir, json),
            DatabaseCommand::Create {
                name,
                spec_name,
                spec_version,
                json,
            } => create_database(
                cli.data_dir,
                &name,
                spec_name.as_deref(),
                spec_version,
                json,
            ),
            DatabaseCommand::Select { selector, json } => {
                select_database(cli.data_dir, &selector, json)
            }
        },
        Command::Admin { action } => admin_cmds::run(cli.data_dir, action),
        cmd @ (Command::Mutate { .. } | Command::GetKeys(_) | Command::Get { .. }) => {
            record_command(&DatabaseTarget::open(cli.data_dir, cli.db.as_deref())?, cmd)
        }
        Command::App { action } => app_command(cli.data_dir, action),
    }
}
