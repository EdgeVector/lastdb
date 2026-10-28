//! Tiny socket-only LastDB operator CLI.
//!
//! This intentionally does not depend on `fold_db_node`: the Homebrew minimal
//! product should ship a daemon plus a small control client, not the full
//! server/UI/ingestion CLI graph.

#[path = "lastdb/app_cmds.rs"]
mod app_cmds;
use app_cmds::*;
#[path = "lastdb/liveness_atom_print.rs"]
mod liveness_atom_print;
#[path = "lastdb/liveness_blob_print.rs"]
mod liveness_blob_print;
#[path = "lastdb/liveness_molecule_print.rs"]
mod liveness_molecule_print;
#[path = "lastdb/schema_record_cmds.rs"]
mod schema_record_cmds;
use schema_record_cmds::*;
#[path = "lastdb/record_get_cmds.rs"]
mod record_get_cmds;
use record_get_cmds::*;
#[path = "lastdb/db_cmds.rs"]
mod db_cmds;
use db_cmds::*;
#[path = "lastdb/cloud_cmds.rs"]
mod cloud_cmds;
use cloud_cmds::*;
#[path = "lastdb/status_ops_cmds.rs"]
mod status_ops_cmds;
use status_ops_cmds::*;

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine as _;
use clap::{Args, Parser, Subcommand};

#[path = "lastdb/cli_defs.rs"]
mod cli_defs;
use cli_defs::*;

fn main() {
    install_broken_pipe_exit_hook();
    let cli = Cli::parse();
    if let Err(error) = run(cli) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

/// Shell exit status for a process that SIGPIPE killed (128 + 13).
///
/// Rust ignores SIGPIPE, so `println!` into a pipe whose reader exited
/// (`lastdb status | head`) panics with "failed printing to stdout: Broken
/// pipe". We do not restore `SIG_DFL` instead: this binary also writes to the
/// daemon's Unix socket, and a default SIGPIPE there would kill the CLI with
/// no error text when the daemon closes a connection.
const BROKEN_PIPE_EXIT_CODE: i32 = 141;

/// True for the panic `print!`/`eprint!` raise when their stream is a closed
/// pipe. EPIPE is errno 32 on macOS and Linux.
fn is_std_stream_broken_pipe_panic(message: &str) -> bool {
    (message.starts_with("failed printing to stdout")
        || message.starts_with("failed printing to stderr"))
        && (message.contains("os error 32") || message.contains("Broken pipe"))
}

/// Exit quietly with [`BROKEN_PIPE_EXIT_CODE`] when a downstream reader closes
/// stdout/stderr early. Non-zero on purpose: output was cut, so a caller that
/// checks the status (for example `restore --json`) must still fail closed.
fn install_broken_pipe_exit_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied());
        if message.is_some_and(is_std_stream_broken_pipe_panic) {
            std::process::exit(BROKEN_PIPE_EXIT_CODE);
        }
        default_hook(info);
    }));
}

/// Explicit node-socket override for client subcommands. Same name the app
/// SDK honours first (`lastdb_app_sdk` transport discovery).
const LASTDB_SOCKET_PATH_ENV: &str = "LASTDB_SOCKET_PATH";

/// Socket a client subcommand talks to.
///
/// Order: an explicit `--data-dir` wins (flag beats env); then a non-empty
/// `LASTDB_SOCKET_PATH`; then `<home>/data/folddb.sock`. Offline commands that
/// probe the socket only to refuse when a daemon holds *this* home's store must
/// keep using the home socket, not this resolver.
fn client_socket_path(
    explicit_data_dir: bool,
    home: &Path,
    env_socket: Option<std::ffi::OsString>,
) -> Result<PathBuf, String> {
    match env_socket.filter(|value| !value.is_empty()) {
        Some(path) if !explicit_data_dir => folddb_profile::paths::expand_tilde_path(path),
        _ => Ok(lastdb_uds::uds::socket_path(&home.join("data"))),
    }
}

/// Resolve the node home and the socket for an online client subcommand.
fn resolve_client_home_and_socket(data_dir: Option<PathBuf>) -> Result<(PathBuf, PathBuf), String> {
    let explicit_data_dir = data_dir.is_some();
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = client_socket_path(
        explicit_data_dir,
        &home,
        std::env::var_os(LASTDB_SOCKET_PATH_ENV),
    )?;
    Ok((home, socket))
}

/// Header line that states how old the sampler snapshot behind `lastdb ops` is.
///
/// The daemon republishes the snapshot about once per sample interval (60 s by
/// default), so traffic from the last minute can be missing. Without an age
/// the reader takes old numbers as current.
fn sampler_snapshot_age_line(last_sample_at: Option<u64>, now_secs: u64, socket: &Path) -> String {
    let age = match last_sample_at {
        None => "sampler has not run yet".to_string(),
        Some(at) if at > now_secs => {
            format!("sampled {} s in the future (clock skew)", at - now_secs)
        }
        Some(at) => format!("snapshot {} s old", now_secs - at),
    };
    format!(
        "Snapshot: {age} (sampler republishes about every {} s) from {}",
        lastdb_node::self_metrics::sample_interval_from_env().as_secs(),
        socket.display()
    )
}

fn service_home_command(action: ServiceHomeCommand) -> Result<(), String> {
    let message = match action {
        ServiceHomeCommand::Set { home } => lastdb_node::service_home::set_configured_home(&home),
        ServiceHomeCommand::Show => lastdb_node::service_home::show_configured_home(),
        ServiceHomeCommand::Clear => lastdb_node::service_home::clear_configured_home(),
    }?;
    println!("{message}");
    Ok(())
}

fn schema_inspect_dispatch(
    data_dir: Option<PathBuf>,
    action: SchemaInspectCommand,
) -> Result<(), String> {
    match action {
        SchemaInspectCommand::Storage { schema, json } => {
            schema_storage_command(data_dir, &schema, json)
        }
        SchemaInspectCommand::StorageReport { json } => {
            schema_storage_report_command(data_dir, json)
        }
        SchemaInspectCommand::Show { name, json } => schema_show_command(data_dir, &name, json),
        SchemaInspectCommand::Drop {
            schema,
            owner_app,
            must_exist,
            json,
        } => schema_drop_command(data_dir, schema, owner_app, must_exist, json),
    }
}

fn liveness_dispatch(data_dir: Option<PathBuf>, action: LivenessCommand) -> Result<(), String> {
    match action {
        LivenessCommand::Explain {
            class,
            id,
            scope,
            json,
        } => liveness_explain_command(data_dir, (&class, &id, scope.as_deref()), json),
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command.unwrap_or(Command::Status {
        json: false,
        contract: false,
        timeout: None,
    }) {
        Command::Status {
            json,
            contract,
            timeout,
        } => match status(cli.data_dir, json, contract, timeout)? {
            StatusRun::Serving => Ok(()),
            StatusRun::Unreachable => std::process::exit(STATUS_UNREACHABLE_EXIT),
        },
        Command::Ops(args) => ops(cli.data_dir, &args),
        Command::AlertCheck(args) => alert_check(cli.data_dir, args),
        Command::LogFilter { directive, json } => {
            log_filter_command(cli.data_dir, directive.as_deref(), json)
        }
        Command::ServiceHome { action } => service_home_command(action),
        Command::IsolateVolume {
            volume_name,
            skip_volume,
            execute,
            json,
        } => isolate_volume_command(cli.data_dir, &volume_name, skip_volume, execute, json),
        Command::Cloud { action } => cloud_command(cli.data_dir, action),
        cmd @ (Command::Mutate { .. }
        | Command::List { .. }
        | Command::GetKeys { .. }
        | Command::Get { .. }) => record_command(cli.data_dir, cmd),
        Command::Schema { action } => schema_inspect_dispatch(cli.data_dir, action),
        Command::Liveness { action } => liveness_dispatch(cli.data_dir, action),
        Command::Db { action } => db_command(cli.data_dir, action),
        Command::SchemaRetention { action } => schema_retention_command(cli.data_dir, action),
        Command::SchemaNameClaim { action } => schema_name_claim_command(cli.data_dir, action),
        Command::SearchRebuild { page_size, json } => {
            search_rebuild_command(cli.data_dir, page_size, json)
        }
        Command::App { action } => app_command(cli.data_dir, action),
    }
}

#[path = "lastdb/lifecycle_cmds.rs"]
mod lifecycle_cmds;
use lifecycle_cmds::*;
#[path = "lastdb/mutate_cmds.rs"]
mod mutate_cmds;
#[path = "lastdb/record_cmds.rs"]
mod record_cmds;
use mutate_cmds::*;
use record_cmds::record_command;

#[path = "lastdb/socket_client.rs"]
mod socket_client;
use socket_client::*;
