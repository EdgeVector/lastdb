//! Tiny socket-only LastDB operator CLI.
//!
//! This intentionally does not depend on `fold_db_node`: the Homebrew minimal
//! product should ship a daemon plus a small control client, not the full
//! server/UI/ingestion CLI graph.

#[path = "lastdb/admin_cmds.rs"]
mod admin_cmds;
use admin_cmds::db_post_json;
#[path = "lastdb/app_cmds.rs"]
mod app_cmds;
use app_cmds::*;
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
#[path = "lastdb/share_cmds.rs"]
mod share_cmds;
#[path = "lastdb/snapshot_cmd.rs"]
mod snapshot_cmd;
use cloud_cmds::*;
#[path = "lastdb/status_ops_cmds.rs"]
mod status_ops_cmds;
use status_ops_cmds::*;
#[path = "lastdb/mutate_cmds.rs"]
mod mutate_cmds;
use mutate_cmds::*;
#[path = "lastdb/socket_client.rs"]
mod socket_client;
use socket_client::*;

use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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
const BROKEN_PIPE_EXIT_CODE: i32 = 141;

/// True for the panic `print!`/`eprint!` raise when their stream is a closed
/// pipe. EPIPE is errno 32 on macOS and Linux.
fn is_std_stream_broken_pipe_panic(message: &str) -> bool {
    (message.starts_with("failed printing to stdout")
        || message.starts_with("failed printing to stderr"))
        && (message.contains("os error 32") || message.contains("Broken pipe"))
}

/// Exit quietly with [`BROKEN_PIPE_EXIT_CODE`] when a downstream reader closes
/// stdout/stderr early.
fn install_broken_pipe_exit_hook() {
    std::panic::set_hook(Box::new(|info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied());
        if message.is_some_and(is_std_stream_broken_pipe_panic) {
            std::process::exit(BROKEN_PIPE_EXIT_CODE);
        }
        observability::crash::write_panic_marker();
    }));
}

/// Explicit node-socket override for client subcommands.
const LASTDB_SOCKET_PATH_ENV: &str = "LASTDB_SOCKET_PATH";

/// Socket a client subcommand talks to.
fn client_socket_path(
    explicit_data_dir: bool,
    home: &Path,
    env_socket: Option<std::ffi::OsString>,
) -> Result<PathBuf, String> {
    match env_socket.filter(|value| !value.is_empty()) {
        Some(path) if !explicit_data_dir => lastdb_node::profile::paths::expand_tilde_path(path),
        _ => Ok(lastdb_node::lastdb_uds::uds::socket_path(
            &home.join("data"),
        )),
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

fn service_home_command(action: ServiceHomeCommand) -> Result<(), String> {
    let message = match action {
        ServiceHomeCommand::Set { home } => lastdb_node::service_home::set_configured_home(&home),
        ServiceHomeCommand::Show => lastdb_node::service_home::show_configured_home(),
        ServiceHomeCommand::Clear => lastdb_node::service_home::clear_configured_home(),
    }?;
    println!("{message}");
    Ok(())
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
                | Command::Schema { .. }
                | Command::Share(ShareCommand::Files(
                    ShareFilesCommand::List(_) | ShareFilesCommand::Save { .. },
                ))
        )
    {
        return Err("--db requires get, get-keys, mutate, schema show, or Files list/save".into());
    }
    match command {
        Command::Status { json, timeout } => match status(cli.data_dir, json, timeout)? {
            StatusRun::Serving => Ok(()),
            StatusRun::Unreachable => std::process::exit(STATUS_UNREACHABLE_EXIT),
        },
        Command::Backup { action } => cloud_command(cli.data_dir, action.into_cloud()?),
        Command::Share(action) => share_cmds::run(cli.data_dir, cli.db.as_deref(), action),
        Command::Db {
            action: DatabaseCommand::List { json },
        } => list_databases(cli.data_dir, json),
        Command::Admin { action } => admin_cmds::run(cli.data_dir, action),
        cmd @ (Command::Mutate { .. } | Command::GetKeys(_) | Command::Get { .. }) => {
            record_command(&DatabaseTarget::open(cli.data_dir, cli.db.as_deref())?, cmd)
        }
        Command::Schema {
            action: SchemaInspectCommand::Show { name, json },
        } => schema_show_command(
            &DatabaseTarget::open(cli.data_dir, cli.db.as_deref())?,
            &name,
            json,
        ),
        Command::App { action } => app_command(cli.data_dir, action),
    }
}
