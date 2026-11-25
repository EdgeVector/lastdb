//! Tiny socket-only LastDB operator CLI.

#[path = "lastdb/admin_cmds.rs"]
mod admin_cmds;
#[path = "lastdb/app_cmds.rs"]
mod app_cmds;
#[path = "lastdb/app_registry_index/mod.rs"]
mod app_registry_index;
#[path = "lastdb/app_run.rs"]
mod app_run;
#[path = "lastdb/cli_defs.rs"]
mod cli_defs;
#[path = "lastdb/cloud_cmds.rs"]
mod cloud_cmds;
#[path = "lastdb/mutate_cmds.rs"]
mod mutate_cmds;
#[path = "lastdb/ops_cmds.rs"]
mod ops_cmds;
#[path = "lastdb/record_get_cmds.rs"]
mod record_get_cmds;
#[path = "lastdb/share_cmds.rs"]
mod share_cmds;
#[path = "lastdb/snapshot_line.rs"]
mod snapshot_line;
#[path = "lastdb/uds_client.rs"]
mod uds_client;
#[path = "lastdb/usage_line.rs"]
mod usage_line;
#[path = "lastdb/v1_records.rs"]
mod v1_records;

pub(crate) use admin_cmds::DatabaseTarget;
use admin_cmds::*;
use app_cmds::*;
use clap::Parser;
use cli_defs::*;
use cloud_cmds::*;
use mutate_cmds::*;
use record_get_cmds::*;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;
pub(crate) use uds_client::*;

pub(crate) fn block_on<T>(
    label: &str,
    future: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("cannot construct {label} runtime: {e}"))?
        .block_on(future)
}

pub(crate) fn write_new_private_file(out: &Path, bytes: &[u8], label: &str) -> Result<(), String> {
    let parent = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let directory =
        std::fs::File::open(parent).map_err(|e| format!("cannot open {label} directory: {e}"))?;
    let tmp = out.with_file_name(format!(".lastdb-{}.pending", uuid::Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(|e| format!("cannot create {label}: {e}"))?;
    let result = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::hard_link(&tmp, out));
    let cleanup = std::fs::remove_file(&tmp);
    result.map_err(|e| format!("cannot publish {label} to a new file: {e}"))?;
    cleanup
        .and_then(|()| directory.sync_all())
        .map_err(|e| format!("cannot sync {label}: {e}"))
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
    if let Some(error) = app_run::run_if_app_command() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
    if let Err(error) = run(Cli::parse()) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), String> {
    let cmd = cli.command.unwrap_or(Command::Status {
        json: false,
        timeout: None,
    });
    let db_scoped = matches!(
        &cmd,
        Command::Get { .. } | Command::GetKeys(_) | Command::Mutate { .. }
    );
    if cli.db.is_some() && !db_scoped {
        return Err("--db requires a record command".into());
    }
    match cmd {
        Command::Status { json, timeout } => {
            if !status(cli.data_dir, json, timeout)? {
                std::process::exit(1);
            }
            Ok(())
        }
        Command::Backup(b) => backup_command(cli.data_dir, b),
        Command::Cloud(c) => cloud_account_command(cli.data_dir, c),
        Command::Share(a) => share_cmds::run(cli.data_dir, a),
        Command::Db(DatabaseCommand::List { json }) => list_databases(cli.data_dir, json),
        Command::Db(DatabaseCommand::Create {
            name,
            private,
            json,
        }) => create_database(cli.data_dir, &name, private, json),
        Command::Admin(a) => run_admin(cli.data_dir, a),
        Command::App(a) => app_command(cli.data_dir, a),
        Command::Ops { by_count, json } => ops_cmds::ops(cli.data_dir, by_count, json),
        c => {
            let t = DatabaseTarget::open(cli.data_dir, cli.db.as_deref())?;
            match c {
                Command::Mutate {
                    request_file,
                    durable,
                } => mutate_command(&t, &request_file, durable),
                Command::GetKeys(a) => list_record_keys(&t, &a),
                Command::Get {
                    schema,
                    key_hash,
                    key_range,
                    json,
                    hydrate,
                } => get_record(
                    &t,
                    &schema,
                    (key_hash.as_deref(), key_range.as_deref()),
                    json,
                    hydrate.as_deref(),
                ),
                _ => unreachable!(),
            }
        }
    }
}
