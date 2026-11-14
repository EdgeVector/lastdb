//! Tiny socket-only LastDB operator CLI.

#[path = "lastdb/admin_cmds.rs"]
mod admin_cmds;
#[path = "lastdb/app_cmds.rs"]
mod app_cmds;
#[path = "lastdb/app_registry_index/mod.rs"]
mod app_registry_index;
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
#[path = "lastdb/share_file/mod.rs"]
mod share_file;
#[path = "lastdb/status_ops_cmds.rs"]
mod status_ops_cmds;
#[path = "lastdb/uds_client.rs"]
mod uds_client;

use admin_cmds::*;
use app_cmds::*;
use clap::Parser;
use cli_defs::*;
use cloud_cmds::*;
use database_cmds::*;
use mutate_cmds::*;
use record_get_cmds::*;
use status_ops_cmds::*;
pub(crate) use uds_client::*;

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
                    BackupCommand::Reclaim(a) => ByteBackupCommand::Reclaim(a),
                    BackupCommand::Snapshot { database, wait } => ByteBackupCommand::Snapshot {
                        database: database.or(cli.db).unwrap_or_else(|| "personal".to_owned()),
                        wait,
                    },
                },
            },
        ),
        Command::Cloud(c) => cloud_account_command(cli.data_dir, c),
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
