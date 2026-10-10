//! Tiny socket-only LastDB operator CLI.
//!
//! This intentionally does not depend on `fold_db_node`: the Homebrew minimal
//! product should ship a daemon plus a small control client, not the full
//! server/UI/ingestion CLI graph.

#[path = "lastdb/restore_failure.rs"]
mod restore_failure;
use restore_failure::*;
#[path = "lastdb/app_cmds.rs"]
mod app_cmds;
use app_cmds::*;
#[path = "lastdb/schema_record_cmds.rs"]
mod schema_record_cmds;
use schema_record_cmds::*;
#[path = "lastdb/record_get_cmds.rs"]
mod record_get_cmds;
use record_get_cmds::*;
#[path = "lastdb/version_retention_cmds.rs"]
mod version_retention_cmds;
use version_retention_cmds::*;
#[path = "lastdb/db_cmds.rs"]
mod db_cmds;
use db_cmds::*;
#[path = "lastdb/restore_cmds.rs"]
mod restore_cmds;
use restore_cmds::*;
#[path = "lastdb/cloud_cmds.rs"]
mod cloud_cmds;
use cloud_cmds::*;
#[path = "lastdb/status_ops_cmds.rs"]
mod status_ops_cmds;
use status_ops_cmds::*;

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf}; // Path used by load_cloud_creds
use std::process::Command as ProcessCommand;
use std::time::{Duration, Instant, SystemTime};

use base64::Engine as _;
use clap::{Args, Parser, Subcommand, ValueEnum};

#[path = "lastdb/backup_atom_rewrite.rs"]
mod backup_atom_rewrite;

#[path = "lastdb/cloud_rescue_publish.rs"]
mod cloud_rescue_publish;

#[path = "lastdb/cloud_primary_resume.rs"]
mod cloud_primary_resume;

#[path = "lastdb/restore_checkpoint.rs"]
mod restore_checkpoint;

#[path = "lastdb/cli_defs.rs"]
mod cli_defs;
use cli_defs::*;

fn main() {
    install_broken_pipe_exit_hook();
    let cli = Cli::parse();
    if matches!(cli.command.as_ref(), Some(Command::Restore { .. })) {
        // Restore replays authenticated historical writes in this one-shot process.
        // Keep the absolute atom cap even when this host uses the 64 KiB default.
        std::env::set_var(
            fold_db::atom::MAX_ATOM_CONTENT_BYTES_ENV,
            fold_db::atom::ABSOLUTE_MAX_ATOM_CONTENT_BYTES.to_string(),
        );
    }
    let json_restore = matches!(
        cli.command.as_ref(),
        Some(Command::Restore { json: true, .. })
    );
    if let Err(e) = run(cli) {
        // A valid `restore --json` command emits its bounded, allowlisted
        // failure envelope at the restore boundary. Do not repeat the private
        // diagnostic text here. Clap parse errors, panics, signals, and broken
        // output remain outside that contract and still fail closed by exit.
        if !json_restore {
            eprintln!("error: {e}");
        }
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

fn normalize_invite_code_input(input: &str) -> Result<String, String> {
    let invite_code = input.trim();
    if invite_code.is_empty() {
        return Err("--invite-code-stdin received empty input".to_string());
    }
    Ok(invite_code.to_string())
}

fn read_invite_code_from_stdin() -> Result<String, String> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|e| format!("failed to read --invite-code-stdin: {e}"))?;
    normalize_invite_code_input(&input)
}

fn require_no_existing_identity_home_override(override_present: bool) -> Result<(), String> {
    if override_present {
        return Err(
            "LASTDB_HOME and FOLDDB_HOME must be unset for --use-existing-identity".to_string(),
        );
    }
    Ok(())
}

fn validate_existing_identity_dev_connect(
    data_dir: Option<&Path>,
    env: Option<&str>,
    api_url: Option<&str>,
    invite_code: Option<&str>,
    invite_code_stdin: bool,
    force: bool,
) -> Result<PathBuf, String> {
    let data_dir = data_dir.ok_or_else(|| {
        "--use-existing-identity requires an explicit root --data-dir".to_string()
    })?;
    if !data_dir.is_absolute() {
        return Err("--use-existing-identity requires an absolute --data-dir path".to_string());
    }
    require_no_existing_identity_home_override(
        lastdb_node::service_home::explicit_home_override_present(),
    )?;
    if env != Some("dev") {
        return Err("--use-existing-identity requires explicit --env dev".to_string());
    }
    if api_url.is_some() {
        return Err("--use-existing-identity does not accept --api-url".to_string());
    }
    if invite_code.is_some() {
        return Err(
            "--use-existing-identity accepts an invite only through --invite-code-stdin"
                .to_string(),
        );
    }
    if !invite_code_stdin {
        return Err("--use-existing-identity requires --invite-code-stdin".to_string());
    }
    if force {
        return Err("--use-existing-identity does not accept --force".to_string());
    }

    let home = lastdb_node::host::resolve_home(Some(data_dir.to_path_buf()))?;
    lastdb_node::cloud::preflight_existing_identity_dev(&home)
}

fn prepare_existing_identity_dev_connect<F>(
    data_dir: Option<&Path>,
    env: Option<&str>,
    api_url: Option<&str>,
    invite_code: Option<&str>,
    invite_code_stdin: bool,
    force: bool,
    read_invite: F,
) -> Result<(PathBuf, String), String>
where
    F: FnOnce() -> Result<String, String>,
{
    let home = validate_existing_identity_dev_connect(
        data_dir,
        env,
        api_url,
        invite_code,
        invite_code_stdin,
        force,
    )?;
    // Do not consume the stdin invite until every option and filesystem guard
    // accepts the target.
    Ok((home, read_invite()?))
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
        Command::ServiceHome { action } => {
            let message = match action {
                ServiceHomeCommand::Set { home } => {
                    lastdb_node::service_home::set_configured_home(&home)
                }
                ServiceHomeCommand::Show => lastdb_node::service_home::show_configured_home(),
                ServiceHomeCommand::Clear => lastdb_node::service_home::clear_configured_home(),
            }?;
            println!("{message}");
            Ok(())
        }
        Command::IsolateVolume {
            volume_name,
            skip_volume,
            execute,
            json,
        } => {
            let home = lastdb_node::host::resolve_home(cli.data_dir)?;
            let plan = lastdb_node::volume_isolation::plan(&home, &volume_name, skip_volume)?;

            if !execute {
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "dry_run": true,
                            "home": plan.home,
                            "volume": plan.volume.as_ref().map(|v| serde_json::json!({
                                "container": v.container,
                                "volume_name": v.volume_name,
                                "mount_point": v.mount_point,
                            })),
                        }))
                        .map_err(|e| format!("failed to serialize plan: {e}"))?
                    );
                } else {
                    println!("Plan (pass --execute to apply; needs root):");
                    match &plan.volume {
                        None => println!(
                            "  exclude {} from Time Machine (no volume: --skip-volume)",
                            plan.home.display()
                        ),
                        Some(v) => {
                            println!("  exclude {} from Time Machine", plan.home.display());
                            println!(
                                "  create APFS volume \"{}\" in container {} at {}",
                                v.volume_name,
                                v.container,
                                v.mount_point.display()
                            );
                            println!(
                                "  disable FSEvents on it ({}/.fseventsd/no_log)",
                                v.mount_point.display()
                            );
                            println!(
                                "  initialize the node home at {} instead of {}",
                                v.mount_point.display(),
                                plan.home.display()
                            );
                        }
                    }
                }
                return Ok(());
            }

            let report = lastdb_node::volume_isolation::execute(&plan)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "time_machine_excluded": report.time_machine_excluded,
                        "effective_home": report.effective_home,
                    }))
                    .map_err(|e| format!("failed to serialize report: {e}"))?
                );
            } else {
                println!("Excluded {} from Time Machine.", plan.home.display());
                if let Some(v) = &plan.volume {
                    println!(
                        "Created volume \"{}\" at {} with FSEvents disabled.",
                        v.volume_name,
                        v.mount_point.display()
                    );
                    println!(
                        "Node home initialized at {}. Use --data-dir {} (or `lastdb service-home set {}`) to serve from it.",
                        report.effective_home.display(),
                        report.effective_home.display(),
                        report.effective_home.display()
                    );
                }
            }
            Ok(())
        }
        Command::Connect {
            env,
            api_url,
            invite_code,
            invite_code_stdin,
            force,
            use_existing_identity,
        } => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("tokio runtime construction failed: {e}"))?;
            if use_existing_identity {
                let (home, invite_code) = prepare_existing_identity_dev_connect(
                    cli.data_dir.as_deref(),
                    env.as_deref(),
                    api_url.as_deref(),
                    invite_code.as_deref(),
                    invite_code_stdin,
                    force,
                    read_invite_code_from_stdin,
                )?;
                let report = runtime.block_on(
                    lastdb_node::cloud::connect_existing_identity_dev(&home, &invite_code),
                )?;
                eprintln!(
                    "Connected the copied identity to DEV at {}.",
                    report.api_url
                );
                eprintln!("account user_hash={}", report.user_hash);
                Ok(())
            } else {
                let home = lastdb_node::host::resolve_home(cli.data_dir)?;
                let url = resolve_exemem_url(api_url, env.as_deref())?;
                let invite_code = if invite_code_stdin {
                    Some(read_invite_code_from_stdin()?)
                } else {
                    invite_code
                };
                runtime.block_on(lastdb_node::cloud::connect(
                    &home,
                    &url,
                    invite_code.as_deref(),
                    force,
                ))
            }
        }
        Command::Restore {
            into,
            env,
            api_url,
            json,
            progress_json,
            reuse_chunks_from,
            remote_s0_only,
            remote_latest,
            db_hash,
            manifest_sha256,
        } => {
            if (db_hash.is_some() || manifest_sha256.is_some()) && !remote_s0_only && !remote_latest
            {
                return Err(
                    "--db-hash and --manifest-sha256 require --remote-s0-only or --remote-latest"
                        .into(),
                );
            }
            if remote_s0_only {
                restore_remote_s0_only_command(
                    cli.data_dir,
                    &into,
                    env.as_deref(),
                    api_url,
                    json,
                    progress_json,
                    RemoteRecoverySelector {
                        db_hash: db_hash.as_deref(),
                        manifest_sha256: manifest_sha256.as_deref(),
                    },
                )
            } else if remote_latest {
                restore_remote_latest_command(
                    cli.data_dir,
                    &into,
                    env.as_deref(),
                    api_url,
                    json,
                    progress_json,
                    RemoteRecoverySelector {
                        db_hash: db_hash.as_deref(),
                        manifest_sha256: manifest_sha256.as_deref(),
                    },
                )
            } else if let Some(cache_home) = reuse_chunks_from {
                restore_command_with_chunk_cache(
                    cli.data_dir,
                    &into,
                    env.as_deref(),
                    api_url,
                    json,
                    progress_json,
                    &cache_home,
                )
            } else if progress_json {
                restore_command_with_progress(cli.data_dir, &into, env.as_deref(), api_url, json)
            } else {
                restore_command(cli.data_dir, &into, env.as_deref(), api_url, json)
            }
        }
        Command::MigrateHashGroup {
            from,
            into,
            hash_group_key,
            partition_fanout,
            json,
        } => migrate_hash_group_command(
            cli.data_dir,
            &from,
            &into,
            hash_group_key,
            partition_fanout,
            json,
        ),
        Command::Cloud { action } => cloud_command(cli.data_dir, action),
        Command::Mutate {
            schema,
            mutation_type,
            key_hash,
            key_range,
            key_range_prefix,
            must_exist,
            durable,
            cloud_publication,
            json: _,
        } => mutate_command(
            cli.data_dir,
            &schema,
            &mutation_type,
            key_hash.as_deref(),
            key_range.as_deref(),
            key_range_prefix.as_deref(),
            MutateRequestOptions {
                must_exist,
                durable,
                cloud_publication,
            },
        ),
        Command::List {
            schema,
            key_hash,
            limit,
            cursor,
            json,
        } => list_record_keys(
            cli.data_dir,
            &schema,
            key_hash.as_deref(),
            limit,
            cursor.as_deref(),
            json,
            "list",
        ),
        Command::GetKeys {
            schema,
            key_hash,
            limit,
            cursor,
            json,
        } => list_record_keys(
            cli.data_dir,
            &schema,
            key_hash.as_deref(),
            limit,
            cursor.as_deref(),
            json,
            "get-keys",
        ),
        Command::Get {
            schema,
            key_hash,
            key_range,
            json,
        } => get_record(
            cli.data_dir,
            &schema,
            key_hash.as_deref(),
            key_range.as_deref(),
            json,
        ),
        Command::CompactRecord {
            schema,
            key_hash,
            key_range,
            json,
        } => compact_record_command(cli.data_dir, &schema, &key_hash, &key_range, json),
        Command::Schema { action } => match action {
            SchemaInspectCommand::Storage { schema, json } => {
                schema_storage_command(cli.data_dir, &schema, json)
            }
            SchemaInspectCommand::StorageReport { json } => {
                schema_storage_report_command(cli.data_dir, json)
            }
            SchemaInspectCommand::Show { name, json } => {
                schema_show_command(cli.data_dir, &name, json)
            }
            SchemaInspectCommand::Drop {
                schema,
                owner_app,
                must_exist,
                json,
            } => schema_drop_command(cli.data_dir, schema, owner_app, must_exist, json),
        },
        Command::Liveness { action } => match action {
            LivenessCommand::Explain { class, id, json } => {
                liveness_explain_command(cli.data_dir, &class, &id, json)
            }
            LivenessCommand::Bootstrap {
                isolated_copy,
                storage_prefix,
                json,
            } => liveness_bootstrap_command(
                cli.data_dir,
                isolated_copy,
                storage_prefix.as_deref(),
                json,
            ),
        },
        Command::Db { action } => db_command(cli.data_dir, action),
        Command::SchemaRetention { action } => schema_retention_command(cli.data_dir, action),
        Command::SchemaNameClaim { action } => schema_name_claim_command(cli.data_dir, action),
        Command::SearchRebuild { page_size, json } => {
            search_rebuild_command(cli.data_dir, page_size, json)
        }
        Command::App { action } => app_command(cli.data_dir, action),
    }
}

#[path = "lastdb/mutate_cmds.rs"]
mod mutate_cmds;
#[path = "lastdb/restore_progress_reporter.rs"]
mod restore_progress_reporter;
use mutate_cmds::*;

#[path = "lastdb/socket_client.rs"]
mod socket_client;
use socket_client::*;
